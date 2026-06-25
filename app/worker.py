# app/worker.py

import asyncio
import hashlib
from collections import defaultdict
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import app.config  # noqa

from app.utils.time_utils import DUBAI_TZ, dubai_now
from app.core.logger import get_logger
from app.scraper.ohlc import _set_ohlc
from app.scraper.sa import StockAnalysisScraper
from app.data.gsheet import GSheet_Manager
from app.data.cache import Cache
from app.data.fx import fetch_and_save_fx
from app.services.quote import QuoteStore
from app.services.watchlist import WatchlistModule
from app.services.watchlist_ai import WatchlistAIScreener
from app.data.db import DB

from datetime import date

logger = get_logger()

# Global lock to ensure heavy jobs (OHLC and Scraping) NEVER run concurrently
_job_lock = asyncio.Lock()


async def fx_job():
    logger.info("FX job starting")
    try:
        await fetch_and_save_fx()
    except Exception:
        logger.exception("FX job failed")


async def quote_job():
    store = QuoteStore()
    store.write()


async def ohlc_job(bars: int = 100):
    async with _job_lock:
        logger.info("OHLC job starting | now=%s", dubai_now().isoformat())
        try:
            gs = GSheet_Manager()
            tickers = gs.fetch_transactions() + gs.fetch_watchlist()

            # Fix: securely append benchmarks once
            benchmark_list = [
                {"ticker": k, **v} for k, v in app.config.BENCHMARKS.items()
            ]
            tickers += benchmark_list

            seen = set()
            for t in tickers:
                key = t.get("ticker")
                if not key or key in seen:
                    continue
                seen.add(key)
                try:
                    await _set_ohlc(
                        tv_exchange=t["exchange"],
                        symbol=t["symbol"],
                        bars=bars,
                    )
                except Exception:
                    logger.exception("OHLC failed for %s", key)
        except Exception:
            logger.exception("OHLC job failed")


async def fundamentals_drip_job():
    """
    Scrapes EXACTLY ONE ticker per run.
    Prioritizes: 1) New tickers, 2) Updated transactions, 3) Stalest ticker.
    """
    async with _job_lock:
        logger.info("Fundamentals drip job starting | now=%s", dubai_now().isoformat())
        try:
            gs = GSheet_Manager()
            transactions = gs.fetch_transactions()
            watchlist = gs.fetch_watchlist()

            # Group purchases by ticker
            purchases_map = defaultdict(list)
            for txn in transactions:
                if key := txn.get("ticker"):
                    purchases_map[key].append(txn)

            # Combine all unique tickers and their meta info
            all_info = {}
            for txn in transactions:
                if key := txn.get("ticker"):
                    all_info[key] = txn
            for item in watchlist:
                if key := item.get("ticker"):
                    if key not in all_info:
                        all_info[key] = item

            if not all_info:
                logger.info("No tickers found in sheets.")
                return

            cache = Cache()

            # Priority Buckets
            missing_tickers = []
            updated_tickers = []
            stale_tickers = []

            for key, info in all_info.items():
                cached_data = cache.load(key)

                # Priority 1: Not cached at all (New)
                if not cached_data:
                    missing_tickers.append(key)
                    continue

                # Priority 2: Transactions changed (Updated)
                cached_purchases = cached_data.get("purchase_details", [])
                current_purchases = purchases_map[key]
                if cached_purchases != current_purchases:
                    updated_tickers.append(key)
                    continue

                # Priority 3: Normal age-based staleness
                scraped_at = cached_data.get("scraped_at", "1970-01-01T00:00:00")
                stale_tickers.append((key, scraped_at))

            # Select the ONE ticker to scrape this run
            target_key = None
            if missing_tickers:
                target_key = missing_tickers[0]
                logger.info("Priority 1: Scraping brand new ticker: %s", target_key)
            elif updated_tickers:
                target_key = updated_tickers[0]
                logger.info(
                    "Priority 2: Scraping ticker with updated transactions: %s",
                    target_key,
                )
            elif stale_tickers:
                # Sort by oldest date first
                stale_tickers.sort(key=lambda x: x[1])
                target_key = stale_tickers[0][0]
                logger.info(
                    "Priority 3: Scraping stalest ticker: %s (last scraped: %s)",
                    target_key,
                    stale_tickers[0][1],
                )

            if not target_key:
                return

            # Execute scrape with a generous 4-minute timeout per ticker
            info = all_info[target_key]
            obj = StockAnalysisScraper()
            try:
                scrape = await asyncio.wait_for(
                    obj.scrape(
                        {
                            "exchange": info["sa_exchange"],
                            "symbol": info["sa_symbol"],
                        }
                    ),
                    timeout=240,  # 4 minutes conservative timeout
                )
                scrape["purchase_details"] = purchases_map[target_key]
                cache.save(target_key, scrape)
                logger.info("Fundamentals saved successfully: %s", target_key)
            except asyncio.TimeoutError:
                logger.error(
                    "Fundamentals timed out for %s — skipping for now", target_key
                )
            except Exception:
                logger.exception("Fundamentals failed for %s", target_key)

        except Exception:
            logger.exception("Fundamentals drip job failed")


async def watchlist_screening_job():
    logger.info("Watchlist screening job starting")
    try:
        gs = GSheet_Manager()
        raw_items = gs.fetch_watchlist()

        module = WatchlistModule(Cache(), DB())
        enriched = module.get_watchlist(raw_items)

        stored = WatchlistAIScreener.read()
        stored_by_ticker = {a["ticker"]: a for a in stored.get("alerts", [])}
        today = date.today()

        def is_due(item: dict) -> bool:
            ticker = item["ticker"]
            stored_alert = stored_by_ticker.get(ticker)
            if not stored_alert:
                return True
            screened_at = stored_alert.get("screened_at")
            if screened_at:
                try:
                    age = (today - date.fromisoformat(screened_at[:10])).days
                    if age >= 14:
                        return True
                except (ValueError, TypeError):
                    return True
            next_check = stored_alert.get("next_check_date")
            if not next_check:
                return True
            try:
                return today >= date.fromisoformat(next_check)
            except (ValueError, TypeError):
                return True

        due_items = [i for i in enriched if is_due(i)]
        logger.info("%d/%d tickers due for screening", len(due_items), len(enriched))

        if not due_items:
            logger.info("No tickers due today, skipping screening")
            return

        fundamentals_map = {
            item["ticker"]: (
                (data.statistics.dict() if data.statistics else {})
                if (data := module.get_ticker(item["ticker"]))
                else {}
            )
            for item in due_items
        }

        new_alerts = WatchlistAIScreener().run(due_items, fundamentals_map)

        updated = {a["ticker"]: a for a in stored.get("alerts", [])}
        for alert in new_alerts:
            updated[alert["ticker"]] = alert

        screener = WatchlistAIScreener()
        screener._persist(list(updated.values()))

    except Exception:
        logger.exception("Watchlist screening job failed")


async def main():
    scheduler = AsyncIOScheduler(timezone=DUBAI_TZ)

    # OHLC runs on the :00, :15, :30, :45 marks during market hours
    scheduler.add_job(
        ohlc_job,
        "cron",
        day_of_week="mon-fri",
        hour="10-23",
        minute="0,15,30,45",
        id="ohlc_intraday",
        max_instances=1,
        misfire_grace_time=120,
    )

    # Drip Scraper runs on the :07, :22, :37, :52 marks (7 minutes offset from OHLC)
    # It runs 24/7. When markets are closed, it keeps catching up on the backlog.
    scheduler.add_job(
        fundamentals_drip_job,
        "cron",
        minute="7,22,37,52",
        id="fundamentals_drip_247",
        max_instances=1,
        misfire_grace_time=300,
    )

    scheduler.add_job(
        fx_job,
        "cron",
        day_of_week="mon-fri",
        hour=6,
        minute=0,
        id="fx_daily",
        max_instances=1,
        misfire_grace_time=300,
    )

    scheduler.add_job(
        quote_job,
        "cron",
        day_of_week="mon",
        hour=0,
        minute=0,
        id="quote_daily",
        max_instances=1,
        misfire_grace_time=300,
    )

    scheduler.add_job(
        watchlist_screening_job,
        "cron",
        hour=18,
        minute=0,
        timezone="Asia/Dubai",
        id="watchlist_screening",
        max_instances=1,
        misfire_grace_time=300,
    )

    scheduler.start()

    # Optional manual triggers on startup:
    # await fx_job()
    # await ohlc_job(bars=100)
    # await quote_job()
    # await watchlist_screening_job()

    try:
        while True:
            await asyncio.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("Worker stopped.")


if __name__ == "__main__":
    asyncio.run(main())
