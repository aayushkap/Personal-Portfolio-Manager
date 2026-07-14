# app/worker.py

import time
import asyncio
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
from app.services.holdings_news import HoldingsNewsAgent

from datetime import date

logger = get_logger()

# Double-guard lock: job_runner is the primary sequencer,
# but this lock protects against any direct manual calls too.
_job_lock = asyncio.Lock()

_scrape_failures: dict[str, int] = defaultdict(int)
_FAILURE_COOLDOWN_SECS = 6 * 60 * 60  # 6 hours after 3 consecutive failures
_FAILURE_THRESHOLD = 3
_scrape_cooldown_until: dict[str, float] = {}


def _current_week_key() -> tuple[int, int]:
    iso = dubai_now().date().isocalendar()
    return iso.year, iso.week


def _week_key_from_scraped_at(scraped_at: str | None) -> tuple[int, int] | None:
    if not scraped_at:
        return None
    try:
        d = date.fromisoformat(scraped_at[:10])
        iso = d.isocalendar()
        return iso.year, iso.week
    except Exception:
        return None


def _was_scraped_this_week(cached_data: dict | None) -> bool:
    if not cached_data:
        return False
    return (
        _week_key_from_scraped_at(cached_data.get("scraped_at")) == _current_week_key()
    )


# Lightweight jobs — keep their cron schedules, no Playwright involved


async def fx_job():
    logger.info("FX job starting")
    try:
        await fetch_and_save_fx()
    except Exception:
        logger.exception("FX job failed")


async def quote_job():
    store = QuoteStore()
    store.write()


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


# Heavy jobs — orchestrated exclusively by job_runner, never by cron


async def ohlc_job(bars: int = 50):
    async with _job_lock:
        logger.info("OHLC job starting | now=%s", dubai_now().isoformat())
        try:
            gs = GSheet_Manager()
            tickers = gs.fetch_transactions() + gs.fetch_watchlist()

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


async def fundamentals_drip_job() -> str:
    """
    Scrapes EXACTLY ONE ticker per run.

    Returns:
        "scraped" -> a ticker was successfully scraped
        "failed"  -> a ticker was attempted but failed/timed out
        "idle"    -> nothing is due this week
    """
    async with _job_lock:
        logger.info("Fundamentals drip starting | now=%s", dubai_now().isoformat())
        try:
            gs = GSheet_Manager()
            transactions = gs.fetch_transactions()
            watchlist = gs.fetch_watchlist()

            purchases_map: dict[str, list] = defaultdict(list)
            for txn in transactions:
                if key := txn.get("ticker"):
                    purchases_map[key].append(txn)

            all_info: dict[str, dict] = {}
            for txn in transactions:
                if key := txn.get("ticker"):
                    all_info[key] = txn
            for item in watchlist:
                if key := item.get("ticker"):
                    if key not in all_info:
                        all_info[key] = item

            if not all_info:
                logger.info("No tickers found in sheets — nothing to scrape.")
                return "idle"

            cache = Cache()

            missing_tickers: list[str] = []
            updated_tickers: list[str] = []
            due_this_week_tickers: list[tuple[str, str]] = []
            fresh_this_week = 0

            for key in all_info:
                # Skip tickers currently in failure cooldown
                if time.time() < _scrape_cooldown_until.get(key, 0):
                    logger.debug("Skipping %s — in failure cooldown", key)
                    fresh_this_week += 1  # count it as "not due" so the log stays clean
                    continue

                cached_data = cache.load(key)

                # Priority 1: never scraped
                if not cached_data:
                    missing_tickers.append(key)
                    continue

                # Priority 2: purchase details changed since last scrape
                if cached_data.get("purchase_details") != purchases_map[key]:
                    updated_tickers.append(key)
                    continue

                # Weekly cap: if already scraped this ISO week, skip it
                if _was_scraped_this_week(cached_data):
                    fresh_this_week += 1
                    continue

                # Priority 3: due again because it has NOT been scraped this week
                scraped_at = cached_data.get("scraped_at", "1970-01-01T00:00:00")
                due_this_week_tickers.append((key, scraped_at))

            logger.info(
                "Fundamentals queue | new=%d updated=%d due=%d already_done_this_week=%d total=%d",
                len(missing_tickers),
                len(updated_tickers),
                len(due_this_week_tickers),
                fresh_this_week,
                len(all_info),
            )

            target_key: str | None = None

            if missing_tickers:
                target_key = missing_tickers[0]
                logger.info("Priority 1 — new ticker: %s", target_key)
            elif updated_tickers:
                target_key = updated_tickers[0]
                logger.info("Priority 2 — updated transactions: %s", target_key)
            elif due_this_week_tickers:
                due_this_week_tickers.sort(key=lambda x: x[1])
                target_key, last_scraped = due_this_week_tickers[0]
                logger.info(
                    "Priority 3 — due this week: %s (last scraped: %s)",
                    target_key,
                    last_scraped,
                )
            else:
                logger.info("No fundamentals work due this week.")
                return "idle"

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
                    timeout=480,
                )
                scrape["purchase_details"] = purchases_map[target_key]
                cache.save(target_key, scrape)
                logger.info("Fundamentals saved: %s", target_key)

                # Reset failure tracking on success
                _scrape_failures.pop(target_key, None)
                _scrape_cooldown_until.pop(target_key, None)
                return "scraped"

            except asyncio.TimeoutError:
                logger.error(
                    "Fundamentals timed out for %s — patching purchase_details and moving on",
                    target_key,
                )

                _scrape_failures[target_key] += 1
                if _scrape_failures[target_key] >= _FAILURE_THRESHOLD:
                    until = time.time() + _FAILURE_COOLDOWN_SECS
                    _scrape_cooldown_until[target_key] = until
                    logger.error(
                        "%s failed %d times — cooling down for %.0fh",
                        target_key,
                        _scrape_failures[target_key],
                        _FAILURE_COOLDOWN_SECS / 3600,
                    )

                # Patch purchase_details into whatever is cached so this ticker
                # stops appearing as Priority 2 on the next cycle.
                existing = cache.load(target_key) or {}
                existing["purchase_details"] = purchases_map[target_key]
                cache.save(target_key, existing)
                return "failed"

            except Exception:
                logger.exception("Fundamentals failed for %s", target_key)
                existing = cache.load(target_key) or {}

                _scrape_failures[target_key] += 1
                if _scrape_failures[target_key] >= _FAILURE_THRESHOLD:
                    until = time.time() + _FAILURE_COOLDOWN_SECS
                    _scrape_cooldown_until[target_key] = until
                    logger.error(
                        "%s failed %d times — cooling down for %.0fh",
                        target_key,
                        _scrape_failures[target_key],
                        _FAILURE_COOLDOWN_SECS / 3600,
                    )

                existing["purchase_details"] = purchases_map[target_key]
                cache.save(target_key, existing)
                return "failed"

        except Exception:
            logger.exception("Fundamentals drip job failed")
            return "failed"


def run_holdings_news_check():
    HoldingsNewsAgent().run()


# Job Runner — single continuous loop, the ONLY place OHLC + drip are called


async def job_runner():
    """
    Serial orchestrator for heavy jobs only.

    Market hours (Mon-Fri 10:00-23:00 Dubai):
      OHLC -> one fundamentals drip -> sleep until next 15-min cycle.

    Off-hours:
      Run drip only when there is work due.
      If everything for the current ISO week is already done, sleep longer.
    """
    logger.info("Job runner started")

    while True:
        try:
            cycle_start = dubai_now()
            hour = cycle_start.hour
            weekday = cycle_start.weekday()  # 0=Mon ... 6=Sun
            is_market_hours = (weekday < 5) and (10 <= hour <= 23)

            if is_market_hours:
                logger.info("Job runner: market hours — running OHLC")
                await ohlc_job()

                logger.info("Job runner: market hours — running one drip scrape")
                drip_status = await fundamentals_drip_job()

                elapsed = (dubai_now() - cycle_start).total_seconds()
                sleep_for = max(30, (15 * 60) - elapsed)

                logger.info(
                    "Job runner: market cycle done | drip_status=%s | elapsed=%.0fs | sleep=%.0fs",
                    drip_status,
                    elapsed,
                    sleep_for,
                )
                await asyncio.sleep(sleep_for)

            else:
                logger.info("Job runner: off-hours — checking fundamentals work")
                drip_status = await fundamentals_drip_job()

                if drip_status == "scraped":
                    sleep_for = 30
                elif drip_status == "failed":
                    sleep_for = 15 * 60
                else:
                    # idle: nothing due this week, so chill
                    sleep_for = 120 * 60 if weekday >= 5 else 60 * 60

                logger.info(
                    "Job runner: off-hours | drip_status=%s | sleep=%.0fs",
                    drip_status,
                    sleep_for,
                )
                await asyncio.sleep(sleep_for)

        except Exception:
            logger.exception("Job runner error — resuming in 60s")
            await asyncio.sleep(60)


# Entry point


async def main():
    scheduler = AsyncIOScheduler(timezone=DUBAI_TZ)

    # Lightweight cron jobs — these never touch Playwright
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
    scheduler.add_job(
        run_holdings_news_check,
        "cron",
        hour=12,
        minute=0,
        id="holdings_news",
        max_instances=1,
        misfire_grace_time=300,
    )

    scheduler.start()

    # Start the heavy job runner as a background task.
    # OHLC and fundamentals scraping are managed solely here.
    asyncio.create_task(job_runner())

    # Manual one-shot triggers:
    # await ohlc_job(bars=100)
    # await fundamentals_drip_job()
    # await fx_job()
    # await quote_job()
    # await watchlist_screening_job()
    # run_holdings_news_check()

    try:
        while True:
            await asyncio.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("Worker stopped.")


if __name__ == "__main__":
    asyncio.run(main())
