# app/worker.py

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

logger = get_logger()


async def fx_job():
    logger.info("FX job starting")
    try:
        await fetch_and_save_fx()
    except Exception:
        logger.exception("FX job failed")


async def ohlc_job(bars: int = 100):
    logger.info("OHLC job starting | now=%s", dubai_now().isoformat())
    try:
        gs = GSheet_Manager()
        tickers = gs.fetch_transactions() + gs.fetch_watchlist()
        tickers.append(app.config.BENCHMARKS)

        benchmark_list = [{"ticker": k, **v} for k, v in app.config.BENCHMARKS.items()]
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


async def fundamentals_job():
    logger.info("Fundamentals job starting | now=%s", dubai_now().isoformat())
    try:
        gs = GSheet_Manager()
        transactions = gs.fetch_transactions()  #
        watchlist = gs.fetch_watchlist()

        # Group transactions by canonical key, watchlist items have no purchases
        grouped: dict[str, dict] = defaultdict(
            lambda: {"ticker_info": None, "purchases": []}
        )
        for txn in transactions:
            key = txn.get("ticker")
            if not key:
                continue
            grouped[key]["ticker_info"] = txn
            grouped[key]["purchases"].append(txn)

        for item in watchlist:
            key = item.get("ticker")
            if key and key not in grouped:
                grouped[key]["ticker_info"] = item
                grouped[key]["purchases"] = []

        obj = StockAnalysisScraper()
        cache = Cache()

        for key, data in grouped.items():
            info = data["ticker_info"]
            try:
                # SA scraper gets sa_exchange so it hits the right URL
                scrape = await obj.scrape(
                    {
                        "exchange": info["sa_exchange"],
                        "symbol": info["sa_symbol"],
                    }
                )
                scrape["purchase_details"] = data["purchases"]
                # Always save under the canonical TV key
                cache.save(key, scrape)
                logger.info("Fundamentals saved: %s (sa=%s)", key, info["sa_exchange"])
            except Exception:
                logger.exception("Fundamentals failed for %s", key)
    except Exception:
        logger.exception("Fundamentals job failed")


async def main():
    scheduler = AsyncIOScheduler(timezone=DUBAI_TZ)

    scheduler.add_job(
        ohlc_job,
        "cron",
        day_of_week="mon-fri",
        hour="10-16",
        minute="*/15",
        id="ohlc_intraday",
        max_instances=1,
        misfire_grace_time=120,
    )
    scheduler.add_job(
        fundamentals_job,
        "cron",
        day_of_week="mon-fri",
        hour=0,
        minute=5,
        id="fundamentals_daily",
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

    scheduler.start()
    # await fundamentals_job()
    # await fx_job()
    await ohlc_job(bars=200)

    try:
        while True:
            await asyncio.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("Worker stopped.")


if __name__ == "__main__":
    asyncio.run(main())
