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

logger = get_logger()


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


def _ticker_bucket(key: str, num_buckets: int = 4) -> int:
    return int(hashlib.md5(key.encode()).hexdigest(), 16) % num_buckets


async def fundamentals_job(bucket: int):
    logger.info(
        "Fundamentals job starting | bucket=%d | now=%s",
        bucket,
        dubai_now().isoformat(),
    )
    try:
        gs = GSheet_Manager()
        transactions = gs.fetch_transactions()
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

        # Only process tickers assigned to this bucket
        bucket_keys = [k for k in grouped if _ticker_bucket(k) == bucket]
        logger.info("Bucket %d: %d/%d tickers", bucket, len(bucket_keys), len(grouped))

        obj = StockAnalysisScraper()
        cache = Cache()

        for key in bucket_keys:
            data = grouped[key]
            info = data["ticker_info"]
            try:
                scrape = await obj.scrape(
                    {
                        "exchange": info["sa_exchange"],
                        "symbol": info["sa_symbol"],
                    }
                )
                scrape["purchase_details"] = data["purchases"]
                cache.save(key, scrape)
                logger.info("Fundamentals saved: %s (sa=%s)", key, info["sa_exchange"])
            except Exception:
                logger.exception("Fundamentals failed for %s", key)
    except Exception:
        logger.exception("Fundamentals job failed")


async def watchlist_screening_job():
    logger.info("Watchlist screening job starting")
    try:
        gs = GSheet_Manager()
        raw_items = gs.fetch_watchlist()

        module = WatchlistModule(Cache(), DB())
        enriched = module.get_watchlist(raw_items)

        fundamentals_map = {
            item["ticker"]: (
                (data.statistics.dict() if data.statistics else {})
                if (data := module.get_ticker(item["ticker"]))
                else {}
            )
            for item in enriched
        }

        WatchlistAIScreener().run(enriched, fundamentals_map)
    except Exception:
        logger.exception("Watchlist screening job failed")


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
        ohlc_job,
        "cron",
        day_of_week="mon-fri",
        hour="17-23",
        minute="0",
        id="ohlc_intraday_slow",
        max_instances=1,
        misfire_grace_time=120,
    )

    # Buckets 0 and 1 run on even days-of-month, buckets 2 and 3 on odd days.
    # Two slots per day (1 AM and 5 AM) keep each session small and finish
    # well before the 10 AM busy window. Every ticker is refreshed every ~2 days.
    scheduler.add_job(
        fundamentals_job,
        "cron",
        args=[0],
        day="2-30/2",
        hour=1,
        minute=0,
        id="fundamentals_bucket_0",
        max_instances=1,
        misfire_grace_time=300,
    )
    scheduler.add_job(
        fundamentals_job,
        "cron",
        args=[1],
        day="2-30/2",
        hour=5,
        minute=0,
        id="fundamentals_bucket_1",
        max_instances=1,
        misfire_grace_time=300,
    )
    scheduler.add_job(
        fundamentals_job,
        "cron",
        args=[2],
        day="1-31/2",
        hour=1,
        minute=0,
        id="fundamentals_bucket_2",
        max_instances=1,
        misfire_grace_time=300,
    )
    scheduler.add_job(
        fundamentals_job,
        "cron",
        args=[3],
        day="1-31/2",
        hour=5,
        minute=0,
        id="fundamentals_bucket_3",
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
        day="*/2",
        hour=18,
        minute=0,
        timezone="Asia/Dubai",
        id="watchlist_screening",
        max_instances=1,
        misfire_grace_time=300,
    )

    scheduler.start()
    # await fundamentals_job()
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
