import asyncio
import logging
import json
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.main import PortfolioManager
from app.analytics import PortfolioAnalytics
from app.data.cache_manager import CacheManager
from app.data.data_collector import StockAnalysisScraper
from app.time_utils import DUBAI_TZ, dubai_now

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


async def ohlc_job():
    logger.info("OHLC job starting | now=%s", dubai_now().isoformat())
    try:
        manager = PortfolioManager()
        tickers = manager.get_tickers()
        cache = CacheManager()

        for ticker in tickers:
            try:
                ohlc = await StockAnalysisScraper.get_ohlc(
                    exchange=ticker["exchange"],
                    symbol=ticker["symbol"],
                    bars=100,
                )
                ticker_key = f"{ticker['exchange'].upper()}:{ticker['symbol']}"
                stat = cache.append_ohlc(ticker_key, ohlc)
                logger.info("%s +%s bars", ticker_key, stat.get("appended", 0))

            except Exception:
                logger.exception("OHLC failed: %s", ticker)

        await snapshot_job()

    except Exception:
        logger.exception("OHLC job failed")


async def fundamentals_job():
    """Heavy — full scrape (overview, financials, dividends, etc). Runs once daily."""
    logger.info("Fundamentals job starting | now=%s", dubai_now().isoformat())
    try:
        manager = PortfolioManager()
        await manager.run()

        results = PortfolioAnalytics().run()
        with open("analytics_output.json", "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, default=str)

        await snapshot_job()

    except Exception:
        logger.exception("Fundamentals job failed")


async def snapshot_job():
    """Records portfolio snapshot using Dubai-local date."""
    logger.info("Snapshot job | now=%s", dubai_now().isoformat())
    try:
        analytics = PortfolioAnalytics()
        summary = analytics.run()
        with open("analytics_output.json", "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, default=str)

        logger.info("Snapshot saved. Analytics done.")

    except Exception:
        logger.exception("Snapshot job failed")


async def main():
    scheduler = AsyncIOScheduler(timezone=DUBAI_TZ)

    # OHLC — every 30 min, Mon-Fri, 10:00–16:30 Asia/Dubai
    scheduler.add_job(
        ohlc_job,
        "cron",
        day_of_week="mon-fri",
        hour="10-16",
        minute="*/30",
        id="ohlc_intraday",
        max_instances=1,
        misfire_grace_time=120,
    )

    # Fundamentals — once daily at 00:05 Asia/Dubai
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

    scheduler.start()

    logger.info(
        "Worker started\n"
        "  Timezone:     Asia/Dubai\n"
        "  OHLC:         Mon-Fri every 30 min (10:00-16:30 Asia/Dubai)\n"
        "  Fundamentals: Mon-Fri once at 00:05 Asia/Dubai"
    )

    await fundamentals_job()
    await ohlc_job()

    try:
        while True:
            await asyncio.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("Worker stopped.")
