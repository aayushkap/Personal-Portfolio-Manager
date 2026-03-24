import asyncio
import logging
import json
from zoneinfo import ZoneInfo
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from main import PortfolioManager
from analytics import PortfolioAnalytics
from cache_manager import CacheManager
from data_collector import StockAnalysisScraper
from time_utils import DUBAI_TZ, dubai_now

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


async def ohlc_job():
    """Lightweight — fetches OHLC only. Runs every 30 min during Dubai market window."""
    logger.info("OHLC job starting | now=%s", dubai_now().isoformat())
    try:
        manager = PortfolioManager()
        tickers = manager.get_tickers()
        cache = CacheManager()

        for ticker in tickers:
            try:
                scraper = StockAnalysisScraper(ticker=ticker)
                ohlc = await scraper.get_ohlc(
                    exchange=ticker["exchange"],
                    symbol=ticker["symbol"],
                    bars=100,
                )
                ticker_key = f"{ticker['exchange'].upper()}:{ticker['symbol']}"
                stat = cache.append_ohlc(ticker_key, ohlc)
                logger.info("%s +%s bars", ticker_key, stat.get("appended", 0))

            except Exception:
                logger.exception("OHLC failed: %s", ticker)

        # Run snapshot once after all tickers finish
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

        ret = results["summary"]["returns"]
        logger.info(
            "Fundamentals done | Value: AED %,.2f | P&L: %+,.2f%% | Total Return: %+,.2f%%",
            ret["total_market_value_aed"],
            ret["price_return_pct"],
            ret["total_return_pct"],
        )

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
        "  OHLC:         Mon-Fri every 30 min (10:00–16:30 Asia/Dubai)\n"
        "  Fundamentals: Mon-Fri once at 00:05 Asia/Dubai"
    )

    await fundamentals_job()
    await ohlc_job()
    await snapshot_job()

    try:
        while True:
            await asyncio.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("Worker stopped.")
