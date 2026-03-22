# worker.py

import asyncio
import logging
import json
from datetime import datetime
from zoneinfo import ZoneInfo
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from main import PortfolioManager
from analytics import PortfolioAnalytics
from portfolio_snapshot import PortfolioSnapshotter
from cache_manager import CacheManager
from data_collector import StockAnalysisScraper

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


async def ohlc_job():
    """Lightweight — fetches OHLC only. Runs every 30min during market hours."""
    logger.info(" OHLC job starting ")
    try:
        manager = PortfolioManager()
        tickers = manager.get_tickers()
        cache = CacheManager()

        for ticker in tickers:
            try:
                # StockAnalysisScraper expects ticker dict, get_ohlc is on the instance
                scraper = StockAnalysisScraper(ticker=ticker)
                ohlc = await scraper.get_ohlc(
                    exchange=ticker["exchange"],
                    symbol=ticker["symbol"],
                    bars=100,
                )
                ticker_key = f"{ticker['exchange'].upper()}:{ticker['symbol']}"
                stat = cache.append_ohlc(ticker_key, ohlc)
                logger.info(f"  {ticker_key} +{stat.get('appended', 0)} bars")

                await snapshot_job()
            except Exception:
                logger.exception(f"  OHLC failed: {ticker}")

    except Exception:
        logger.exception("OHLC job failed")


async def fundamentals_job():
    """Heavy — full scrape (overview, financials, dividends, etc). Runs once daily."""
    logger.info(" Fundamentals job starting ")
    try:
        manager = PortfolioManager()
        await manager.run()  # full scrape, saves to cache

        # Write analytics JSON + record snapshot
        results = PortfolioAnalytics().run()
        with open("analytics_output.json", "w") as f:
            json.dump(results, f, indent=2, default=str)

        ret = results["summary"]["returns"]
        logger.info(
            f" Fundamentals done | "
            f"Value: AED {ret['total_market_value_aed']:,.2f} | "
            f"P&L: {ret['price_return_pct']:+.2f}% | "
            f"Total Return: {ret['total_return_pct']:+.2f}% "
        )

        await snapshot_job()

    except Exception:
        logger.exception("Fundamentals job failed")


async def snapshot_job():
    """Records end-of-day portfolio snapshot to CSV."""
    logger.info(" Snapshot job ")
    try:
        analytics = PortfolioAnalytics()
        summary = analytics.run()

        logger.info(f"  Snapshot saved. Analytics done.")

    except Exception:
        logger.exception("Snapshot job failed")


async def main():
    scheduler = AsyncIOScheduler(timezone=ZoneInfo("Asia/Dubai"))

    #  OHLC — every 30min, Mon-Fri, 10:00-14:30 GST
    scheduler.add_job(
        ohlc_job,
        "cron",
        day_of_week="mon-fri",
        hour="10-14",
        minute="*/30",
        id="ohlc_intraday",
        max_instances=1,
        misfire_grace_time=120,
    )

    #  Fundamentals — once daily at market open (10:05 GST)
    scheduler.add_job(
        fundamentals_job,
        "cron",
        day_of_week="mon-fri",
        hour=10,
        minute=5,
        id="fundamentals_daily",
        max_instances=1,
        misfire_grace_time=300,
    )

    scheduler.start()
    await snapshot_job()
    logger.info(
        "Worker started\n"
        "  OHLC:         Mon-Fri every 30min (10:00-14:30 GST)\n"
        "  Fundamentals: Mon-Fri once at 10:05 GST\n"
        "  Snapshot:     Mon-Fri once at 15:30 GST"
    )

    # Run both immediately on startup
    # await fundamentals_job()
    # await ohlc_job()

    try:
        while True:
            await asyncio.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("Worker stopped.")


if __name__ == "__main__":
    asyncio.run(main())
