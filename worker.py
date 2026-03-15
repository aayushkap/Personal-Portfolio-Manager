# worker.py

import asyncio
import logging
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from portfolio_manager import PortfolioManager
from analytics import PortfolioAnalytics
from portfolio_snapshot import PortfolioSnapshotter
from cache_manager import CacheManager

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


async def scrape_job():
    """Full scrape + snapshot. Runs on schedule."""
    logger.info(" Scrape job starting ")
    try:
        manager = PortfolioManager()
        await manager.run()  # scrapes all tickers, saves to cache

        # After scrape, record daily snapshot
        analytics = PortfolioAnalytics()
        cache = CacheManager()
        snapshotter = PortfolioSnapshotter()
        summary = analytics.portfolio_summary()

        market_value = summary["total_market_value_aed"]
        total_invested = summary["total_invested_aed"]

        # Detect new purchases today by comparing invested vs last snapshot
        last = snapshotter._last_row()
        prev_invested = float(last["total_invested_aed"]) if last else 0.0
        cash_flow_today = max(0.0, total_invested - prev_invested)  # only inflows

        snapshotter.record(
            market_value=market_value,
            total_invested=total_invested,
            cash_flow_today=cash_flow_today,
        )
        logger.info(f" Scrape job done | Portfolio: AED {market_value:,.2f} ")

    except Exception:
        logger.exception("Scrape job failed")


async def main():
    scheduler = AsyncIOScheduler()

    # During market hours (ADX/DFM: Sun–Thu 10:00–15:00 GST = UTC+4)
    # Run every 15 min during market hours, once at close
    scheduler.add_job(
        scrape_job,
        "cron",
        day_of_week="sun-thu",
        hour="10-14",
        minute="*/15",
        id="scrape_intraday",
        max_instances=1,  # never overlap
        misfire_grace_time=120,
    )

    # End-of-day snapshot (15:10 GST — 10 min after close)
    scheduler.add_job(
        scrape_job,
        "cron",
        day_of_week="sun-thu",
        hour=15,
        minute=10,
        id="scrape_eod",
        max_instances=1,
    )

    scheduler.start()
    logger.info("Worker started — scraping every 15min Sun–Thu 10:00–15:00 GST")

    # Run once immediately on startup
    await scrape_job()

    # Keep alive
    try:
        while True:
            await asyncio.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("Worker stopped.")


if __name__ == "__main__":
    asyncio.run(main())
