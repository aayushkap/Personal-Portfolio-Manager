import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import app.config  # noqa

from app.utils import DUBAI_TZ, dubai_now
from app.core.logger import get_logger

from app.scraper.ohlc import _set_ohlc
from app.scraper.sa import StockAnalysisScraper
from app.data.gsheet import GSheet_Manager
from app.data.cache import Cache


logger = get_logger()


async def ohlc_job():
    logger.info("OHLC job starting | now=%s", dubai_now().isoformat())
    try:
        gsheets_fetcher = GSheet_Manager()
        tickers = gsheets_fetcher.fetch_transactions()

        for ticker in tickers:
            try:
                if ticker.get("exchange") and ticker.get("symbol"):
                    await _set_ohlc(
                        exchange=ticker["exchange"],
                        symbol=ticker["symbol"],
                        bars=25,
                    )
                    logger.info(
                        f"Set bars for: {ticker['exchange']}:{ticker['symbol']}"
                    )

            except Exception as e:
                logger.exception("OHLC failed for %s: %s", ticker, str(e))

    except Exception:
        logger.exception("OHLC job failed")


async def fundamentals_job():
    logger.info("Fundamentals job starting | now=%s", dubai_now().isoformat())
    try:
        gsheets_fetcher = GSheet_Manager()
        tickers = gsheets_fetcher.fetch_transactions()

        for ticker in tickers:
            try:
                if ticker.get("exchange") and ticker.get("symbol"):
                    ticker_key = f"{ticker['exchange']}:{ticker['symbol']}"
                    obj = StockAnalysisScraper()
                    scrape = await obj.scrape(
                        {"exchange": ticker["exchange"], "symbol": ticker["symbol"]}
                    )

                    cache = Cache()
                    cache.save(ticker_key, scrape)
                    logger.info(f"Set fundamentals for: {ticker_key}")

            except Exception as e:
                logger.exception("Fundamentals failed for %s: %s", ticker, str(e))

    except Exception:
        logger.exception("Fundamentals job failed")


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

    await fundamentals_job()

    try:
        while True:
            await asyncio.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("Worker stopped.")


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
