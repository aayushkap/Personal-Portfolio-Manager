import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import app.config  # noqa
from collections import defaultdict

from app.utils.time_utils import DUBAI_TZ, dubai_now
from app.core.logger import get_logger

from app.scraper.ohlc import _set_ohlc
from app.scraper.sa import StockAnalysisScraper
from app.data.gsheet import GSheet_Manager
from app.data.cache import Cache


logger = get_logger()


async def ohlc_job(bars: int = 100):
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
                        bars=bars,
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
        transactions = gsheets_fetcher.fetch_transactions()

        grouped = defaultdict(list)
        for txn in transactions:
            exchange = txn.get("exchange")
            symbol = txn.get("symbol")
            if exchange and symbol:
                ticker_key = f"{exchange}:{symbol}"
                grouped[ticker_key].append(txn)

        obj = StockAnalysisScraper()
        cache = Cache()

        for ticker_key, purchase_details in grouped.items():
            try:
                exchange, symbol = ticker_key.split(":", 1)

                scrape = await obj.scrape({"exchange": exchange, "symbol": symbol})

                # keep all purchases for this ticker
                scrape["purchase_details"] = purchase_details

                cache.save(ticker_key, scrape)
                logger.info(
                    "Set fundamentals for: %s (%d purchases)",
                    ticker_key,
                    len(purchase_details),
                )

            except Exception as e:
                logger.exception("Fundamentals failed for %s: %s", ticker_key, str(e))

    except Exception:
        logger.exception("Fundamentals job failed")


async def main():
    scheduler = AsyncIOScheduler(timezone=DUBAI_TZ)

    # OHLC — every 20 min, Mon-Fri, 10:00–16:00 Asia/Dubai
    scheduler.add_job(
        ohlc_job,
        "cron",
        day_of_week="mon-fri",
        hour="10-16",
        minute="*/20",
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

    # await fundamentals_job()
    # await ohlc_job(bars=2500)  # First time get all. Then default to 100

    try:
        while True:
            await asyncio.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("Worker stopped.")


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
