import gspread
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

from data_collector import StockAnalysisScraper
from cache_manager import CacheManager
from collections import defaultdict

import asyncio

load_dotenv()

SERVICE_ACCOUNT_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    os.getenv("GOOGLE_SHEETS_SERVICE_ACCOUNT_FILE"),
)
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")


class PortfolioManager:

    def __init__(self):
        pass

    #  GOOGLE SHEETS
    def fetch_investments(self):
        if not os.path.exists(SERVICE_ACCOUNT_FILE):
            print(f"ERROR: JSON not found at: {SERVICE_ACCOUNT_FILE}")
            return None
        try:
            gc = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)
            sh = gc.open_by_key(SPREADSHEET_ID)
            worksheet = sh.get_worksheet(0)
            rows = worksheet.get_all_records()
            return [r for r in rows if r.get("Symbol") and ":" in str(r.get("Symbol"))]
        except gspread.exceptions.SpreadsheetNotFound:
            print("ERROR: Share the sheet with your service account email.")
        except Exception:
            import traceback

            traceback.print_exc()

    async def run(self):
        results = []
        tickers = set()

        print("Fetching Google Sheets data...")
        investments = self.fetch_investments()
        if not investments:
            print("No investments found.")
            return {}

        #  Group purchases by ticker key
        purchases_by_ticker = defaultdict(list)
        for inv in investments:
            symbol = inv.get("Symbol")
            if symbol and ":" in symbol:
                purchases_by_ticker[symbol].append(inv)

        # Collect unique tickers
        for symbol in purchases_by_ticker:
            ex, sym = symbol.split(":", 1)
            if ex and sym:
                tickers.add((ex, sym))

        tickers = [{"exchange": ex, "symbol": sym} for ex, sym in tickers]
        print(f"Final list of Tickers: {tickers}")

        cache = CacheManager()
        for ticker_key, purchase_list in purchases_by_ticker.items():
            cache.save_purchases(ticker_key, purchase_list)

        #  Scrape
        semaphore = asyncio.Semaphore(2)

        async def scrape_with_limit(ticker):
            async with semaphore:
                print(f"Scraping: {ticker}")
                scraper = StockAnalysisScraper(ticker=ticker)
                return await scraper.scrape()

        tasks = [scrape_with_limit(ticker) for ticker in tickers]
        results = await asyncio.gather(*tasks)

        cache.save_batch(results)

        return results


if __name__ == "__main__":
    import asyncio

    async def main():

        start = datetime.now()

        manager = PortfolioManager()
        results = await manager.run()

        import json

        with open("filename.json", "w") as f:
            json.dump(results, f, indent=2)

        print(datetime.now() - start)

    asyncio.run(main())
