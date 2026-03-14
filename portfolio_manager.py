import gspread
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

from stock_analysis_scraper import StockAnalysisScraper

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

        # Collect unique tickers
        for investment in investments:
            instrument = investment.get("Symbol")
            split = instrument.split(":")

            exchange = split[0] or None
            ticker = split[1] or None

            if exchange and ticker:
                tickers.add((exchange, ticker))

        tickers = [{"exchange": ex, "symbol": sym} for ex, sym in tickers]

        print(f"Final list of Tickers: {tickers}")

        semaphore = asyncio.Semaphore(2)

        async def scrape_with_limit(ticker):
            async with semaphore:
                print(f"Scraping: {ticker}")
                scraper = StockAnalysisScraper(ticker=ticker)
                return await scraper.scrape()

        tasks = [scrape_with_limit(ticker) for ticker in tickers]

        results = await asyncio.gather(*tasks)

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
