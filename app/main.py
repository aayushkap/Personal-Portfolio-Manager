import gspread
import os
from dotenv import load_dotenv

from data.data_collector import StockAnalysisScraper
from app.data.cache_manager import CacheManager
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
        try:
            gc = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)
            sh = gc.open_by_key(SPREADSHEET_ID)
            worksheet = sh.get_worksheet(0)

            # Get values (for data) and formulas (for IMAGE cells) separately
            rows = worksheet.get_all_records()  # rendered values
            formula_rows = worksheet.get_all_values(
                value_render_option="FORMULA"
            )  # raw formulas

            # Build a map of row_index → logo_url by parsing IMAGE() formula
            import re

            logo_col_index = None
            headers = formula_rows[0] if formula_rows else []

            # Find which column is "Logo"
            if "Logo" in headers:
                logo_col_index = headers.index("Logo")

            for i, row in enumerate(rows):
                if logo_col_index is not None and i + 1 < len(formula_rows):
                    formula_cell = formula_rows[i + 1][
                        logo_col_index
                    ]  # +1 to skip header
                    # Parse: =IMAGE("https://...") or =IMAGE(A1) or =IMAGE("url", mode)
                    match = re.search(r'IMAGE\("([^"]+)"', formula_cell, re.IGNORECASE)
                    if match:
                        row["logo_url"] = match.group(1)
                    else:
                        row["logo_url"] = None
                else:
                    row["logo_url"] = None

            return [r for r in rows if r.get("Symbol") and ":" in str(r.get("Symbol"))]

        except Exception:
            import traceback

            traceback.print_exc()

    def get_tickers(self) -> list[dict]:
        investments = self.fetch_investments()
        if not investments:
            return []
        seen = set()
        tickers = []
        for inv in investments:
            symbol = inv.get("Symbol", "")
            if ":" in symbol and symbol not in seen:
                seen.add(symbol)
                exchange, sym = symbol.split(":", 1)
                tickers.append(
                    {"exchange": exchange.upper().strip(), "symbol": sym.strip()}
                )
        return tickers

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
        semaphore = asyncio.Semaphore(1)

        async def scrape_with_limit(ticker):
            async with semaphore:
                print(f"Scraping: {ticker}")
                scraper = StockAnalysisScraper(ticker=ticker)
                return await scraper.scrape()

        tasks = [scrape_with_limit(ticker) for ticker in tickers]
        results = await asyncio.gather(*tasks)

        cache.save_batch(results)

        return results


# if __name__ == "__main__":
#     import asyncio

#     async def main():

#         start = datetime.now()

#         manager = PortfolioManager()
#         results = await manager.run()

#         import json

#         with open("filename.json", "w") as f:
#             json.dump(results, f, indent=2)

#         print(datetime.now() - start)

#     asyncio.run(main())
