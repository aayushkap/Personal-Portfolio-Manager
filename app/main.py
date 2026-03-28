from app.scraper.data_collector import StockAnalysisScraper
from app.data.cache_manager import CacheManager
from collections import defaultdict
import asyncio


class PortfolioManager:
    def __init__(self):
        pass

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
