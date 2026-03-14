import gspread
import os
import json
from datetime import datetime, timedelta
from tradingview_scraper.symbols.overview import Overview
from tradingview_scraper.symbols.fundamental_graphs import FundamentalGraphs
from tradingview_scraper.symbols.news import NewsScraper
from tradingview_scraper.symbols.cal import CalendarScraper
from tvDatafeed import TvDatafeed, Interval
from dotenv import load_dotenv

load_dotenv()

SERVICE_ACCOUNT_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    os.getenv("GOOGLE_SHEETS_SERVICE_ACCOUNT_FILE"),
)
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
TV_USERNAME = os.getenv("TV_USERNAME", "")  # optional, add to .env
TV_PASSWORD = os.getenv("TV_PASSWORD", "")  # optional, add to .env


class PortfolioManager:

    def __init__(self):
        self.tv = TvDatafeed(TV_USERNAME, TV_PASSWORD) if TV_USERNAME else TvDatafeed()
        self.overview_api = Overview()
        self.fg_api = FundamentalGraphs()
        self.news_api = NewsScraper()
        self.cal_api = CalendarScraper()

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

    #  BUILD HOLDINGS MAP
    def build_holdings(self, sheet_data):
        holdings = {}
        for row in sheet_data:
            tv_symbol = row["Symbol"]
            if ":" not in tv_symbol:
                continue
            short_name = tv_symbol.split(":")[1]
            holdings[short_name] = {
                "tv_symbol": tv_symbol,
                "platform": row.get("Platform", ""),
                "purchase_date": row.get("Purchase Date", ""),
                "shares": row.get("Shares", 0),
                "cost_per_share": row.get("Cost per Share", ""),
                "commission_paid": row.get("Commision Paid", ""),
                "total_cost": row.get("Total Cost", ""),
                "next_div_date": row.get("Next Expected Dividend Date", ""),
                "next_div_amount": row.get("Next Expected Dividend Amount", ""),
            }
        return holdings

    #  OHLCV BARS
    def fetch_ohlcv(self, symbol: str, exchange: str, n_bars: int = 0) -> list:
        try:
            df = self.tv.get_hist(
                symbol=symbol,
                exchange=exchange,
                interval=Interval.in_daily,
                n_bars=n_bars,
            )
            if df is None or df.empty:
                return []
            df = df.reset_index()
            df["datetime"] = df["datetime"].astype(str)
            return df[["datetime", "open", "high", "low", "close", "volume"]].to_dict(
                orient="records"
            )
        except Exception as e:
            print(f"  ⚠ OHLCV failed for {exchange}:{symbol} — {e}")
            return []

    #  HISTORICAL DIVIDENDS
    def fetch_dividend_history(self, tv_symbol: str) -> dict:
        try:
            result = self.fg_api.get_dividends(symbol=tv_symbol)
            if not result or result.get("status") != "success":
                return {}
            d = result["data"]
            history_raw = d.get("dividends_history", []) or []
            history = [
                {
                    "date": entry.get("payment_date") or entry.get("ex_date"),
                    "ex_date": entry.get("ex_date"),
                    "payment_date": entry.get("payment_date"),
                    "amount": entry.get("amount"),
                    "currency": entry.get("currency"),
                }
                for entry in history_raw
            ]
            return {
                "yield_ttm": d.get("dividends_yield"),
                "dps_fq": d.get("dividends_per_share_fq"),  # latest quarterly DPS
                "payout_ratio": d.get("dividend_payout_ratio_ttm"),
                "next_date": d.get("next_dividend_date"),  # ← next amount date
                "next_amount": d.get("next_dividend_per_share"),  # ← next DPS
                "history": history,
            }
        except Exception as e:
            print(f"  ⚠ Dividend history failed for {tv_symbol} — {e}")
            return {}

    #  ENRICH
    def enrich_with_tradingview(self, holdings):
        # One calendar call for all ME upcoming dividends
        now = datetime.now().timestamp()
        in_90_days = (datetime.now() + timedelta(days=90)).timestamp()
        div_calendar = self.cal_api.scrape_dividends(
            now,
            in_90_days,
            ["middle_east"],
            values=["logoid", "name", "dividends_yield"],
        )
        cal_by_name = {d["name"].upper(): d for d in (div_calendar or [])}

        portfolio = {}

        for short_name, holding in holdings.items():
            tv_symbol = holding["tv_symbol"]
            exchange, sym = tv_symbol.split(":")
            print(f"Fetching {tv_symbol}...")

            # Fundamentals
            fundamentals = {}
            result = self.overview_api.get_symbol_overview(symbol=tv_symbol)
            if result and result.get("status") == "success":
                d = result["data"]
                fundamentals = {
                    "price": d.get("close"),
                    "market_cap": d.get("market_cap_basic"),
                    "pe_ratio": d.get("price_earnings_ttm"),
                    "dividend_yield": d.get("dividends_yield"),
                    "perf_1y": d.get("Perf.Y"),
                    "perf_ytd": d.get("Perf.YTD"),
                    "rsi": d.get("RSI"),
                    "52w_high": d.get("High.All"),
                    "52w_low": d.get("Low.All"),
                    "volume": d.get("volume"),
                    "sector": d.get("sector"),
                }

            # News
            news_items = []
            headlines = self.news_api.scrape_headlines(
                symbol=sym, exchange=exchange, sort="latest"
            )
            if headlines:
                for article in headlines[:5] if isinstance(headlines, list) else []:
                    news_items.append(
                        {
                            "title": article.get("title"),
                            "source": article.get("source"),
                            "published": article.get("published"),
                            "url": article.get("url"),
                        }
                    )

            # Historical dividends + next amount
            div_data = self.fetch_dividend_history(tv_symbol)
            cal_div = cal_by_name.get(short_name.upper(), {})

            # OHLCV bars
            print(f"Fetching OHLCV bars...")
            ohlcv = self.fetch_ohlcv(sym, exchange)

            portfolio[short_name] = {
                "symbol": tv_symbol,
                "position": {
                    "platform": holding["platform"],
                    "purchase_date": holding["purchase_date"],
                    "shares": holding["shares"],
                    "cost_per_share": holding["cost_per_share"],
                    "commission_paid": holding["commission_paid"],
                    "total_cost": holding["total_cost"],
                },
                "dividends": {
                    # From TradingView FundamentalGraphs (live)
                    "next_date": div_data.get("next_date"),
                    "next_amount": div_data.get("next_amount"),
                    "yield_ttm": div_data.get("yield_ttm"),
                    "dps_fq": div_data.get("dps_fq"),
                    "payout_ratio": div_data.get("payout_ratio"),
                    "calendar_yield": cal_div.get("dividends_yield"),
                    "history": div_data.get("history", []),
                },
                "fundamentals": fundamentals,
                "ohlcv": ohlcv,  # full daily bars list
                "ohlcv_count": len(ohlcv),  # quick sanity check
                "news": news_items,
                "last_updated": datetime.now().isoformat(),
            }
            print(f"{len(ohlcv)} bars fetched")

        return portfolio

    #  RUN
    def run(self):
        print("Fetching Google Sheets data...")
        investments = self.fetch_investments()
        if not investments:
            print("No investments found.")
            return {}

        holdings = self.build_holdings(investments)
        portfolio = self.enrich_with_tradingview(holdings)

        return portfolio


if __name__ == "__main__":
    obj = PortfolioManager()
    result = obj.run()

    import json

    with open("filename.json", "w") as f:
        json.dump(result, f, indent=2)
