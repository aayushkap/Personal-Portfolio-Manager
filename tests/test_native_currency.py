from copy import deepcopy
from datetime import date, timedelta
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from app.hql.queries.portfolio import PortfolioQuery
from app.hql.queries.ticker import TickerQuery
from app.hql.repositories import CacheRepository, FXService, PriceRepository
from app.scraper.sa import StockAnalysisScraper
from app.services.filters import DateRange, PortfolioFilters
from app.services.holdings import HoldingsModule
from app.services.watchlist import WatchlistModule


class NativeCurrencyTests(unittest.TestCase):
    def setUp(self):
        self.today = date.today()
        self.raw = {
            "ticker": "LSE:TEST",
            "overview": {
                "quote_currency": "GBX",
                "stats": {
                    "Previous Close": "112",
                    "Open": "112",
                    "Price Target": "150",
                },
            },
            "purchase_details": [
                {
                    "purchase_date": (self.today - timedelta(days=60)).isoformat(),
                    "transaction": "BUY",
                    "shares": "10",
                    "cost_per_share": "GBX 100",
                    "total_cost": "GBX 1000",
                }
            ],
            "dividends": {
                "rows": [
                    {
                        "Ex-Dividend Date": (
                            self.today - timedelta(days=10)
                        ).isoformat(),
                        "Pay Date": (self.today - timedelta(days=2)).isoformat(),
                        "Cash Amount": "£0.20",
                    }
                ]
            },
        }
        rates = patch(
            "app.hql.repositories.load_fx_rates",
            return_value={
                "AED": 1.0,
                "GBP": 5.0,
                "GBX": 0.05,
                "USD": 3.67,
                "EUR": 4.0,
            },
        )
        rates.start()
        self.addCleanup(rates.stop)
        self.fx = FXService()
        self.cache = Mock()
        self.cache.load.side_effect = (
            lambda ticker: self.raw
            if ticker == "LSE:TEST"
            else {
                "ticker": ticker,
                "overview": {"quote_currency": "USD"},
            }
        )
        self.repo = CacheRepository(self.cache)
        self.repo.list_tickers = Mock(return_value=["LSE:TEST"])
        self.db = Mock()
        self.db.get.side_effect = lambda ticker, limit: [
            {
                "symbol": ticker,
                "timestamp": f"{self.today - timedelta(days=39 - index)}T12:00:00+04:00",
                "close": 112.0 if ticker == "LSE:TEST" else 50.0,
                "volume": 100,
            }
            for index in range(40)
        ]
        self.db.get_latest.return_value = {"close": 112.0}
        self.prices = PriceRepository(self.db, self.fx, self.repo)
        receipt = {
            "ticker": "LSE:TEST",
            "received_date": (self.today - timedelta(days=2)).isoformat(),
            "shares_held": "10",
            "dividend_per_share": "GBP 0.20",
            "total_dividend": "GBP 2.00",
            "tax_paid": "GBP 0.00",
        }
        self.portfolio = PortfolioQuery(
            self.repo,
            self.prices,
            self.fx,
            confirmed_dividends_loader=lambda: [receipt],
        )

    def ticker(self, ticker="LSE:TEST"):
        return TickerQuery(ticker, self.repo, self.prices, self.fx)

    def module(self, cls):
        module = object.__new__(cls)
        module._cache = self.cache
        module._db = self.db
        module._fx = None
        module.hql = SimpleNamespace(
            ticker=self.ticker, portfolio=lambda: self.portfolio, fx=self.fx
        )
        return module

    def detail(self, cls, overlays=None):
        module = self.module(cls)
        with (
            patch("app.services.holdings.HoldingsNewsAgent") as news,
            patch("app.services.watchlist.WatchlistAIScreener") as alerts,
        ):
            news.return_value.merge_news.side_effect = lambda rows: rows
            alerts.return_value.merge_alerts.side_effect = lambda rows: rows
            if cls is HoldingsModule:
                return module.get_holding_detail("LSE:TEST", overlays=overlays)
            return module.get_watchlist_detail("LSE:TEST", overlays=overlays)

    def test_native_prices_do_not_change_aed_reads_or_portfolio_aggregation(self):
        ticker = self.ticker()
        self.assertEqual(ticker.ohlcv(native=True).iloc[-1]["close"], 112.0)
        self.assertAlmostEqual(ticker.ohlcv().iloc[-1]["close"], 5.6)
        self.assertEqual(ticker.overview(native=True)["price_target"], 150.0)
        self.assertEqual(ticker.overview()["price_target"], 7.5)
        self.assertAlmostEqual(
            self.portfolio.holdings().iloc[0]["market_value_aed"], 56.0
        )
        self.fx._rates["GBX"] = 0.06
        self.assertEqual(ticker.ohlcv(native=True).iloc[-1]["close"], 112.0)

    def test_both_details_use_native_charts_indicators_and_transaction_amounts(self):
        before = deepcopy(self.raw)
        for cls in (HoldingsModule, WatchlistModule):
            with self.subTest(endpoint=cls.__name__):
                detail = self.detail(cls, ["SMA_20"])
                self.assertEqual(detail["currency"], "GBX")
                self.assertEqual(detail["chart_currency"], "GBX")
                self.assertEqual(detail["chart"][-1]["close"], 112.0)
                self.assertEqual(detail["overlays"]["SMA_20"][-1]["value"], 112.0)
                self.assertEqual(
                    detail["fundamentals"]["snapshot"]["price_target"], 150.0
                )
                self.assertEqual(detail["transactions"][0]["price"], 100.0)
                self.assertEqual(detail["transactions"][0]["total"], 1000.0)
                received = detail["transactions"][1]
                self.assertEqual(received["type"], "DIVIDEND")
                self.assertEqual(received["currency"], "GBX")
                self.assertEqual(received["price"], 20.0)
                self.assertEqual(received["total"], 200.0)
                self.assertEqual(self.raw, before)

    def test_cross_currency_chart_and_its_indicators_use_aed(self):
        for cls in (HoldingsModule, WatchlistModule):
            with self.subTest(endpoint=cls.__name__):
                detail = self.detail(cls, ["NASDAQ:OTHER", "SMA_20"])
                self.assertEqual(detail["currency"], "GBX")
                self.assertEqual(detail["chart_currency"], "AED")
                self.assertEqual(detail["chart"][-1]["close"], 5.6)
                self.assertEqual(detail["overlays"]["SMA_20"][-1]["value"], 5.6)
                self.assertEqual(detail["overlays"]["NASDAQ:OTHER"][-1]["value"], 183.5)
                self.assertEqual(detail["transactions"][0]["price"], 100.0)

    def test_individual_cards_are_native_with_explicit_aed_values(self):
        with patch("app.services.holdings.HoldingsNewsAgent") as news:
            news.return_value.merge_news.side_effect = lambda rows: rows
            rows = self.module(HoldingsModule).get_holdings_list(
                PortfolioFilters(
                    date_range=DateRange(
                        start=self.today - timedelta(days=60), end=self.today
                    )
                )
            )
        self.assertEqual(rows[0]["currency"], "GBX")
        self.assertEqual(rows[0]["current_price"], 112.0)
        self.assertEqual(rows[0]["total_value"], 1120.0)
        self.assertEqual(rows[0]["cost_basis"], 1000.0)
        self.assertAlmostEqual(rows[0]["total_value_aed"], 56.0)
        self.assertEqual(rows[0]["sparkline"][-1]["close"], 112.0)

    def test_watchlist_prices_stay_native_even_without_purchase_currency(self):
        self.raw["purchase_details"] = []
        rows = self.db.get("LSE:TEST", limit=50_000)
        for index, row in enumerate(rows[:-1]):
            row["close"] = 100.0 + index * 0.3
        self.db.get.side_effect = None
        self.db.get.return_value = rows
        with patch("app.services.watchlist.WatchlistAIScreener") as alerts:
            alerts.return_value.merge_alerts.side_effect = lambda rows: rows
            rows = self.module(WatchlistModule).get_watchlist([{"ticker": "LSE:TEST"}])
        self.assertEqual(rows[0]["currency"], "GBX")
        self.assertEqual(rows[0]["current_price"], 112.0)
        self.assertEqual(rows[0]["mom_pct"], 9.37)

    def test_pound_transactions_are_displayed_in_pence_for_a_pence_quote(self):
        self.raw["purchase_details"][0]["cost_per_share"] = "GBP 1.00"
        self.raw["purchase_details"][0]["total_cost"] = "GBP 10.00"
        detail = self.detail(HoldingsModule)
        self.assertEqual(detail["transactions"][0]["currency"], "GBX")
        self.assertEqual(detail["transactions"][0]["price"], 100.0)
        self.assertEqual(detail["transactions"][0]["total"], 1000.0)

    def test_missing_currency_and_fx_are_not_mislabeled_or_guessed(self):
        self.assertIsNone(self.repo.resolve_currency({}, default=None))
        self.assertEqual(self.repo.resolve_currency({}), "AED")
        self.assertIsNone(self.fx.convert(10.0, "AED", None))
        self.assertIsNone(self.fx.convert(10.0, "AED", "JPY"))
        self.assertEqual(self.fx.convert(2.0, "GBP", "GBX"), 200.0)
        self.assertEqual(self.fx.convert(200.0, "GBX", "GBP"), 2.0)

    def test_quote_unit_overrides_exchange_currency_without_guessing_london_etfs(self):
        parse = StockAnalysisScraper._quote_currency_from_text
        self.assertEqual(parse("Currency is GBP · Price in GBX"), "GBX")
        self.assertEqual(parse("Currency is GBP · Price in USD"), "USD")
        self.assertEqual(parse("Currency is EUR"), "EUR")
        self.assertIsNone(parse("No currency metadata"))
        raw = {
            "overview": {"quote_currency": "USD"},
            "purchase_details": [{"cost_per_share": "GBP 10"}],
        }
        self.assertEqual(self.repo.resolve_currency(raw), "USD")


if __name__ == "__main__":
    unittest.main()
