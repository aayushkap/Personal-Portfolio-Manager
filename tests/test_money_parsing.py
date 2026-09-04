import json
import unittest
from unittest.mock import patch

from app.data.fx import FX_PAIRS
from app.hql.queries.portfolio import PortfolioQuery
from app.hql.repositories import CacheRepository, FXService
from app.services.analytics import AnalyticsModule
from app.utils.fin import parse_money
from app.utils.parsers import parse_money_string


class MoneyParsingTests(unittest.TestCase):
    def test_parses_ukw_pound_dividend(self):
        self.assertEqual(parse_money_string("£0.0268"), (0.0268, "GBP"))

    def test_parses_supported_symbol_currencies(self):
        self.assertEqual(parse_money_string("$1.25"), (1.25, "USD"))
        self.assertEqual(parse_money_string("€1.25"), (1.25, "EUR"))
        self.assertEqual(parse_money_string("1.25€"), (1.25, "EUR"))
        self.assertEqual(parse_money_string("EUR 1.25"), (1.25, "EUR"))
        self.assertEqual(parse_money_string("1.25 EUR"), (1.25, "EUR"))
        self.assertEqual(parse_money_string("CA$1.25"), (1.25, "CAD"))

    def test_legacy_money_parser_uses_the_same_currency_rules(self):
        self.assertEqual(parse_money("£0.0268"), (0.0268, "GBP"))
        self.assertEqual(parse_money("GBX 102.80"), (102.8, "GBX"))

    def test_eur_currency_is_inferred_from_transaction_details(self):
        repo = CacheRepository(cache=None)
        self.assertEqual(
            repo.resolve_currency({"purchase_details": [{"cost_per_share": "€42.50"}]}),
            "EUR",
        )

    def test_eur_amounts_are_converted_to_aed(self):
        with patch(
            "app.hql.repositories.load_fx_rates",
            return_value={"AED": 1.0, "EUR": 4.0},
        ):
            self.assertEqual(FXService().to_aed(42.5, "EUR"), 170.0)

    def test_eur_to_aed_rate_is_fetched(self):
        self.assertIn(
            {"tv_exchange": "FX_IDC", "symbol": "EURAED"},
            FX_PAIRS,
        )

    def test_eur_transactions_are_converted_to_aed(self):
        class StubCacheRepository:
            def list_tickers(self):
                return ["EPA:AI"]

            def get_raw_ticker(self, ticker):
                return {
                    "purchase_details": [
                        {
                            "purchase_date": "2026-09-01",
                            "transaction": "BUY",
                            "shares": "2",
                            "cost_per_share": "€42.50",
                            "total_cost": "EUR 85.00",
                        }
                    ]
                }

            def resolve_currency(self, raw):
                return "EUR"

        class StubFX:
            def to_aed(self, value, currency):
                return value * 4 if value is not None and currency == "EUR" else value

        query = PortfolioQuery(StubCacheRepository(), price_repo=None, fx=StubFX())
        transaction = query.transactions().iloc[0]

        self.assertEqual(transaction["currency"], "EUR")
        self.assertEqual(transaction["price_aed"], 170.0)
        self.assertEqual(transaction["total_cost_aed"], 340.0)

    def test_pnl_replaces_missing_exchange_with_json_null(self):
        class StubPortfolio:
            def holdings(self):
                import pandas as pd

                return pd.DataFrame(
                    [
                        {
                            "ticker": "XETR:SIE",
                            "shares": 1,
                            "last_price_aed": 100,
                            "market_value_aed": 100,
                            "cost_basis_aed": 90,
                        }
                    ]
                )

            def transactions(self):
                import pandas as pd

                return pd.DataFrame(
                    [
                        {
                            "ticker": "XETR:SIE",
                            "transaction": "buy",
                            "shares": 1,
                            "total_cost_aed": 90,
                            "sector": "Industrials",
                            "exchange": float("nan"),
                        }
                    ]
                )

            def dividends(self):
                import pandas as pd

                return pd.DataFrame()

        class StubHQL:
            def portfolio(self):
                return StubPortfolio()

        module = object.__new__(AnalyticsModule)
        module.hql = StubHQL()

        result = module.get_pnl(mode="price_return")

        self.assertIsNone(result["positions"][0]["exchange"])
        json.dumps(result, allow_nan=False)


if __name__ == "__main__":
    unittest.main()
