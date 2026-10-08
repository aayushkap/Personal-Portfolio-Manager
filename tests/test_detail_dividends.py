from copy import deepcopy
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from app.api.holdings import get_holding_detail
from app.api.watchlist import get_watchlist_detail
from app.hql.queries.ticker import TickerQuery
from app.services.holdings import HoldingsModule
from app.services.watchlist import WatchlistModule


class DetailDividendTests(unittest.TestCase):
    def detail(self, cls, raw):
        cache = Mock()
        cache.get_raw_ticker.return_value = raw
        cache.resolve_currency.return_value = "GBP"
        query = TickerQuery("LSE:TEST", cache, Mock(), Mock())
        module = object.__new__(cls)
        module.hql = SimpleNamespace(ticker=Mock(return_value=query), portfolio=Mock())
        module._build_overlays = Mock(return_value={})
        module._build_chart = Mock(return_value=[])
        with (
            patch.object(HoldingsModule, "_build_transactions", return_value=[]),
            patch("app.services.holdings.HoldingsNewsAgent"),
            patch("app.services.watchlist.WatchlistAIScreener") as screener,
        ):
            screener.return_value.merge_alerts.side_effect = lambda rows: rows
            if cls is HoldingsModule:
                return get_holding_detail("LSE:TEST", "1m", [], module)
            return get_watchlist_detail("LSE:TEST", "1m", [], module)

    def test_both_detail_apis_return_complete_dividend_table_under_fundamentals(self):
        raw = {
            "ticker": "LSE:TEST",
            "dividends": {
                "headers": [
                    "Ex-Dividend Date",
                    "Cash Amount",
                    "Record Date",
                    "Pay Date",
                ],
                "rows": [
                    {
                        "Ex-Dividend Date": f"{2026 - index}-08-13",
                        "Cash Amount": "£0.0268",
                        "Record Date": f"{2026 - index}-08-14",
                        "Pay Date": f"{2026 - index}-08-28",
                    }
                    for index in range(20)
                ],
            },
        }
        expected = deepcopy(raw)
        for cls in (HoldingsModule, WatchlistModule):
            with self.subTest(endpoint=cls.__name__):
                response = self.detail(cls, raw)
                self.assertEqual(
                    response["fundamentals"]["dividends"], expected["dividends"]
                )
                self.assertEqual(raw, expected)

    def test_empty_missing_and_failed_histories_return_empty_tables(self):
        for section in ({"headers": [], "rows": []}, None, {"error": "blocked"}):
            for cls in (HoldingsModule, WatchlistModule):
                with self.subTest(endpoint=cls.__name__, section=section):
                    response = self.detail(cls, {"dividends": section})
                    self.assertEqual(
                        response["fundamentals"]["dividends"],
                        {"headers": [], "rows": []},
                    )


if __name__ == "__main__":
    unittest.main()
