from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from app.services.holdings import HoldingsModule
from app.services.watchlist import WatchlistModule


class DetailRawFundamentalsTests(unittest.TestCase):
    def make_module(self, cls, raw):
        module = object.__new__(cls)
        query = Mock()
        query.raw.return_value = raw
        query.info.return_value = {"last_updated": "2026-10-07"}
        module.hql = SimpleNamespace(ticker=Mock(return_value=query), portfolio=Mock())
        module._build_overlays = Mock(return_value={})
        module._build_chart = Mock(return_value=[])
        module._build_fundamentals = Mock(return_value={
            "dividends_yields": {"dividend_yield": 0.06379},
        })
        module._build_transactions = Mock(return_value=[])
        return module

    def response(self, module):
        if isinstance(module, HoldingsModule):
            with patch("app.services.holdings.HoldingsNewsAgent") as news:
                news.return_value.merge_news.return_value = []
                return module.get_holding_detail("LSE:TEST")
        with patch("app.services.watchlist.WatchlistAIScreener") as screener, patch.object(
            HoldingsModule, "_build_transactions", return_value=[]
        ):
            screener.return_value.merge_alerts.side_effect = lambda rows: rows
            return module.get_watchlist_detail("LSE:TEST")

    def test_both_details_preserve_complete_dividend_history_and_other_cache_fields(self):
        raw = {
            "ticker": "LON:TEST",
            "dividends": {
                "headers": ["Ex-Dividend Date", "Cash Amount", "Record Date", "Pay Date"],
                "rows": [{
                    "Ex-Dividend Date": f"{2026 - index}-09-24",
                    "Cash Amount": "£0.2805",
                    "Record Date": f"{2026 - index}-09-25",
                    "Pay Date": f"{2026 - index}-10-29",
                } for index in range(10)],
            },
            "statistics": {"sections": {"Important Dates": {"Earnings Date": "2026-11-06"}}},
            "financials": {"rows": [{"Fiscal Year": "Revenue", "TTM": "40,499"}]},
            "new_provider_data": {"unavailable": "n/a", "zero": 0},
        }
        expected = deepcopy(raw)
        for cls in [HoldingsModule, WatchlistModule]:
            with self.subTest(endpoint=cls.__name__):
                response = self.response(self.make_module(cls, raw))
                self.assertEqual(response["raw_data"], expected)
                self.assertEqual(len(response["raw_data"]["dividends"]["rows"]), 10)
                self.assertEqual(response["fundamentals"]["dividends_yields"]["dividend_yield"], 0.06379)
                self.assertEqual(response["ticker"], "LSE:TEST")
                self.assertEqual(raw, expected)

    def test_empty_dividend_history_is_preserved_without_inventing_payments(self):
        raw = {"overview": {"stats": {"EPS": "2.00"}}, "dividends": {"rows": []}}
        for cls in [HoldingsModule, WatchlistModule]:
            with self.subTest(endpoint=cls.__name__):
                response = self.response(self.make_module(cls, raw))
                self.assertEqual(response["raw_data"]["dividends"]["rows"], [])
                self.assertEqual(response["raw_data"]["overview"], raw["overview"])


if __name__ == "__main__":
    unittest.main()
