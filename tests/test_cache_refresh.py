import tempfile
import unittest
from pathlib import Path

from app.data.cache import Cache


class CacheRefreshTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.cache = Cache(Path(self.temp_dir.name), read_only=False)
        self.ticker = "NYSE:MCD"
        self.previous_dividends = {
            "symbol": "MCD",
            "rows": [{"Ex-Dividend Date": "2026-09-01", "Cash Amount": "$1.86"}],
        }
        self.assertTrue(
            self.cache.save(
                self.ticker,
                {
                    "scraped_at": "2026-09-07T00:00:00+04:00",
                    "overview": {"symbol": "MCD"},
                    "dividends": self.previous_dividends,
                },
            )
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_partial_refresh_keeps_previous_dividend_history(self):
        self.assertTrue(
            self.cache.save(
                self.ticker,
                {
                    "scraped_at": "2026-09-14T00:00:00+04:00",
                    "overview": {"symbol": "MCD", "price": "$260"},
                    "dividends": None,
                },
            )
        )

        saved = self.cache.load(self.ticker)
        self.assertEqual(saved["overview"]["price"], "$260")
        self.assertEqual(saved["dividends"], self.previous_dividends)
        self.assertEqual(saved["last_updated"], "2026-09-14T00:00:00+04:00")

    def test_empty_successful_dividend_table_replaces_previous_history(self):
        replacement = {"symbol": "MCD", "rows": []}
        self.assertTrue(
            self.cache.save(
                self.ticker,
                {
                    "overview": {"symbol": "MCD"},
                    "dividends": replacement,
                },
            )
        )

        self.assertEqual(self.cache.load(self.ticker)["dividends"], replacement)

    def test_error_only_dividend_table_keeps_previous_history(self):
        self.assertTrue(
            self.cache.save(
                self.ticker,
                {
                    "overview": {"symbol": "MCD"},
                    "dividends": {"rows": [{"error": "timed out"}]},
                },
            )
        )

        self.assertEqual(
            self.cache.load(self.ticker)["dividends"], self.previous_dividends
        )
