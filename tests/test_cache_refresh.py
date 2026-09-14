import tempfile
import unittest
from pathlib import Path

from app.data.cache import Cache
from app.scraper.sa import StockAnalysisScraper


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

    def test_explicit_no_dividend_overview_skips_the_dividend_page(self):
        self.assertTrue(
            StockAnalysisScraper._overview_confirms_no_dividends(
                {"stats": {"Dividend (ttm)": "n/a"}}
            )
        )

    def test_missing_dividend_overview_value_does_not_skip_retries(self):
        self.assertFalse(
            StockAnalysisScraper._overview_confirms_no_dividends({"stats": {}})
        )

    def test_parses_server_rendered_dividend_table(self):
        headers, rows = StockAnalysisScraper._parse_dividend_html(
            """
            <div class="table-wrap"><table>
              <thead><tr><th>Ex-Dividend Date</th><th>Cash Amount</th></tr></thead>
              <tbody><tr><td>Sep 1, 2026</td><td>$1.86</td></tr></tbody>
            </table></div>
            """
        )
        self.assertEqual(headers, ["Ex-Dividend Date", "Cash Amount"])
        self.assertEqual(
            rows, [{"Ex-Dividend Date": "Sep 1, 2026", "Cash Amount": "$1.86"}]
        )

    def test_normalizes_header_fragmented_by_nested_html(self):
        self.assertEqual(
            StockAnalysisScraper._normalise_dividend_header("Ex-Div idend Date"),
            "Ex-Dividend Date",
        )

    def test_explicit_no_dividend_history_html_is_a_valid_empty_table(self):
        headers, rows = StockAnalysisScraper._parse_dividend_html(
            "<title>Netflix Dividend History</title>"
            "<p>There is no dividend history available for Netflix. "
            "This usually means that the stock has never paid a dividend.</p>"
        )
        self.assertEqual(headers, [])
        self.assertEqual(rows, [])
