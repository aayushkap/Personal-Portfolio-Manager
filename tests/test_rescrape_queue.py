import tempfile
import unittest
from pathlib import Path

from app.data.rescrape_queue import RescrapeQueue
from app.scraper.sa import StockAnalysisScraper


class RescrapeQueueTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.queue = RescrapeQueue(Path(self.temp_dir.name))

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_request_is_coalesced_and_discarded_by_identity(self):
        request, already_queued = self.queue.schedule("LSE:EIMI")
        self.assertFalse(already_queued)

        duplicate, already_queued = self.queue.schedule("LSE:EIMI")
        self.assertTrue(already_queued)
        self.assertEqual(duplicate, request)
        self.assertEqual(self.queue.next(), request)

        self.queue.discard(request)
        self.assertIsNone(self.queue.next())

    def test_london_etfs_use_exchange_quote_url(self):
        scraper = StockAnalysisScraper()
        self.assertEqual(
            scraper._get_base_url("LON", "EIMI", is_etf=True),
            "https://stockanalysis.com/quote/lon/eimi",
        )
        self.assertEqual(
            scraper._get_base_url("AMEX", "XLF", is_etf=True),
            "https://stockanalysis.com/etf/xlf",
        )
