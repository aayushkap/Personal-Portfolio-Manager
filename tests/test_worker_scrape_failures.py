import unittest
from unittest.mock import patch

from app import worker


class SourceBlockQueueTests(unittest.TestCase):
    def setUp(self):
        worker._scrape_failures.clear()
        worker._scrape_cooldown_until.clear()

    def tearDown(self):
        worker._scrape_failures.clear()
        worker._scrape_cooldown_until.clear()

    @patch("app.worker.time.time", return_value=1_000.0)
    def test_source_block_quarantines_ticker_on_first_attempt(self, _time):
        until = worker._quarantine_source_blocked_ticker("NYSE:ZETA")

        self.assertEqual(worker._scrape_failures["NYSE:ZETA"], 1)
        self.assertEqual(until, 1_000.0 + worker._FAILURE_COOLDOWN_SECS)
        self.assertEqual(worker._scrape_cooldown_until["NYSE:ZETA"], until)
