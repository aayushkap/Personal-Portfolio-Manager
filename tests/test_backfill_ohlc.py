import unittest
from unittest.mock import patch

from app.scraper.backfill_ohlc import _remove_alerts_without_conditions


class _FakeScreener:
    payload = {
        "generated_at": "2026-09-23T00:00:00Z",
        "alerts": [],
    }
    persisted = None

    @classmethod
    def read(cls):
        return cls.payload

    def _persist(self, alerts):
        type(self).persisted = alerts


class BackfillAlertCleanupTests(unittest.TestCase):
    def setUp(self):
        _FakeScreener.payload = {
            "generated_at": "2026-09-23T00:00:00Z",
            "alerts": [
                {"ticker": "NASDAQ:KEEP", "ready_to_buy": False},
                {"ticker": "NYSE:REMOVE", "ready_to_buy": True},
            ],
        }
        _FakeScreener.persisted = None

    @patch("app.scraper.backfill_ohlc.WatchlistAIScreener", _FakeScreener)
    def test_removes_alerts_when_criteria_has_been_cleared(self):
        removed = _remove_alerts_without_conditions(
            [
                {"ticker": "NASDAQ:KEEP", "criteria": "Price below $100"},
                {"ticker": "NYSE:REMOVE", "criteria": None},
            ]
        )

        self.assertEqual(removed, ["NYSE:REMOVE"])
        self.assertEqual(
            _FakeScreener.persisted,
            [{"ticker": "NASDAQ:KEEP", "ready_to_buy": False}],
        )

    @patch("app.scraper.backfill_ohlc.WatchlistAIScreener", _FakeScreener)
    def test_does_not_rewrite_alert_file_when_nothing_is_stale(self):
        removed = _remove_alerts_without_conditions(
            [
                {"ticker": "NASDAQ:KEEP", "criteria": "Price below $100"},
                {"ticker": "NYSE:REMOVE", "criteria": "Earnings improve"},
            ]
        )

        self.assertEqual(removed, [])
        self.assertIsNone(_FakeScreener.persisted)
