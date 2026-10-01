from __future__ import annotations

import tempfile
import threading
import unittest
import sys
from datetime import date
from pathlib import Path

from app.core.singleton import SingletonLock
from app.data.cache import Cache
from app.data.db import DB
from app.hql import HQL


class DataBoundaryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.database_path = self.root / "portfolio.db"
        DB.bootstrap(self.database_path)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_sqlite_is_wal_readable_and_write_access_is_explicit(self) -> None:
        writer = DB(self.database_path, read_only=False)
        writer.upsert("DFM:TEST", "2026-01-01T10:00:00+04:00", 12.5, 100)

        reader = DB(self.database_path)
        self.assertEqual(reader.get_latest("DFM:TEST")["close"], 12.5)

        with reader.connection() as conn:
            self.assertEqual(conn.execute("PRAGMA journal_mode").fetchone()[0], "wal")

        with self.assertRaises(PermissionError):
            reader.upsert("DFM:TEST", "2026-01-01T10:15:00+04:00", 13.0)

    def test_read_connections_are_short_lived(self) -> None:
        writer = DB(self.database_path, read_only=False)
        writer.upsert("DFM:TEST", "2026-01-01T10:00:00+04:00", 12.5)
        reader = DB(self.database_path)

        for _ in range(100):
            self.assertEqual(len(reader.get("DFM:TEST")), 1)

    def test_reader_and_single_writer_can_run_concurrently(self) -> None:
        writer = DB(self.database_path, read_only=False, busy_timeout_ms=2_000)
        reader = DB(self.database_path, busy_timeout_ms=2_000)
        errors: list[Exception] = []

        def write_prices() -> None:
            try:
                for index in range(50):
                    writer.upsert(
                        "DFM:TEST",
                        f"2026-01-01T10:{index:02d}:00+04:00",
                        float(index),
                    )
            except Exception as exc:  # pragma: no cover - asserted below
                errors.append(exc)

        writer_thread = threading.Thread(target=write_prices)
        writer_thread.start()
        try:
            for _ in range(100):
                reader.get("DFM:TEST", limit=100)
        except Exception as exc:  # pragma: no cover - asserted below
            errors.append(exc)
        writer_thread.join()

        self.assertEqual(errors, [])
        self.assertEqual(len(reader.get("DFM:TEST", limit=100)), 50)

    def test_cache_requires_explicit_writer_and_preserves_hql_reads(self) -> None:
        writer_cache = Cache(self.root / "cache", read_only=False)
        payload = {
            "ticker": "DFM:TEST",
            "purchase_details": [{"cost_per_share": "AED 10"}],
        }
        self.assertTrue(writer_cache.save("DFM:TEST", payload))

        reader_cache = Cache(self.root / "cache")
        self.assertEqual(reader_cache.load("DFM:TEST")["ticker"], "DFM:TEST")
        self.assertFalse(reader_cache.save("DFM:TEST", {"ticker": "changed"}))
        self.assertEqual(reader_cache.load("DFM:TEST")["ticker"], "DFM:TEST")

        writer = DB(self.database_path, read_only=False)
        writer.upsert("DFM:TEST", "2026-01-01T10:00:00+04:00", 12.5)
        hql = HQL(cache=reader_cache, db=DB(self.database_path))
        prices = hql.ticker("DFM:TEST").ohlcv(
            start=date(2026, 1, 1), end=date(2026, 1, 1)
        )
        self.assertEqual(float(prices.iloc[0]["close"]), 12.5)

    def test_only_one_worker_lock_can_be_held(self) -> None:
        first = SingletonLock(self.root / "worker.lock")
        second = SingletonLock(self.root / "worker.lock")
        first.acquire()
        try:
            with self.assertRaises(RuntimeError):
                second.acquire()
        finally:
            first.release()

        second.acquire()
        second.release()

    def test_api_import_does_not_start_or_import_the_scheduled_worker(self) -> None:
        from app.api import app

        self.assertEqual(app.title, "HSFW BE")
        self.assertNotIn("app.worker", sys.modules)
