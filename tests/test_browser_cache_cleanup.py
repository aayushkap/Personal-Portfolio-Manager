import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from app.scraper import sa


class BrowserCacheCleanupTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.profile = Path(self.temp.name) / "profile"
        self.profile.mkdir()
        self.override = patch.object(
            sa, "STOCKANALYSIS_BROWSER_PROFILE_DIR", self.profile
        )
        self.override.start()
        self.addCleanup(self.override.stop)
        self.cache = self.profile / "Default/Cache/Cache_Data/asset"
        self.code_cache = self.profile / "Default/Code Cache/js/asset"
        self.cookies = self.profile / "Default/Cookies"
        self.storage = self.profile / "Default/Local Storage/leveldb/data"
        self.portfolio = self.profile.parent / "portfolio.db"
        for file in (
            self.cache,
            self.code_cache,
            self.cookies,
            self.storage,
            self.portfolio,
        ):
            file.parent.mkdir(parents=True, exist_ok=True)
            file.write_bytes(b"keep-or-clear")

    def test_clears_only_disposable_caches_and_preserves_daily_cadence(self):
        sa.StockAnalysisScraper._clear_browser_cache_if_due()

        self.assertFalse(self.cache.exists())
        self.assertFalse(self.code_cache.exists())
        for file in (self.cookies, self.storage, self.portfolio):
            self.assertEqual(file.read_bytes(), b"keep-or-clear")
        marker = self.profile / ".cache-cleaned-at"
        self.assertTrue(marker.exists())

        self.cache.parent.mkdir(parents=True)
        self.cache.write_bytes(b"new-cache")
        sa.StockAnalysisScraper()._clear_browser_cache_if_due()
        self.assertTrue(self.cache.exists())

        day_ago = time.time() - sa._BROWSER_CACHE_CLEANUP_INTERVAL_SECONDS - 1
        os.utime(marker, (day_ago, day_ago))
        sa.StockAnalysisScraper._clear_browser_cache_if_due()
        self.assertFalse(self.cache.exists())

    def test_skips_browser_lock_including_dangling_symlink(self):
        lock = self.profile / "SingletonLock"
        for symlink in (False, True):
            with self.subTest(symlink=symlink):
                if symlink:
                    lock.symlink_to("hostname-12345")
                else:
                    lock.touch()
                sa.StockAnalysisScraper._clear_browser_cache_if_due()
                self.assertTrue(self.cache.exists())
                self.assertFalse((self.profile / ".cache-cleaned-at").exists())
                lock.unlink()

    def test_does_not_follow_cache_or_default_symlinks(self):
        outside = self.profile.parent / "outside"
        outside.mkdir()
        sentinel = outside / "sentinel"
        sentinel.write_bytes(b"preserve")
        cache_dir = self.profile / "Default/Cache"
        sa.shutil.rmtree(cache_dir)
        cache_dir.symlink_to(outside, target_is_directory=True)
        sa.StockAnalysisScraper._clear_browser_cache_if_due()
        self.assertEqual(sentinel.read_bytes(), b"preserve")

        sa.shutil.rmtree(self.profile / "Default")
        (self.profile / "Default").symlink_to(outside, target_is_directory=True)
        (self.profile / ".cache-cleaned-at").unlink()
        sa.StockAnalysisScraper._clear_browser_cache_if_due()
        self.assertEqual(sentinel.read_bytes(), b"preserve")
        self.assertFalse((self.profile / ".cache-cleaned-at").exists())

    def test_cleanup_failure_is_nonfatal_and_retried(self):
        with patch.object(sa.shutil, "rmtree", side_effect=PermissionError("busy")):
            with self.assertLogs("app", level="WARNING"):
                sa.StockAnalysisScraper._clear_browser_cache_if_due()
        self.assertTrue(self.cache.exists())
        self.assertFalse((self.profile / ".cache-cleaned-at").exists())
        sa.StockAnalysisScraper._clear_browser_cache_if_due()
        self.assertFalse(self.cache.exists())


class BrowserCacheLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_cleanup_runs_after_close_on_both_paths_including_scrape_failure(
        self,
    ):
        for dividends_only in (False, True):
            for fails in (False, True):
                with self.subTest(dividends_only=dividends_only, fails=fails):
                    scraper = sa.StockAnalysisScraper()
                    context = MagicMock()
                    context.close = AsyncMock()
                    context.new_page = AsyncMock(
                        return_value=MagicMock(close=AsyncMock())
                    )
                    scraper._launch_context = AsyncMock(return_value=context)
                    result = {"ok": True}
                    scrape = AsyncMock(
                        return_value=result,
                        side_effect=RuntimeError("scrape failed") if fails else None,
                    )
                    scraper._scrape_ticker = scrape
                    scraper._scrape_dividends = scrape
                    scraper._clear_browser_cache_if_due = MagicMock(
                        side_effect=lambda: context.close.assert_awaited_once()
                    )
                    with patch.object(sa, "async_playwright"):
                        method = (
                            scraper.scrape_dividends_only
                            if dividends_only
                            else scraper.scrape
                        )
                        ticker = {"exchange": "NYSE", "symbol": "TEST"}
                        if fails:
                            with self.assertRaisesRegex(RuntimeError, "scrape failed"):
                                await method(ticker)
                        else:
                            self.assertEqual(await method(ticker), result)
                    scraper._clear_browser_cache_if_due.assert_called_once()


if __name__ == "__main__":
    unittest.main()
