from __future__ import annotations

import fcntl
import json
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from app.core.logger import get_logger
from app.data.files import atomic_write_json
from app.data.merge import merge_ticker
from app.utils.time_utils import dubai_now_iso
from app.config import CACHE_DIR

logger = get_logger()


class Cache:
    """
    Fundamental cache (flat structure)

    - cache/ADX_FAB.json
    - merge on save (see app/data/merge.py)
    """

    def __init__(
        self, cache_dir: str | Path = CACHE_DIR, *, read_only: bool = True
    ) -> None:
        self.cache_dir = Path(cache_dir)
        self.read_only = read_only
        if not read_only:
            self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _safe_ticker(self, ticker_key: str) -> str:
        return ticker_key.replace(":", "_").lower()

    def _path(self, ticker_key: str) -> Path:
        return self.cache_dir / f"{self._safe_ticker(ticker_key)}.json"

    @staticmethod
    def _has_error(data: dict[str, Any]) -> bool:
        if not isinstance(data, dict):
            return True

        if "error" in data:
            return True

        section_keys = ("overview", "financials", "dividends", "statistics", "ratios")
        sections = [data.get(k) for k in section_keys if k in data]

        return bool(sections) and all(
            isinstance(s, dict) and "error" in s for s in sections
        )

    @contextmanager
    def _write_lock(self, ticker_key: str) -> Iterator[None]:
        """Serialise load-merge-write per ticker across worker and scripts."""
        lock_dir = self.cache_dir / ".locks"
        lock_dir.mkdir(parents=True, exist_ok=True)
        with (lock_dir / f"{self._safe_ticker(ticker_key)}.lock").open("w") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def _archive_superseded(
        self, ticker_key: str, section_name: str, section: Any
    ) -> None:
        """Keep discarded history recoverable instead of silently losing it."""
        stamp = dubai_now_iso().replace(":", "").replace("+", "_")
        path = (
            self.cache_dir
            / "archive"
            / f"{self._safe_ticker(ticker_key)}.{section_name}.{stamp}.json"
        )
        try:
            atomic_write_json(path, section)
            logger.warning(
                "Archived superseded %s %s -> %s", ticker_key, section_name, path
            )
        except Exception:
            logger.exception("Failed to archive %s %s", ticker_key, section_name)

    def save(self, ticker_key: str, data: dict[str, Any]) -> bool:
        if self.read_only:
            logger.error("Refusing to write ticker cache from a read-only Cache")
            return False
        if self._has_error(data):
            logger.warning("Cache skip %s: payload contains error", ticker_key)
            return False

        path = self._path(ticker_key)

        try:
            with self._write_lock(ticker_key):
                # Source pages fail independently and only show a rolling
                # window, so merge into the cached document rather than
                # overwrite: failed sections keep their last good copy and
                # history that scrolled off the source is retained.
                payload, report = merge_ticker(self.load(ticker_key), data)
                for note in report.notes:
                    logger.info("Cache merge %s | %s", ticker_key, note)
                for section_name, section in report.superseded.items():
                    self._archive_superseded(ticker_key, section_name, section)
                payload["last_updated"] = data.get("scraped_at") or dubai_now_iso()

                atomic_write_json(path, payload)

            logger.info("Cached %s -> %s", ticker_key, path)
            return True

        except Exception:
            logger.exception("Failed to save %s", ticker_key)
            return False

    def load(self, ticker_key: str) -> dict[str, Any] | None:
        path = self._path(ticker_key)

        if not path.exists():
            return None

        try:
            with path.open("r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            logger.exception("Failed to load %s", ticker_key)
            return None

    def delete(self, ticker_key: str) -> bool:
        if self.read_only:
            logger.error("Refusing to delete ticker cache from a read-only Cache")
            return False
        path = self._path(ticker_key)

        if not path.exists():
            return False

        try:
            path.unlink()
            logger.info("Deleted cache %s", ticker_key)
            return True
        except Exception:
            logger.exception("Failed to delete %s", ticker_key)
            return False

    def status(self) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}

        for path in self.cache_dir.glob("*.json"):
            ticker_key = path.stem.replace("_", ":")

            try:
                with path.open("r", encoding="utf-8") as f:
                    data = json.load(f)

                out[ticker_key] = {
                    "last_updated": data.get("last_updated"),
                }
            except Exception:
                logger.exception("Failed to read %s", ticker_key)
                out[ticker_key] = {
                    "last_updated": None,
                }

        return out
