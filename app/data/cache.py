from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from app.core.logger import get_logger
from app.data.files import atomic_write_json
from app.utils.time_utils import dubai_now_iso
from app.config import CACHE_DIR

logger = get_logger()


class Cache:
    """
    Fundamental cache (flat structure)

    - cache/ADX_FAB.json
    - overwrite on save
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

    @staticmethod
    def _section_failed(section: Any) -> bool:
        """Return whether a scraper section carries no usable result.

        An empty ``rows`` list is valid: companies can genuinely have no
        dividend history.  Conversely, a section set to ``None`` by the
        scraper orchestration, or a table containing only its captured error,
        must never replace a previously successful section.
        """
        if not isinstance(section, dict) or "error" in section:
            return True

        rows = section.get("rows")
        return bool(rows) and all(
            isinstance(row, dict) and "error" in row for row in rows
        )

    @classmethod
    def _preserve_successful_sections(
        cls, previous: dict[str, Any] | None, incoming: dict[str, Any]
    ) -> dict[str, Any]:
        """Keep last-known-good scraped sections when a refresh is partial."""
        if not isinstance(previous, dict):
            return incoming

        merged = dict(incoming)
        for section_name in (
            "overview",
            "financials",
            "dividends",
            "statistics",
            "ratios",
        ):
            if not cls._section_failed(merged.get(section_name)):
                continue

            prior_section = previous.get(section_name)
            if not cls._section_failed(prior_section):
                merged[section_name] = prior_section
                logger.warning(
                    "Keeping last known good %s section during partial refresh",
                    section_name,
                )

        return merged

    def save(self, ticker_key: str, data: dict[str, Any]) -> bool:
        if self.read_only:
            logger.error("Refusing to write ticker cache from a read-only Cache")
            return False
        if self._has_error(data):
            logger.warning("Cache skip %s: payload contains error", ticker_key)
            return False

        # A ticker refresh is intentionally sectional: source pages can fail
        # independently.  Merge before publishing so a transient timeout
        # cannot erase history that powers the dividends ledger.
        payload = self._preserve_successful_sections(self.load(ticker_key), data)
        payload["last_updated"] = data.get("scraped_at") or dubai_now_iso()

        path = self._path(ticker_key)

        try:
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
