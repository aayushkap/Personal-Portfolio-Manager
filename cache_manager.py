# cache_manager.py

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cache")


class CacheManager:
    """
    Per-ticker cache layout:
        cache/
            DFM_EMAAR/
                fundamentals.json   ← overwritten each clean scrape
                ohlc.jsonl          ← append-only, deduped by timestamp

    OHLC uses .jsonl (newline-delimited JSON) — one record per line.
    This is O(1) append and avoids loading/parsing the entire file on reads.
    Fundamentals is plain JSON — always overwritten if no error.
    """

    OHLC_FILE = "ohlc.jsonl"
    FUNDAMENTALS_FILE = "fundamentals.json"

    def __init__(self, cache_dir: str = CACHE_DIR):
        self.cache_dir = cache_dir
        os.makedirs(self.cache_dir, exist_ok=True)

    #  Path helpers
    def _ticker_dir(self, ticker_key: str) -> str:
        """'ADX:FAB' → cache/ADX_FAB/"""
        safe = ticker_key.replace(":", "_")
        path = os.path.join(self.cache_dir, safe)
        os.makedirs(path, exist_ok=True)
        return path

    def _ohlc_path(self, ticker_key: str) -> str:
        return os.path.join(self._ticker_dir(ticker_key), self.OHLC_FILE)

    def _fundamentals_path(self, ticker_key: str) -> str:
        return os.path.join(self._ticker_dir(ticker_key), self.FUNDAMENTALS_FILE)

    #  Timestamp normalisation
    @staticmethod
    def _normalise_ts(raw: Any) -> Optional[str]:
        if raw is None:
            return None
        try:
            # pandas Timestamp or datetime object
            if hasattr(raw, "strftime"):
                if hasattr(raw, "tzinfo") and raw.tzinfo is not None:
                    raw = raw.astimezone(timezone.utc).replace(tzinfo=None)
                return raw.strftime("%Y-%m-%dT%H:%M")

            # Epoch int/float
            if isinstance(raw, (int, float)):
                ts = raw / 1000 if raw > 32503680000 else raw
                return datetime.utcfromtimestamp(ts).strftime("%Y-%m-%dT%H:%M")

            if isinstance(raw, str):
                raw = raw.strip()

                # Strip timezone suffix (+04:00, +00:00, Z) before parsing
                import re

                raw_clean = re.sub(r"([+-]\d{2}:\d{2}|Z)$", "", raw).strip()

                for fmt in (
                    "%Y-%m-%dT%H:%M:%S",
                    "%Y-%m-%dT%H:%M",
                    "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%d %H:%M",
                    "%Y-%m-%d",
                ):
                    try:
                        return datetime.strptime(raw_clean, fmt).strftime(
                            "%Y-%m-%dT%H:%M"
                        )
                    except ValueError:
                        continue

        except Exception:
            pass
        return None

    #  Error detection
    @staticmethod
    def _has_error(data: Dict[str, Any]) -> bool:
        if "error" in data:
            return True
        section_keys = ("overview", "financials", "dividends", "statistics", "ratios")
        sections = [data[k] for k in section_keys if k in data]
        return bool(sections) and all("error" in s for s in sections)

    #  OHLC (append-only .jsonl)
    def _load_ohlc_timestamps(self, ticker_key: str) -> set:
        """Read only timestamps from existing .jsonl — fast, no full parse."""
        path = self._ohlc_path(ticker_key)
        seen = set()
        if not os.path.exists(path):
            return seen
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    ts = self._normalise_ts(
                        record.get("datetime")
                        or record.get("date")
                        or record.get("time")
                    )
                    if ts:
                        seen.add(ts)
                except json.JSONDecodeError:
                    continue
        return seen

    def append_ohlc(
        self, ticker_key: str, records: List[Dict[str, Any]]
    ) -> Dict[str, int]:
        """
        Append new OHLC records, skipping duplicates by normalised timestamp.
        Returns {"appended": n, "skipped": n}.
        """
        if not records:
            return {"appended": 0, "skipped": 0}

        existing_ts = self._load_ohlc_timestamps(ticker_key)
        path = self._ohlc_path(ticker_key)

        appended = skipped = 0
        with open(path, "a", encoding="utf-8") as f:
            for record in records:
                raw_ts = (
                    record.get("datetime") or record.get("date") or record.get("time")
                )
                ts = self._normalise_ts(raw_ts)

                if ts is None:
                    skipped += 1
                    continue
                if ts in existing_ts:
                    skipped += 1
                    continue

                # Normalise the stored timestamp key for consistency
                out = {**record, "datetime": ts}
                out.pop("date", None)
                out.pop("time", None)
                f.write(json.dumps(out, ensure_ascii=False) + "\n")
                existing_ts.add(ts)
                appended += 1

        return {"appended": appended, "skipped": skipped}

    def load_ohlc(self, ticker_key: str) -> List[Dict[str, Any]]:
        """Load full OHLC history, sorted by datetime ascending."""
        path = self._ohlc_path(ticker_key)
        records = []
        if not os.path.exists(path):
            return records
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        return sorted(records, key=lambda r: r.get("datetime", ""))

    #  Fundamentals (plain JSON overwrite)
    def save_fundamentals(self, ticker_key: str, data: Dict[str, Any]) -> bool:
        """
        Write overview, financials, dividends, statistics, ratios to fundamentals.json.
        Skipped if data contains an error. Returns True if written.
        """
        if self._has_error(data):
            print(f"  [cache] SKIP fundamentals {ticker_key} — error in result.")
            return False

        fundamental_keys = (
            "overview",
            "financials",
            "dividends",
            "statistics",
            "ratios",
            "ticker",
            "scraped_at",
        )
        payload = {k: data[k] for k in fundamental_keys if k in data}
        payload["last_updated"] = data.get("scraped_at", datetime.utcnow().isoformat())

        path = self._fundamentals_path(ticker_key)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
        print(f"  [cache] SAVED fundamentals {ticker_key}")
        return True

    def load_fundamentals(self, ticker_key: str) -> Optional[Dict[str, Any]]:
        path = self._fundamentals_path(ticker_key)
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    #  entry point
    def save_batch(self, results: list) -> Dict[str, Dict]:
        """
        Process asyncio.gather output.
        Each element: {"ADX:FAB": {full scrape dict}}
        """
        report = {}
        for result_group in results:
            if not isinstance(result_group, dict):
                continue
            for ticker_key, data in result_group.items():
                print(data)
                ohlc_records = data.get("ohlc", [])
                ohlc_stat = (
                    self.append_ohlc(ticker_key, ohlc_records)
                    if ohlc_records
                    else {"appended": 0, "skipped": 0}
                )
                fund_saved = self.save_fundamentals(ticker_key, data)

                report[ticker_key] = {
                    "fundamentals_saved": fund_saved,
                    "ohlc": ohlc_stat,
                }
                print(
                    f"  [cache] {ticker_key} | "
                    f"fundamentals={'OK' if fund_saved else 'SKIP'} | "
                    f"ohlc +{ohlc_stat['appended']} new, {ohlc_stat['skipped']} dupes"
                )
        return report

    #  Utility
    def status(self) -> Dict[str, Dict]:
        """Quick overview of all cached tickers."""
        out = {}
        for name in sorted(os.listdir(self.cache_dir)):
            ticker_key = name.replace("_", ":", 1)
            fund = self.load_fundamentals(ticker_key)
            ohlc_count = (
                sum(
                    1
                    for _ in open(self._ohlc_path(ticker_key), encoding="utf-8")
                    if os.path.exists(self._ohlc_path(ticker_key))
                )
                if os.path.exists(self._ohlc_path(ticker_key))
                else 0
            )
            out[ticker_key] = {
                "last_updated": fund.get("last_updated") if fund else None,
                "ohlc_rows": ohlc_count,
            }
        return out
