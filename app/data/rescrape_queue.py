"""Durable requests for the scheduled fundamentals worker.

API processes may request a refresh, but they must never run Playwright or
write a ticker cache themselves.  A small file per ticker is an atomic,
cross-process handoff to the one scheduled worker.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator
from uuid import uuid4

from app.config import CACHE_DIR
from app.data.files import atomic_write_json
from app.utils.time_utils import dubai_now_iso


@dataclass(frozen=True)
class RescrapeRequest:
    ticker: str
    request_id: str
    requested_at: str


class RescrapeQueue:
    """A coalescing, filesystem-backed request queue.

    At most one outstanding request exists per ticker.  The lock also prevents
    a worker completing an older request from removing a newer button click.
    """

    def __init__(self, queue_dir: Path | str | None = None) -> None:
        self.queue_dir = Path(queue_dir or CACHE_DIR / "rescrape-requests")

    def _path(self, ticker: str) -> Path:
        # Do not derive a filename directly from user input.  The payload still
        # contains the canonical ticker for inspection and ordering.
        token = hashlib.sha256(ticker.encode("utf-8")).hexdigest()
        return self.queue_dir / f"{token}.json"

    @contextmanager
    def _locked(self, ticker: str) -> Iterator[None]:
        self.queue_dir.mkdir(parents=True, exist_ok=True)
        lock_path = self._path(ticker).with_suffix(".lock")
        with lock_path.open("a+") as lock_file:
            fcntl.flock(lock_file, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_file, fcntl.LOCK_UN)

    @staticmethod
    def _read(path: Path) -> RescrapeRequest | None:
        try:
            with path.open(encoding="utf-8") as request_file:
                data = json.load(request_file)
            ticker = data.get("ticker")
            request_id = data.get("request_id")
            requested_at = data.get("requested_at")
            if all(
                isinstance(value, str) and value
                for value in (ticker, request_id, requested_at)
            ):
                return RescrapeRequest(ticker, request_id, requested_at)
        except (OSError, ValueError, TypeError):
            pass
        return None

    def schedule(self, ticker: str) -> tuple[RescrapeRequest, bool]:
        """Queue ``ticker`` and return it with whether it was already pending."""
        path = self._path(ticker)
        with self._locked(ticker):
            current = self._read(path)
            if current:
                return current, True

            request = RescrapeRequest(
                ticker=ticker,
                request_id=str(uuid4()),
                requested_at=dubai_now_iso(),
            )
            atomic_write_json(path, request.__dict__)
            return request, False

    def next(self) -> RescrapeRequest | None:
        """Return the oldest valid outstanding request without consuming it."""
        requests = (
            [
                request
                for path in self.queue_dir.glob("*.json")
                if (request := self._read(path)) is not None
            ]
            if self.queue_dir.exists()
            else []
        )
        return min(requests, key=lambda request: request.requested_at, default=None)

    def has_pending(self) -> bool:
        return self.next() is not None

    def discard(self, request: RescrapeRequest) -> None:
        """Remove only the exact request the worker has attempted."""
        path = self._path(request.ticker)
        with self._locked(request.ticker):
            current = self._read(path)
            if current and current.request_id == request.request_id:
                path.unlink(missing_ok=True)
