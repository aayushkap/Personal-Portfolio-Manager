"""Cross-process singleton protection for the scheduled worker."""

from __future__ import annotations

import fcntl
import os
from pathlib import Path
from typing import TextIO


class SingletonLock:
    """An advisory non-blocking lock held for one process lifetime."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._handle: TextIO | None = None

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            handle.close()
            raise RuntimeError(
                f"Another scheduled worker already holds {self.path}"
            ) from exc

        handle.seek(0)
        handle.truncate()
        handle.write(f"{os.getpid()}\n")
        handle.flush()
        self._handle = handle

    def release(self) -> None:
        if self._handle is None:
            return
        try:
            fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
        finally:
            self._handle.close()
            self._handle = None
