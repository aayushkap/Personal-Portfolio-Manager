"""SQLite access with explicit ownership, transactions, and connection cleanup."""

from __future__ import annotations

import sqlite3
from contextlib import closing, contextmanager
from pathlib import Path
from typing import Iterator, Optional

from app.config import DB_PATH


class DB:
    """Access the OHLC database.

    API and HQL callers use the read-only default. The one scheduled worker opts
    into ``read_only=False`` for short, batched write transactions.
    """

    BUSY_TIMEOUT_MS = 10_000

    def __init__(
        self,
        path: str | Path = DB_PATH,
        *,
        read_only: bool = True,
        busy_timeout_ms: int = BUSY_TIMEOUT_MS,
    ) -> None:
        self.path = Path(path)
        self.read_only = read_only
        self.busy_timeout_ms = busy_timeout_ms

    @classmethod
    def bootstrap(cls, path: str | Path = DB_PATH) -> None:
        """Create the schema and enable WAL before API/worker processes start."""
        database_path = Path(path)
        database_path.parent.mkdir(parents=True, exist_ok=True)

        with closing(
            sqlite3.connect(database_path, timeout=cls.BUSY_TIMEOUT_MS / 1000)
        ) as conn:
            conn.execute(f"PRAGMA busy_timeout = {cls.BUSY_TIMEOUT_MS}")
            mode = conn.execute("PRAGMA journal_mode=WAL").fetchone()[0].lower()
            if mode != "wal":
                raise RuntimeError(f"Could not enable WAL mode (current mode: {mode})")

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ohlc (
                    symbol    TEXT    NOT NULL,
                    timestamp TEXT    NOT NULL,
                    close     REAL    NOT NULL,
                    volume    REAL,
                    PRIMARY KEY (symbol, timestamp)
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ohlc_symbol ON ohlc(symbol)")
            conn.commit()

    def _connect(self) -> sqlite3.Connection:
        if self.read_only:
            uri = f"{self.path.resolve().as_uri()}?mode=ro"
            conn = sqlite3.connect(
                uri,
                uri=True,
                timeout=self.busy_timeout_ms / 1000,
            )
            conn.execute("PRAGMA query_only = ON")
        else:
            conn = sqlite3.connect(
                self.path,
                timeout=self.busy_timeout_ms / 1000,
            )

        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
        return conn

    @contextmanager
    def connection(self, *, write: bool = False) -> Iterator[sqlite3.Connection]:
        """Yield one connection and always close it, including on exceptions."""
        if write and self.read_only:
            raise PermissionError("This DB instance is read-only")

        conn = self._connect()
        try:
            yield conn
            if write:
                conn.commit()
        except Exception:
            if write:
                conn.rollback()
            raise
        finally:
            conn.close()

    # Write methods are intentionally only usable by DB(read_only=False).
    def upsert(
        self, symbol: str, timestamp: str, close: float, volume: Optional[float] = None
    ) -> None:
        with self.connection(write=True) as conn:
            conn.execute(
                """
                INSERT INTO ohlc (symbol, timestamp, close, volume)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (symbol, timestamp) DO UPDATE SET
                    close = excluded.close,
                    volume = excluded.volume
                """,
                (symbol, timestamp, close, volume),
            )

    def upsert_many(self, rows: list[dict]) -> None:
        if not rows:
            return

        with self.connection(write=True) as conn:
            conn.executemany(
                """
                INSERT INTO ohlc (symbol, timestamp, close, volume)
                VALUES (:symbol, :timestamp, :close, :volume)
                ON CONFLICT (symbol, timestamp) DO UPDATE SET
                    close = excluded.close,
                    volume = excluded.volume
                """,
                rows,
            )

    # Read methods are safe for both API and worker callers.
    def get(self, symbol: str, limit: int = 500) -> list[dict]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT symbol, timestamp, close, volume
                FROM ohlc
                WHERE symbol = ?
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                (symbol, limit),
            ).fetchall()
        return [dict(row) for row in reversed(rows)]

    def get_latest(self, symbol: str) -> Optional[dict]:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT symbol, timestamp, close, volume
                FROM ohlc
                WHERE symbol = ?
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                (symbol,),
            ).fetchone()
        return dict(row) if row else None

    def get_all_symbols(self) -> list[str]:
        with self.connection() as conn:
            rows = conn.execute("SELECT DISTINCT symbol FROM ohlc").fetchall()
        return [row["symbol"] for row in rows]
