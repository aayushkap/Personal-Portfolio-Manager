# db.py

import sqlite3
from typing import Optional

from app.config import DB_PATH


class DB:
    def __init__(self, path: str = DB_PATH):
        """
        Interact with local DB. Each functions establishes its own conn
        """
        self.path = path
        self._init()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init(self):
        with self._connect() as conn:
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

    # Write
    def upsert(
        self, symbol: str, timestamp: str, close: float, volume: Optional[float] = None
    ):
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO ohlc (symbol, timestamp, close, volume)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (symbol, timestamp) DO UPDATE SET
                    close  = excluded.close,
                    volume = excluded.volume
            """,
                (symbol, timestamp, close, volume),
            )

    def upsert_many(self, rows: list[dict]):
        """
        rows: [{"symbol": "ADX:IHC", "timestamp": "2026-03-06T13:30", "close": 390.1, "volume": 15066}]
        """
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO ohlc (symbol, timestamp, close, volume)
                VALUES (:symbol, :timestamp, :close, :volume)
                ON CONFLICT (symbol, timestamp) DO UPDATE SET
                    close  = excluded.close,
                    volume = excluded.volume
            """,
                rows,
            )

    # Read
    def get(self, symbol: str, limit: int = 500) -> list[dict]:
        with self._connect() as conn:
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
        return [dict(r) for r in reversed(rows)]  # return ascending

    def get_latest(self, symbol: str) -> Optional[dict]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT symbol, timestamp, close, volume
                FROM ohlc WHERE symbol = ?
                ORDER BY timestamp DESC LIMIT 1
            """,
                (symbol,),
            ).fetchone()
        return dict(row) if row else None

    def get_all_symbols(self) -> list[str]:
        with self._connect() as conn:
            rows = conn.execute("SELECT DISTINCT symbol FROM ohlc").fetchall()
        return [r["symbol"] for r in rows]
