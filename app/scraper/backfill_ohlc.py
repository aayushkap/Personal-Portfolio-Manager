import asyncio
from datetime import date, timedelta

from app.config import BENCHMARKS
from app.data.db import DB
from app.data.gsheet import GSheet_Manager
from app.scraper.ohlc import _set_ohlc
from app.utils.time_utils import dubai_today

MIN_ROWS = 10_000
STALE_AFTER_DAYS = 1


def _is_stale(latest_timestamp: str | None, today: date | None = None) -> bool:
    """Whether the latest OHLC bar is older than one Dubai calendar day."""
    if not latest_timestamp:
        return True
    try:
        latest_date = date.fromisoformat(latest_timestamp[:10])
    except (TypeError, ValueError):
        return True
    return latest_date < (today or dubai_today()) - timedelta(days=STALE_AFTER_DAYS)


def _required_instruments() -> dict[str, tuple[str, str]]:
    """Return every OHLC storage key that is still used by the application."""
    sheets = GSheet_Manager()
    portfolio_items = sheets.fetch_transactions()
    watchlist_items = sheets.fetch_watchlist()

    # The sheet client reports failures as an empty list.  Do not mistake a
    # transient Google Sheets failure for an intentionally empty portfolio and
    # purge the entire price cache.
    if not portfolio_items and not watchlist_items:
        raise RuntimeError(
            "No transaction or watchlist instruments were loaded; cleanup aborted."
        )

    instruments = [*portfolio_items, *watchlist_items, *BENCHMARKS.values()]
    required: dict[str, tuple[str, str]] = {}
    for instrument in instruments:
        exchange = str(instrument.get("exchange") or "").strip()
        symbol = str(instrument.get("symbol") or "").strip()
        if exchange and symbol:
            required[f"{exchange}:{symbol}"] = (exchange, symbol)

    return required


def _remove_stale_ohlc(db: DB, required_symbols: set[str]) -> list[str]:
    """Delete price rows for instruments no longer in sheets or overlays."""
    with db.connection(write=True) as conn:
        cached_symbols = {
            row["symbol"] for row in conn.execute("SELECT DISTINCT symbol FROM ohlc")
        }
        stale_symbols = sorted(cached_symbols - required_symbols)
        if stale_symbols:
            conn.executemany(
                "DELETE FROM ohlc WHERE symbol = ?",
                [(symbol,) for symbol in stale_symbols],
            )

    # DELETE releases rows for reuse, while VACUUM returns their disk space in
    # cache/portfolio.db to the filesystem.
    if stale_symbols:
        with db.connection(write=True) as conn:
            conn.execute("VACUUM")

    return stale_symbols


async def main():
    DB.bootstrap()
    db = DB(read_only=False)
    required = _required_instruments()
    stale = _remove_stale_ohlc(db, set(required))
    print(f"Removed {len(stale)} stale symbols")

    with db.connection() as conn:
        row_stats = {
            row["symbol"]: (row["row_count"], row["latest_timestamp"])
            for row in conn.execute(
                """
                SELECT symbol, COUNT(*) AS row_count, MAX(timestamp) AS latest_timestamp
                FROM ohlc
                GROUP BY symbol
                """
            )
        }
        short = [
            (storage_key, exchange, symbol)
            for storage_key, (exchange, symbol) in required.items()
            if (
                row_stats.get(storage_key, (0, None))[0] < MIN_ROWS
                or _is_stale(row_stats.get(storage_key, (0, None))[1])
            )
        ]

    print(f"Len: {len(short)}")

    for storage_key, exchange, symbol in short:
        print(f"Backfilling {storage_key}")
        await _set_ohlc(exchange, symbol, bars=MIN_ROWS)


if __name__ == "__main__":
    asyncio.run(main())
