import asyncio

from ..data.db import DB

MIN_ROWS = 5_000


async def main():
    db = DB()
    with db._connect() as conn:
        short = conn.execute(
            "SELECT symbol FROM ohlc GROUP BY symbol HAVING COUNT(*) < ?", (MIN_ROWS,)
        ).fetchall()

        print(f"Len: {len(short)}")

    for row in short:
        exchange, symbol = row["symbol"].split(":", 1)
        print(f"Backfilling {row['symbol']}")
        # await _set_ohlc(exchange, symbol, bars=MIN_ROWS)


if __name__ == "__main__":
    asyncio.run(main())
