import asyncio
import random

import pandas as pd
from tvDatafeed import Interval, TvDatafeed

from app.core.logger import get_logger
from app.data.db import DB

logger = get_logger()


async def _set_ohlc(
    exchange: str,
    symbol: str,
    interval=Interval.in_15_minute,
    bars: int = 100,
    max_retries: int = 4,
    base_delay: float = 3.0,
):
    loop = asyncio.get_running_loop()

    for attempt in range(1, max_retries + 1):
        try:
            df = await loop.run_in_executor(
                None,
                lambda: TvDatafeed().get_hist(
                    symbol=symbol,
                    exchange=exchange,
                    interval=interval,
                    n_bars=bars,
                ),
            )

            if df is not None and not df.empty:
                df = df.reset_index().rename(columns={"index": "datetime"})
                dt = pd.to_datetime(
                    df["datetime"], errors="coerce", utc=True
                ).dt.tz_convert("Asia/Dubai")
                df["datetime"] = dt.apply(lambda ts: ts.isoformat())

                DB().upsert_many(
                    [
                        {
                            "symbol": f"{exchange}:{symbol}",
                            "timestamp": row["datetime"],
                            "close": row["close"],
                            "volume": row["volume"],
                        }
                        for row in df[["datetime", "close", "volume"]].to_dict(
                            orient="records"
                        )
                    ]
                )

                logger.info("%s:%s — upserted %d bars", exchange, symbol, len(df))
                return

            # Empty df — retry
            if attempt == max_retries:
                logger.error(
                    "%s:%s — all %d attempts returned empty",
                    exchange,
                    symbol,
                    max_retries,
                )
                return

        except Exception as exc:
            if attempt == max_retries:
                logger.error(
                    "%s:%s — all %d attempts failed: %s",
                    exchange,
                    symbol,
                    max_retries,
                    exc,
                )
                return

        delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 1.5)
        logger.warning(
            "%s:%s — attempt %d failed, retrying in %.1fs",
            exchange,
            symbol,
            attempt,
            delay,
        )
        await asyncio.sleep(delay)
