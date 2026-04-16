import asyncio
import random

import pandas as pd
from tvDatafeed import Interval, TvDatafeed

from app.core.logger import get_logger
from app.data.db import DB

logger = get_logger()


async def _set_ohlc(
    tv_exchange: str,
    symbol: str,
    interval=Interval.in_15_minute,
    bars: int = 100,
    max_retries: int = 4,
    base_delay: float = 3.0,
):
    loop = asyncio.get_running_loop()
    storage_key = f"{tv_exchange}:{symbol}"

    for attempt in range(1, max_retries + 1):
        try:
            df = await loop.run_in_executor(
                None,
                lambda: TvDatafeed().get_hist(
                    symbol=symbol,
                    exchange=tv_exchange,
                    interval=interval,
                    n_bars=bars,
                ),
            )

            if df is not None and not df.empty:
                df = df.reset_index().rename(columns={"index": "datetime"})

                # TvDatafeed returns UTC — localize as UTC then convert to Dubai
                dt = pd.to_datetime(df["datetime"], errors="coerce")

                # If it's a naive datetime (it is), just tag it as Dubai time directly.
                if dt.dt.tz is None:
                    dt = dt.dt.tz_localize(
                        "Asia/Dubai", ambiguous="NaT", nonexistent="NaT"
                    )
                else:
                    dt = dt.dt.tz_convert("Asia/Dubai")

                df["datetime"] = dt.apply(
                    lambda ts: ts.isoformat() if pd.notna(ts) else None
                )

                DB().upsert_many(
                    [
                        {
                            "symbol": storage_key,
                            "timestamp": row["datetime"],
                            "close": row["close"],
                            "volume": row["volume"],
                        }
                        for row in df[["datetime", "close", "volume"]].to_dict(
                            orient="records"
                        )
                        if row["datetime"]
                    ]
                )

                logger.info("%s — upserted %d bars", storage_key, len(df))
                return

            if attempt == max_retries:
                logger.error(
                    "%s — all %d attempts returned empty", storage_key, max_retries
                )
                return

        except Exception as exc:
            if attempt == max_retries:
                logger.error(
                    "%s — all %d attempts failed: %s", storage_key, max_retries, exc
                )
                return

        delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 1.5)
        logger.warning(
            "%s — attempt %d failed, retrying in %.1fs", storage_key, attempt, delay
        )
        await asyncio.sleep(delay)
