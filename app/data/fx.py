import json
from pathlib import Path
from app.config import CACHE_DIR
from app.core.logger import get_logger

logger = get_logger()
FX_FILE = Path(CACHE_DIR) / "exchange.json"


FX_PAIRS = [
    {"tv_exchange": "FX_IDC", "symbol": "GBPAED"},
    {"tv_exchange": "FX_IDC", "symbol": "USDAED"},
    {"tv_exchange": "FX_IDC", "symbol": "EURAED"},
]


async def fetch_and_save_fx() -> dict[str, float]:
    from app.scraper.ohlc import _set_ohlc
    from app.data.db import DB

    db = DB()
    for pair in FX_PAIRS:
        try:
            await _set_ohlc(
                tv_exchange=pair["tv_exchange"],
                sa_exchange="",
                symbol=pair["symbol"],
                bars=5,
            )
        except Exception as e:
            logger.warning("FX fetch failed for %s: %s", pair["symbol"], e)

    rates = {"AED": 1.0}
    for pair in FX_PAIRS:
        ticker_key = f"{pair['tv_exchange']}:{pair['symbol']}"
        row = db.get_latest(ticker_key)
        if row:
            base = pair["symbol"][:3].upper()  # 'GBP', 'USD', 'EUR'
            rates[base] = row["close"]
            if base == "GBP":
                rates["GBX"] = row["close"] / 100

    FX_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(FX_FILE, "w") as f:
        json.dump(rates, f)

    logger.info("FX rates saved: %s", rates)
    return rates


def load_fx_rates() -> dict[str, float]:
    try:
        with open(FX_FILE) as f:
            rates = {k.upper(): float(v) for k, v in json.load(f).items()}
        if "GBP" in rates:
            rates.setdefault("GBX", rates["GBP"] / 100)
        rates.setdefault("AED", 1.0)
        return rates
    except FileNotFoundError:
        logger.warning("FX file missing — run fx_job first")
        return {"AED": 1.0}
    except Exception as e:
        logger.error("FX load error: %s", e)
        return {"AED": 1.0}
