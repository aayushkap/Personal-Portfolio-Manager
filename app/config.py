from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[1]
ACCESS_DIR = BASE_DIR / "access"
CACHE_DIR = BASE_DIR / "cache"
DB_PATH = CACHE_DIR / "portfolio.db"
QUOTE_PATH = CACHE_DIR / "quote.json"
WORKER_LOCK_PATH = CACHE_DIR / "pbe-worker.lock"

GEMINI_KEY = os.getenv("GEMINI_KEY")

# All times are Asia/Dubai.  The worker applies this buffer on both sides of a
# session when deciding whether an instrument needs an intraday OHLC refresh.
OHLC_SESSION_BUFFER_MINUTES = 30

# Configure exceptions here; exchanges not listed use DEFAULT (US market hours).
# Weekdays use Python's convention: Monday=0 through Friday=4.
OHLC_MARKET_SESSIONS: dict[str, dict[str, object]] = {
    "ADX": {"open": "10:00", "close": "15:00", "weekdays": (0, 1, 2, 3, 4)},
    "DFM": {"open": "10:00", "close": "15:00", "weekdays": (0, 1, 2, 3, 4)},
    "LSE": {"open": "10:00", "close": "17:30", "weekdays": (0, 1, 2, 3, 4)},
    # TradingView symbols and the configured FTSE benchmark can use these aliases.
    "LON": {"open": "10:00", "close": "17:30", "weekdays": (0, 1, 2, 3, 4)},
    "FTSE": {"open": "10:00", "close": "17:30", "weekdays": (0, 1, 2, 3, 4)},
    "DEFAULT": {
        "open": "17:30",
        "close": "00:00",
        "weekdays": (0, 1, 2, 3, 4),
    },
}


BENCHMARKS: dict[str, dict] = {
    "DFM:DFMGI": {
        "label": "DFM General Index",
        "exchange": "DFM",
        "symbol": "DFMGI",
        "type": "index",
    },
    "ADX:FADGI": {
        "label": "ADX General Index",
        "exchange": "ADX",
        "symbol": "FADGI",
        "type": "index",
    },
    "TVC:SPX": {
        "label": "S&P 500",
        "exchange": "TVC",
        "symbol": "SPX",
        "type": "index",
    },
    "DFM:DFMREI": {
        "label": "DFM Real Estate Index",
        "exchange": "DFM",
        "symbol": "DFMREI",
        "type": "index",
    },
    "TVC:US05Y": {
        "label": "US Government Bonds 5 YR Yield",
        "exchange": "TVC",
        "symbol": "US05Y",
        "type": "index",
    },
    "TVC:US10Y": {
        "label": "US Government Bonds 10 YR Yield",
        "exchange": "TVC",
        "symbol": "US10Y",
        "type": "index",
    },
    "TVC:US20Y": {
        "label": "US Government Bonds 20 YR Yield",
        "exchange": "TVC",
        "symbol": "US20Y",
        "type": "index",
    },
    "TVC:US30Y": {
        "label": "US Government Bonds 30 YR Yield",
        "exchange": "TVC",
        "symbol": "US30Y",
        "type": "index",
    },
    "AMEX:XLE": {
        "label": "Energy Select Sector SPDR Fund",
        "exchange": "AMEX",
        "symbol": "XLE",
        "type": "etf",
    },
    "AMEX:XLF": {
        "label": "Financial Select Sector SPDR Fund",
        "exchange": "AMEX",
        "symbol": "XLF",
        "type": "etf",
    },
    "AMEX:XLK": {
        "label": "Technology Select Sector SPDR Fund",
        "exchange": "AMEX",
        "symbol": "XLK",
        "type": "etf",
    },
    "AMEX:XLRE": {
        "label": "Real Estate Select Sector SPDR Fund",
        "exchange": "AMEX",
        "symbol": "XLRE",
        "type": "etf",
    },
    "AMEX:XLU": {
        "label": "Utilities Select Sector SPDR Fund",
        "exchange": "AMEX",
        "symbol": "XLU",
        "type": "etf",
    },
    "FTSE-UKX": {
        "label": "FTSE 100 Index",
        "exchange": "FTSE",
        "symbol": "UKX",
        "type": "index",
    },
    "NSE-NIFTY": {
        "label": "NIFTY 50 Index",
        "exchange": "NSE",
        "symbol": "NIFTY",
        "type": "index",
    },
}
