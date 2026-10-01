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

# StockAnalysis is an external HTML source rather than a market-data API.  Keep
# a conservative, process-wide spacing between ticker attempts so retries and
# growing portfolios cannot turn into a request burst.  Fifteen minutes allows
# 192 ticker attempts across a 48-hour weekend before weekday runs contribute.
FUNDAMENTALS_MIN_INTERVAL_SECONDS = int(
    os.getenv("FUNDAMENTALS_MIN_INTERVAL_SECONDS", str(15 * 60))
)
FUNDAMENTALS_SOURCE_COOLDOWN_SECONDS = int(
    os.getenv("FUNDAMENTALS_SOURCE_COOLDOWN_SECONDS", str(60 * 60))
)
# The original profile accumulated sessions from a detectable headless browser;
# the Patchright profile starts clean.
STOCKANALYSIS_BROWSER_PROFILE_DIR = Path(
    os.getenv(
        "STOCKANALYSIS_BROWSER_PROFILE_DIR",
        str(CACHE_DIR / "stockanalysis-browser-profile-patchright"),
    )
)
# Cloudflare rejects headless Chromium outright.  The headful browser uses a
# private Xvfb display on servers without a desktop (see StockAnalysisScraper).
STOCKANALYSIS_HEADLESS = os.getenv("STOCKANALYSIS_HEADLESS", "false").lower() in {
    "1",
    "true",
    "yes",
}
STOCKANALYSIS_DEBUG_SCREENSHOT_DIR = Path(
    os.getenv(
        "STOCKANALYSIS_DEBUG_SCREENSHOT_DIR",
        str(CACHE_DIR / "stockanalysis-debug"),
    )
)

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
