from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[1]
ACCESS_DIR = BASE_DIR / "access"
CACHE_DIR = BASE_DIR / "cache"
DB_PATH = CACHE_DIR / "portfolio.db"
QUOTE_PATH = CACHE_DIR / "quote.json"

GEMINI_KEY = os.getenv("GEMINI_KEY")

CACHE_DIR.mkdir(parents=True, exist_ok=True)


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
}
