from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[1]
ACCESS_DIR = BASE_DIR / "access"
CACHE_DIR = BASE_DIR / "cache"
DB_PATH = CACHE_DIR / "portfolio.db"

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
    "DFM:DFMREI": {
        "label": "DFM Real Estate Index",
        "exchange": "DFM",
        "symbol": "DFMREI",
        "type": "index",
    },
    "TVUS05Y": {
        "label": "US Government Bonds 5 YR Yield",
        "exchange": "TVC",
        "symbol": "US05Y",
        "type": "index",
    },
    "TVUS10Y": {
        "label": "US Government Bonds 10 YR Yield",
        "exchange": "TVC",
        "symbol": "US10Y",
        "type": "index",
    },
}
