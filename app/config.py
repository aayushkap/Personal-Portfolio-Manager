from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[1]
ACCESS_DIR = BASE_DIR / "access"
CACHE_DIR = BASE_DIR / "cache"
DB_PATH = CACHE_DIR / "portfolio.db"

CACHE_DIR.mkdir(parents=True, exist_ok=True)
