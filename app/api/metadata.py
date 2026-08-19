import threading
import time

from fastapi import APIRouter, Depends


from app.api.deps import get_overview_module
from app.services.overview import OverviewModule
from app.services.overlays import OverlayResolver
from app.data.gsheet import GSheet_Manager

router = APIRouter(prefix="/metadata", tags=["Metadata"])

_CACHE_TTL_SECONDS = 60
_metadata_cache: dict | None = None
_metadata_cache_expires_at = 0.0
_metadata_cache_lock = threading.Lock()


@router.get("")
def get_metadata(module: OverviewModule = Depends(get_overview_module)):
    global _metadata_cache, _metadata_cache_expires_at

    now = time.monotonic()
    with _metadata_cache_lock:
        if _metadata_cache is not None and now < _metadata_cache_expires_at:
            return _metadata_cache

        tx = module.get_all_transactions()

        # Fetch watchlist separately since they might not be in transactions yet.
        gs = GSheet_Manager()
        watchlist_items = gs.fetch_watchlist()
        watchlist_tickers = sorted(
            list({item["ticker"] for item in watchlist_items if item.get("ticker")})
        )

        if tx.empty:
            result = {
                "sectors": [],
                "instruments": [],
                "first_investment_date": None,
                "available_overlays": OverlayResolver(module).catalogue(),
                "watchlist": watchlist_tickers,
            }
        else:
            result = {
                "sectors": sorted(tx["sector"].dropna().unique().tolist()),
                "instruments": sorted(tx["ticker"].unique().tolist()),
                "first_investment_date": tx["trade_date"].min().isoformat(),
                "available_overlays": OverlayResolver(module).catalogue(),
                "watchlist": watchlist_tickers,
            }

        _metadata_cache = result
        _metadata_cache_expires_at = time.monotonic() + _CACHE_TTL_SECONDS
        return result
