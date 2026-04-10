# app/api/routes/watchlist.py

from fastapi import APIRouter, Depends, Query

from app.api.deps import get_watchlist_module
from app.data.gsheet import GSheet_Manager
from app.services.watchlist import WatchlistModule

router = APIRouter(prefix="/watchlist", tags=["watchlist"])


@router.get("/")
def get_watchlist(module: WatchlistModule = Depends(get_watchlist_module)):
    items = GSheet_Manager().fetch_watchlist()
    return module.get_watchlist(items)


@router.get("/{ticker}")
def get_watchlist_detail(
    ticker: str,
    timeframe: str = Query("1m", pattern="^(1d|1w|1m|3m|all)$"),
    module: WatchlistModule = Depends(get_watchlist_module),
):
    return module.get_watchlist_detail(
        ticker=ticker.upper(),
        timeframe=timeframe,
    )
