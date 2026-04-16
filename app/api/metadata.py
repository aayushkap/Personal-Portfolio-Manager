from fastapi import APIRouter, Depends


from app.api.deps import get_overview_module
from app.services.overview import OverviewModule
from app.services.overlays import OverlayResolver
from app.data.gsheet import GSheet_Manager

router = APIRouter(prefix="/metadata", tags=["Metadata"])


@router.get("")
async def get_metadata(module: OverviewModule = Depends(get_overview_module)):
    tx = module.get_all_transactions()

    # Fetch watchlist separately since they might not be in transactions yet
    gs = GSheet_Manager()
    watchlist_items = gs.fetch_watchlist()
    watchlist_tickers = sorted(
        list({item["ticker"] for item in watchlist_items if item.get("ticker")})
    )

    if tx.empty:
        return {
            "sectors": [],
            "instruments": [],
            "first_investment_date": None,
            "available_overlays": OverlayResolver(module).catalogue(),
            "watchlist": watchlist_tickers,
        }

    return {
        "sectors": sorted(tx["sector"].dropna().unique().tolist()),
        "instruments": sorted(tx["ticker"].unique().tolist()),
        "first_investment_date": tx["trade_date"].min().isoformat(),
        "available_overlays": OverlayResolver(module).catalogue(),
        "watchlist": watchlist_tickers,
    }
