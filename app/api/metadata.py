from fastapi import APIRouter, Depends


from app.api.deps import get_overview_module
from app.services.overview import OverviewModule
from app.services.overlays import OverlayResolver

router = APIRouter(prefix="/metadata", tags=["Metadata"])


@router.get("")
async def get_metadata(module: OverviewModule = Depends(get_overview_module)):
    tx = module.get_all_transactions()

    if tx.empty:
        return {
            "sectors": [],
            "instruments": [],
            "first_investment_date": None,
            "available_overlays": OverlayResolver(module).catalogue(),
        }

    return {
        "sectors": sorted(tx["sector"].dropna().unique().tolist()),
        "instruments": sorted(tx["ticker"].unique().tolist()),
        "first_investment_date": tx["trade_date"].min().isoformat(),
        "available_overlays": OverlayResolver(module).catalogue(),
    }
