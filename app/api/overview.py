from fastapi import APIRouter, Depends

from app.api.schema import PerformanceRequest
from app.api.deps import get_overview_module
from app.services.overview import OverviewModule
from app.services.filters import PortfolioFilters, DateRange


router = APIRouter(prefix="/overview", tags=["Overview"])


@router.get("/metadata")
async def get_metadata(module: OverviewModule = Depends(get_overview_module)):
    tx = module.get_all_transactions()

    if tx.empty:
        return {
            "sectors": [],
            "instruments": [],
            "first_investment_date": None,
            "available_overlays": ["SMA", "PORTFOLIO"],
        }

    return {
        "sectors": sorted(tx["sector"].dropna().unique().tolist()),
        "instruments": sorted(tx["ticker"].unique().tolist()),
        "first_investment_date": tx["trade_date"].min().isoformat(),
        "available_overlays": ["SMA", "PORTFOLIO"],
    }


@router.post("")
async def get_overview(
    body: PerformanceRequest,
    module: OverviewModule = Depends(get_overview_module),
):
    filters = PortfolioFilters(
        date_range=DateRange(start=body.start_date, end=body.end_date),
        tickers=body.instruments,
        sectors=body.sectors,
        overlays=body.overlays,
    )

    return module.get_overview(
        filters, include_events=body.include_dividends_and_events
    )
