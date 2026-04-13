from fastapi import APIRouter, Depends

from app.api.schema import PerformanceRequest
from app.api.deps import get_overview_module
from app.services.overview import OverviewModule
from app.services.filters import PortfolioFilters, DateRange


router = APIRouter(prefix="/overview", tags=["Overview"])


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

    return module.get_overview(filters, include_events=body.include_events)
