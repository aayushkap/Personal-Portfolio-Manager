from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import date, timedelta

from app.api.deps import get_overview_module
from app.services.overview import OverviewModule
from app.services.filters import PortfolioFilters, DateRange


router = APIRouter(prefix="/performance", tags=["Performance"])


class PerformanceRequest(BaseModel):
    start_date: date = Field(default_factory=lambda: date.today() - timedelta(days=120))
    end_date: date = Field(default_factory=lambda: date.today())
    instruments: Optional[List[str]] = None
    sectors: Optional[List[str]] = None
    include_dividends_and_events: bool = False
    overlays: List[str] = Field(default_factory=list)


@router.get("/metadata")
async def get_metadata(module: OverviewModule = Depends(get_overview_module)):
    tx = module.get_all_transactions()

    if tx.empty:
        return {
            "sectors": [],
            "instruments": [],
            "first_investment_date": None,
            "available_overlays": [],
        }

    return {
        "sectors": sorted(tx["sector"].dropna().unique().tolist()),
        "instruments": sorted(tx["ticker"].unique().tolist()),
        "first_investment_date": tx["trade_date"].min().isoformat(),
        "available_overlays": [],
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
    )
    print(filters)
    return module.get_overview(
        filters, include_events=body.include_dividends_and_events
    )
