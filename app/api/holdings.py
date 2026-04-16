# app/api/routes/holdings.py

from fastapi import APIRouter, Depends, Query
from typing import Optional

from app.api.deps import get_holdings_module
from app.services.holdings import HoldingsModule
from app.services.filters import PortfolioFilters, DateRange
from datetime import date

router = APIRouter(prefix="/holdings", tags=["Holdings"])


@router.get("/")
def list_holdings(
    sectors: Optional[str] = Query(None, description="Comma-separated sectors"),
    search: Optional[str] = Query(None, description="Search by ticker or company name"),
    module: HoldingsModule = Depends(get_holdings_module),
):
    sectors_list = sectors.split(",") if sectors else []
    filters = PortfolioFilters(
        date_range=DateRange(start=date(2000, 1, 1), end=date.today()),
        sectors=[sec.title() for sec in sectors_list],
    )
    results = module.get_holdings_list(filters)

    if search:
        q = search.lower()
        results = [
            r
            for r in results
            if q in r.get("ticker", "").lower() or q in r.get("name", "").lower()
        ]

    return results


@router.get("/{ticker}")
def get_holding_detail(
    ticker: str,
    timeframe: str = Query("1m", pattern="^(1d|1w|1m|3m|all)$"),
    module: HoldingsModule = Depends(get_holdings_module),
):
    """
    Detailed view for a single holding.
    Returns:
    - chart:        OHLCV bars for selected timeframe (1d/1w/1m/3m/all)
    - transactions: buys, sells, dividends received — chronological
    - fundamentals: key company metrics from cache
    """
    return module.get_holding_detail(
        ticker=ticker.upper(),
        timeframe=timeframe,
    )
