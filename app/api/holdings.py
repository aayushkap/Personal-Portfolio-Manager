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
    exchanges: Optional[str] = Query(None, description="Comma-separated exchanges"),
    tickers: Optional[str] = Query(None, description="Comma-separated tickers"),
    module: HoldingsModule = Depends(get_holdings_module),
):
    """
    Returns all active holding cards.
    Each card includes: name, sector, exchange, shares, cost basis,
    market value, total return, DoD/MoM/3M %, YoC, sparkline (1M).
    """
    filters = PortfolioFilters(
        date_range=DateRange(start=date(2000, 1, 1), end=date.today()),
        sectors=sectors.split(",") if sectors else [],
        exchanges=exchanges.split(",") if exchanges else [],
        tickers=tickers.split(",") if tickers else [],
    )
    return module.get_holdings_list(filters)


@router.get("/{ticker}")
def get_holding_detail(
    ticker: str,
    timeframe: str = Query("1m", regex="^(1d|1w|1m|3m|all)$"),
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
