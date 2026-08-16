# app/api/analytics.py

from typing import Literal
from fastapi import APIRouter, Depends, HTTPException
from app.api.schema import AnalyticsPerformanceRequest
from app.services.analytics import AnalyticsModule
from app.api.deps import get_analytics_module

router = APIRouter(prefix="/analytics", tags=["Analytics"])


@router.post("/pnl")
def get_pnl(
    mode: Literal["price_return", "total"] = "total",
    module: AnalyticsModule = Depends(get_analytics_module),
):
    return module.get_pnl(mode=mode)


@router.post("/allocation")
def get_allocation(
    by: Literal["position", "sector", "exchange"] = "position",
    module: AnalyticsModule = Depends(get_analytics_module),
):
    return module.get_allocation(by=by)


@router.post("/income")
def get_income(
    module: AnalyticsModule = Depends(get_analytics_module),
):
    return module.get_income()


@router.get("/indexes")
def get_indexes(
    module: AnalyticsModule = Depends(get_analytics_module),
):
    return module.get_indexes()


@router.post("/performance")
def get_performance(
    body: AnalyticsPerformanceRequest,
    module: AnalyticsModule = Depends(get_analytics_module),
):
    try:
        return module.get_performance(
            body.to_filters(), include_dividends=body.include_dividends
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
