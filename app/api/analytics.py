# app/api/analytics.py

from typing import Literal
from fastapi import APIRouter, Depends
from app.services.analytics import AnalyticsModule
from app.api.deps import get_analytics_module

router = APIRouter(prefix="/analytics", tags=["Analytics"])


@router.post("/pnl")
async def get_pnl(
    mode: Literal["price_return", "total"] = "total",
    module: AnalyticsModule = Depends(get_analytics_module),
):
    return module.get_pnl(mode=mode)


@router.post("/allocation")
async def get_allocation(
    by: Literal["position", "sector", "exchange"] = "position",
    module: AnalyticsModule = Depends(get_analytics_module),
):
    return module.get_allocation(by=by)


@router.post("/income")
async def get_income(
    module: AnalyticsModule = Depends(get_analytics_module),
):
    return module.get_income()
