"""HTTP API for the consolidated Portfolio Performance page."""

from fastapi import APIRouter, Depends, HTTPException

from app.api.deps import get_performance_module
from app.api.schema import PerformancePageRequest
from app.services.performance import PerformanceModule

router = APIRouter(prefix="/performance", tags=["Performance"])


@router.post("")
def get_performance(
    body: PerformancePageRequest,
    module: PerformanceModule = Depends(get_performance_module),
):
    """Return the verdict, benchmark chart, positions, and risk/reward data."""
    try:
        return module.get_performance(
            body.to_filters(),
            include_dividends=body.include_dividends,
            benchmark_mode=body.benchmark_mode,
            benchmark_indices=body.benchmark_indices,
            x_axis=body.x_axis,
            y_axis=body.y_axis,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
