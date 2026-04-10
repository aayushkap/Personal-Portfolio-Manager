# app/api/correlation.py

from typing import List
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from app.services.correlation import CorrelationModule, Period
from app.api.deps import get_correlation_module

router = APIRouter(prefix="/correlation", tags=["Correlation"])


class CorrelationRequest(BaseModel):
    instruments: List[str]
    period: Period = "1y"


@router.post("")
async def get_correlation(
    body: CorrelationRequest,
    module: CorrelationModule = Depends(get_correlation_module),
):
    return module.get_matrix(body.instruments, body.period)
