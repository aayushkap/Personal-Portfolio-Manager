# app/api/system.py

from fastapi import APIRouter, Depends, Query
from app.api.deps import get_system_module
from app.services.system import SystemModule

router = APIRouter(prefix="/system", tags=["System"])


@router.post("/health")
def get_health(
    module: SystemModule = Depends(get_system_module),
):
    return module.get_health()


@router.post("/llm")
def get_llm_metrics(
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    module: SystemModule = Depends(get_system_module),
):
    return module.get_llm_metrics(limit=limit, offset=offset)
