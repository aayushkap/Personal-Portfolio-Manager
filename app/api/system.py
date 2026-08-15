# app/api/system.py

from fastapi import APIRouter, Depends
from app.api.deps import get_system_module
from app.services.system import SystemModule

router = APIRouter(prefix="/system", tags=["System"])


@router.post("/health")
async def get_health(
    module: SystemModule = Depends(get_system_module),
):
    return module.get_health()
