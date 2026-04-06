"""
api/deps.py
-----------
FastAPI dependency providers.

All heavy objects (Cache, DB, modules) are instantiated ONCE at startup
and reused across every request via lru_cache. This means:
  - No repeated file-system or DB connection setup per request
  - All modules share the same underlying Cache and DB instances
  - Swapping implementations (e.g. for testing) is done in one place

Usage in a route:
    @router.post("/kpis")
    async def kpis(module: OverviewModule = Depends(get_overview_module)):
        ...
"""

from functools import lru_cache

from app.data.cache import Cache
from app.data.db import DB
from app.services.overview import OverviewModule


@lru_cache(maxsize=1)
def get_cache() -> Cache:
    return Cache()


@lru_cache(maxsize=1)
def get_db() -> DB:
    return DB()


# Module providers (one per module, all share the same Cache + DB)
@lru_cache(maxsize=1)
def get_overview_module() -> OverviewModule:
    return OverviewModule(cache=get_cache(), db=get_db())
