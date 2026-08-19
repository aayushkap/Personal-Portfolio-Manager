# app/api/__init_.py

import time

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from dotenv import load_dotenv
from app.api.overview import router as overview
from app.api.analytics import router as analytics
from app.api.correlation import router as correlation
from app.api.holdings import router as holdings
from app.api.watchlist import router as watchlist
from app.api.metadata import router as metadata
from app.api.quote import router as quote
from app.api.system import router as system
from app.core.logger import get_logger

load_dotenv()

logger = get_logger()

app = FastAPI(title="HSFW BE")
app.include_router(overview)
app.include_router(analytics)
app.include_router(correlation)
app.include_router(holdings)
app.include_router(watchlist)
app.include_router(metadata)
app.include_router(quote)
app.include_router(system)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def log_request(request: Request, call_next):
    """Record every completed API request in the live API log."""
    started_at = time.perf_counter()
    status_code = 500
    try:
        response = await call_next(request)
        status_code = response.status_code
        return response
    finally:
        logger.info(
            "API request | method=%s path=%s status=%d elapsed_ms=%.1f",
            request.method,
            request.url.path,
            status_code,
            (time.perf_counter() - started_at) * 1000,
        )


@app.get("/")
async def root():
    return {"status": "ok", "service": "HSFW BE"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.api:app", host="0.0.0.0", port=8080, reload=False, access_log=False
    )
