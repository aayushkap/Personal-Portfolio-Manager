# app/api/__init_.py

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import asyncio
from contextlib import asynccontextmanager

from app.worker import main as worker_main
from dotenv import load_dotenv
from app.api.overview import router as overview
from app.api.analytics import router as analytics
from app.api.correlation import router as correlation
from app.api.holdings import router as holdings
from app.api.watchlist import router as watchlist
from app.api.metadata import router as metadata
from app.api.quote import router as quote

load_dotenv()


@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(worker_main())
    yield


app = FastAPI(title="BBBB BE", lifespan=lifespan)
app.include_router(overview)
app.include_router(analytics)
app.include_router(correlation)
app.include_router(holdings)
app.include_router(watchlist)
app.include_router(metadata)
app.include_router(quote)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    return {"status": "ok", "service": "BBBB BE"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.api:app", host="0.0.0.0", port=8000, reload=False)
