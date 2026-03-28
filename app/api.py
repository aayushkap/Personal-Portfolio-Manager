# app.py
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic_settings import BaseSettings
import json
import asyncio
from pathlib import Path
from contextlib import asynccontextmanager

from worker import main as worker_main


from pydantic import ConfigDict


class Settings(BaseSettings):
    model_config = ConfigDict(extra="ignore")

    APP_NAME: str = "Analytics API"
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    ALLOWED_ORIGINS: list[str] = ["*"]
    ANALYTICS_FILE: str = "analytics_output.json"


settings = Settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(worker_main())
    yield


app = FastAPI(title=settings.APP_NAME, lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    return {"status": "ok", "service": settings.APP_NAME}


@app.get("/analytics")
async def get_analytics():
    file_path = Path(settings.ANALYTICS_FILE)
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="Worker still initializing...")
    try:
        with open(file_path) as f:
            return json.load(f)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("api:app", host=settings.HOST, port=settings.PORT, reload=False)
