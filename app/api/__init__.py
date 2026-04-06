# app/api/__init_.py

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import asyncio
from contextlib import asynccontextmanager

from app.worker import main as worker_main
from dotenv import load_dotenv
from app.api.overview import router

load_dotenv()


@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(worker_main())
    yield


app = FastAPI(title="APP NAME", lifespan=lifespan)
app.include_router(router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    return {"status": "ok", "service": "APP NAME"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.api:app", host="0.0.0.0", port=8000, reload=False)
