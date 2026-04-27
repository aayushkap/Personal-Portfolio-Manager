# app/api/quote.py

from typing import Literal
from fastapi import APIRouter, Depends
from app.services.quote import QuoteStore
from app.api.deps import get_analytics_module

router = APIRouter(prefix="/quote", tags=["Quote"])


@router.post("/")
async def get_quote():
    return QuoteStore.read()
