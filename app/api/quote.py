# app/api/quote.py

from fastapi import APIRouter
from app.services.quote import QuoteStore

router = APIRouter(prefix="/quote", tags=["Quote"])


@router.post("/")
def get_quote():
    return QuoteStore.read()
