from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

from app.data.rescrape_queue import RescrapeQueue
from app.data.ticker import parse_ticker

router = APIRouter(prefix="/rescrape", tags=["Rescrape"])


class RescrapeRequestBody(BaseModel):
    ticker: str


@router.post("", status_code=status.HTTP_202_ACCEPTED)
def schedule_rescrape(payload: RescrapeRequestBody):
    """Prioritize one fundamentals refresh in the existing scheduled worker."""
    ticker = parse_ticker(payload.ticker.strip().replace(" ", ""))
    if not ticker:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="ticker must use EXCHANGE:SYMBOL format",
        )

    request, already_queued = RescrapeQueue().schedule(ticker.key)
    return {
        "ticker": request.ticker,
        "status": "already_queued" if already_queued else "queued",
        "requested_at": request.requested_at,
    }
