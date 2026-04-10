"""
api/schemas.py
--------------
Pydantic request/response schemas for the API layer.

Why a separate schema layer?
  The service layer uses Python dataclasses (PortfolioFilters, DateRange).
  FastAPI uses Pydantic for request body parsing, validation, and OpenAPI docs.
  Keeping these separate means:
    - The service layer has zero dependency on FastAPI or Pydantic
    - API schemas can evolve (rename fields, add validation) without
      touching the service layer
    - .to_filters() is the only bridge between the two worlds

All request schemas live here. Response shapes are returned as plain dicts
or DataFrames converted with .to_dict(orient="records") — no response
schemas needed unless strict typing is required later.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Optional, List

from pydantic import BaseModel, field_validator, model_validator

from app.services.filters import DateRange, PortfolioFilters
from pydantic import Field


class PerformanceRequest(BaseModel):
    start_date: date = Field(default_factory=lambda: date.today() - timedelta(days=120))
    end_date: date = Field(default_factory=lambda: date.today())
    instruments: Optional[List[str]] = None
    sectors: Optional[List[str]] = None
    include_dividends_and_events: bool = False
    overlays: List[str] = Field(default_factory=list)


class DateRangeRequest(BaseModel):
    start: date = None  # type: ignore[assignment]  — defaults set in validator
    end: date = None  # type: ignore[assignment]

    @model_validator(mode="before")
    @classmethod
    def apply_defaults(cls, values: dict) -> dict:
        today = date.today()
        values.setdefault("start", today - timedelta(days=365))
        values.setdefault("end", today)
        return values

    @field_validator("start", "end", mode="before")
    @classmethod
    def parse_date(cls, v):
        if isinstance(v, str):
            return date.fromisoformat(v)
        return v

    def to_domain(self) -> DateRange:
        return DateRange(start=self.start, end=self.end)


class FilterRequest(BaseModel):
    """
    Universal filter body accepted by every /overview, /dividends,
    /risk endpoint. All fields are optional — omitting them means "all".

    Example payloads:

      # Full portfolio, last year (default)
      {}

      # YTD, only DFM stocks
      { "date_range": { "start": "2026-01-01" }, "exchanges": ["DFM"] }

      # Specific tickers, custom window
      {
        "date_range": { "start": "2025-06-01", "end": "2026-03-31" },
        "tickers": ["DFM:DEWA", "DFM:EMAAR"]
      }
    """

    date_range: DateRangeRequest = DateRangeRequest()
    sectors: Optional[list[str]] = None
    exchanges: Optional[list[str]] = None
    tickers: Optional[list[str]] = None

    def to_filters(self) -> PortfolioFilters:
        """Converts the API request schema into the service-layer domain object."""
        return PortfolioFilters(
            date_range=self.date_range.to_domain(),
            sectors=self.sectors,
            exchanges=self.exchanges,
            tickers=self.tickers,
        )
