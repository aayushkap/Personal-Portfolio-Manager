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
from typing import Literal, Optional, List

from pydantic import BaseModel, field_validator, model_validator

from app.services.filters import DateRange, PortfolioFilters
from pydantic import Field


class PerformanceRequest(BaseModel):
    start_date: date = Field(
        default_factory=lambda: (date.today() - timedelta(days=120))
    )
    end_date: date = Field(default_factory=lambda: date.today())
    instruments: Optional[List[str]] = None
    sectors: Optional[List[str]] = None
    include_events: bool = False
    overlays: List[str] = Field(default_factory=list)
    breakdown: bool = False
    period_returns: bool = False


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

    @model_validator(mode="after")
    def validate_order(self):
        if self.start > self.end:
            raise ValueError("date_range.start must be on or before date_range.end")
        return self

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


class AnalyticsPerformanceRequest(BaseModel):
    """Request options for portfolio performance analytics.

    Performance is always calculated for the complete portfolio. Keeping this
    request deliberately small prevents its return and benchmark calculations
    from being compared across different, independently filtered universes.
    """

    date_range: DateRangeRequest = Field(default_factory=DateRangeRequest)
    include_dividends: bool = True

    def to_filters(self) -> PortfolioFilters:
        return PortfolioFilters(date_range=self.date_range.to_domain())


class PerformancePageRequest(BaseModel):
    """Options for the consolidated Portfolio Performance page."""

    date_range: DateRangeRequest = Field(default_factory=DateRangeRequest)
    include_dividends: bool = True
    benchmark_mode: Literal["blended", "single"] = "blended"
    benchmark_indices: list[str] | None = None
    x_axis: Literal[
        "volatility_annualized_pct",
        "max_drawdown_pct",
        "beta",
        "sharpe_ratio",
        "sortino_ratio",
        "risk_contribution_pct",
        "weight_pct",
        "yield_on_cost_pct",
        "twr_pct",
    ] = "volatility_annualized_pct"
    y_axis: Literal[
        "volatility_annualized_pct",
        "max_drawdown_pct",
        "beta",
        "sharpe_ratio",
        "sortino_ratio",
        "risk_contribution_pct",
        "weight_pct",
        "yield_on_cost_pct",
        "twr_pct",
    ] = "twr_pct"

    @field_validator("benchmark_indices")
    @classmethod
    def clean_benchmark_indices(cls, values: list[str] | None) -> list[str] | None:
        if values is None:
            return None
        cleaned = list(
            dict.fromkeys(value.strip().upper() for value in values if value.strip())
        )
        if not cleaned:
            raise ValueError(
                "benchmark_indices must contain at least one non-empty index"
            )
        return cleaned

    def to_filters(self) -> PortfolioFilters:
        return PortfolioFilters(date_range=self.date_range.to_domain())
