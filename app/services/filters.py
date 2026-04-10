"""
services/filters.py
-------------------
Unified filter state shared across all dashboard modules.
All public service methods accept a PortfolioFilters instance — nothing else.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Optional


@dataclass
class DateRange:
    """
    Inclusive date window applied to all charts and aggregations.
    The filter only ever narrows price/dividend series — it never affects
    KPI sparklines, which always use their own fixed lookback window.
    """

    start: date = field(default_factory=lambda: date.today() - timedelta(days=365))
    end: date = field(default_factory=date.today)

    def __post_init__(self) -> None:
        if self.start > self.end:
            raise ValueError(
                f"DateRange: start ({self.start}) cannot be after end ({self.end})"
            )

    @property
    def days(self) -> int:
        return (self.end - self.start).days


@dataclass
class PortfolioFilters:
    """
    Unified filter object passed to every module service method.

    Global filters (date_range, sectors, exchanges) affect KPIs, chips,
    and chart series simultaneously. None values mean "include all".

    Convenience class-methods cover the most common filter presets.
    """

    date_range: DateRange = field(default_factory=DateRange)
    sectors: Optional[list[str]] = None  # None = all sectors
    exchanges: Optional[list[str]] = None  # None = DFM + ADX
    tickers: Optional[list[str]] = None  # None = all current holdings
    overlays: Optional[list[str]] = None

    # Preset constructors
    @classmethod
    def default(cls) -> "PortfolioFilters":
        """Default: last 365 days, all holdings."""
        return cls()

    @classmethod
    def ytd(cls) -> "PortfolioFilters":
        """Year-to-date from Jan 1 of the current year."""
        today = date.today()
        return cls(date_range=DateRange(start=date(today.year, 1, 1), end=today))

    @classmethod
    def last_n_days(cls, n: int) -> "PortfolioFilters":
        today = date.today()
        return cls(date_range=DateRange(start=today - timedelta(days=n), end=today))

    @classmethod
    def inception(cls) -> "PortfolioFilters":
        """Full history from the earliest possible date."""
        return cls(date_range=DateRange(start=date(2000, 1, 1), end=date.today()))
