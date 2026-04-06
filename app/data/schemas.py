"""
app/data/schemas.py
-------------------
Typed schemas for the scraped ticker cache JSON.

Usage:
    raw = cache.load("ADX:FAB")
    ticker = TickerCache.model_validate(raw)

    # Full intellisense from here:
    ticker.dividends.rows[0].pay_date
    ticker.statistics.sections.dividends_and_yields.dividend_yield
    ticker.purchase_details[0].trade_date
"""

from __future__ import annotations

from datetime import date
from typing import Optional, Any
from pydantic import BaseModel, Field, field_validator


#  Shared
class ScrapedSection(BaseModel):
    """Base for any scraped sub-page that has common metadata."""

    symbol: Optional[str] = None
    exchange: Optional[str] = None
    url: Optional[str] = None
    scraped_at: Optional[str] = None
    error: Optional[str] = None


#  Overview
class OverviewStats(BaseModel):
    market_cap: Optional[str] = Field(None, alias="Market Cap")
    pe_ratio: Optional[str] = Field(None, alias="PE Ratio")
    forward_pe: Optional[str] = Field(None, alias="Forward PE")
    eps: Optional[str] = Field(None, alias="EPS")
    dividend: Optional[str] = Field(None, alias="Dividend")
    ex_dividend_date: Optional[str] = Field(None, alias="Ex-Dividend Date")
    earnings_date: Optional[str] = Field(None, alias="Earnings Date")
    volume: Optional[str] = Field(None, alias="Volume")
    week_52_range: Optional[str] = Field(None, alias="52-Week Range")
    beta: Optional[str] = Field(None, alias="Beta")

    model_config = {"populate_by_name": True, "extra": "allow"}


class OverviewData(ScrapedSection):
    price: Optional[str] = None
    price_change: Optional[str] = None
    stats: OverviewStats = Field(default_factory=OverviewStats)

    @field_validator("stats", mode="before")
    @classmethod
    def coerce_stats(cls, v):
        return v if isinstance(v, dict) else {}


#  Dividends
class DividendRow(BaseModel):
    ex_date: Optional[date] = Field(None, alias="Ex-Dividend Date")
    cash_amount: Optional[str] = Field(None, alias="Cash Amount")
    record_date: Optional[date] = Field(None, alias="Record Date")
    pay_date: Optional[date] = Field(None, alias="Pay Date")

    model_config = {"populate_by_name": True}

    @field_validator("ex_date", "record_date", "pay_date", mode="before")
    @classmethod
    def parse_date(cls, v):
        if not v or str(v).strip() in {"-", "None", ""}:
            return None
        if isinstance(v, date):
            return v
        try:
            return date.fromisoformat(str(v).strip())
        except ValueError:
            return None


class DividendsData(ScrapedSection):
    headers: list[str] = Field(default_factory=list)
    rows: list[DividendRow] = Field(default_factory=list)

    @field_validator("rows", mode="before")
    @classmethod
    def coerce_rows(cls, v):
        return v if isinstance(v, list) else []


#  Statistics
class DividendsAndYields(BaseModel):
    dividend_per_share: Optional[str] = Field(None, alias="Dividend Per Share")
    dividend_yield: Optional[str] = Field(None, alias="Dividend Yield")
    dividend_growth_yoy: Optional[str] = Field(None, alias="Dividend Growth (YoY)")
    payout_ratio: Optional[str] = Field(None, alias="Payout Ratio")
    years_of_growth: Optional[str] = Field(None, alias="Years of Dividend Growth")
    earnings_yield: Optional[str] = Field(None, alias="Earnings Yield")

    model_config = {"populate_by_name": True, "extra": "allow"}


class ImportantDates(BaseModel):
    earnings_date: Optional[str] = Field(None, alias="Earnings Date")
    ex_dividend_date: Optional[str] = Field(None, alias="Ex-Dividend Date")

    model_config = {"populate_by_name": True, "extra": "allow"}


class FinancialEfficiency(BaseModel):
    roe: Optional[str] = Field(None, alias="Return on Equity (ROE)")
    roa: Optional[str] = Field(None, alias="Return on Assets (ROA)")
    roic: Optional[str] = Field(None, alias="Return on Invested Capital (ROIC)")
    wacc: Optional[str] = Field(None, alias="Weighted Average Cost of Capital (WACC)")

    model_config = {"populate_by_name": True, "extra": "allow"}


class ValuationRatios(BaseModel):
    pe_ratio: Optional[str] = Field(None, alias="PE Ratio")
    forward_pe: Optional[str] = Field(None, alias="Forward PE")
    ps_ratio: Optional[str] = Field(None, alias="PS Ratio")
    pb_ratio: Optional[str] = Field(None, alias="PB Ratio")
    peg_ratio: Optional[str] = Field(None, alias="PEG Ratio")

    model_config = {"populate_by_name": True, "extra": "allow"}


class StatisticsSections(BaseModel):
    dividends_and_yields: DividendsAndYields = Field(default_factory=DividendsAndYields)
    important_dates: ImportantDates = Field(default_factory=ImportantDates)
    financial_efficiency: FinancialEfficiency = Field(
        default_factory=FinancialEfficiency
    )
    valuation_ratios: ValuationRatios = Field(default_factory=ValuationRatios)
    raw: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_raw_sections(cls, sections: dict) -> "StatisticsSections":
        return cls(
            dividends_and_yields=DividendsAndYields.model_validate(
                sections.get("Dividends & Yields", {})
            ),
            important_dates=ImportantDates.model_validate(
                sections.get("Important Dates", {})
            ),
            financial_efficiency=FinancialEfficiency.model_validate(
                sections.get("Financial Efficiency", {})
            ),
            valuation_ratios=ValuationRatios.model_validate(
                sections.get("Valuation Ratios", {})
            ),
            raw=sections,
        )


class StatisticsData(ScrapedSection):
    sections: StatisticsSections = Field(default_factory=StatisticsSections)

    @field_validator("sections", mode="before")
    @classmethod
    def coerce_sections(cls, v):
        if isinstance(v, dict):
            return StatisticsSections.from_raw_sections(v)
        return v


#  Financials / Ratios (tabular, keep flexible)
class TabularData(ScrapedSection):
    headers: list[str] = Field(default_factory=list)
    rows: list[dict[str, Any]] = Field(default_factory=list)


#  Purchase Details
class PurchaseDetail(BaseModel):
    symbol: str
    exchange: str
    transaction: str
    platform: Optional[str] = None
    sector: Optional[str] = None
    purchase_date: Optional[date] = None
    shares: float
    cost_per_share: Optional[str] = None
    commission_paid: Optional[str] = Field(None, alias="commision_paid")
    total_cost: Optional[str] = None
    logo_url: Optional[str] = None

    model_config = {"populate_by_name": True}

    @field_validator("purchase_date", mode="before")
    @classmethod
    def parse_date(cls, v):
        if not v:
            return None
        if isinstance(v, date):
            return v
        try:
            return date.fromisoformat(str(v).strip())
        except ValueError:
            return None

    @property
    def ticker(self) -> str:
        return f"{self.exchange}:{self.symbol}"


#  Top-level Cache Entry
class TickerCache(BaseModel):
    """
    Typed wrapper around a single ticker's cache JSON.

    Usage:
        raw  = cache.load("ADX:FAB")
        data = TickerCache.model_validate(raw)

        data.dividends.rows[0].pay_date          # date | None
        data.statistics.sections.dividends_and_yields.dividend_yield  # "4.608%"
        data.purchase_details[0].purchase_date   # date | None
    """

    ticker: Optional[str] = None
    scraped_at: Optional[str] = None
    overview: Optional[OverviewData] = None
    financials: Optional[TabularData] = None
    dividends: Optional[DividendsData] = None
    statistics: Optional[StatisticsData] = None
    ratios: Optional[TabularData] = None
    purchase_details: list[PurchaseDetail] = Field(default_factory=list)

    model_config = {"extra": "allow"}
