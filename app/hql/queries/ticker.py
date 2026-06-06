# app/hql/queries/ticker.py

from __future__ import annotations

import pandas as pd
from datetime import date
from typing import Any

from app.hql.repositories import CacheRepository, PriceRepository
from app.hql.queries.__init__ import (
    _tabular_rows_to_df,
    _coerce_period_df,
    _coerce_date_range,
    _overview_stats,
    _statistics_sections,
    _first_purchase_detail,
)
from app.utils.parsers import (
    parse_money_string,
    parse_any_stat,
    parse_mixed_stat,
    parse_price_target,
    parse_range,
    parse_date,
)
from app.hql.constants import FIELD_MAP
from app.hql.errors import HQLFieldError
from app.hql.repositories import FXService


def _extract_field_from_raw(raw: dict, field: str) -> Any:
    """
    Resolve a canonical HQL field alias from FIELD_MAP against raw statistics.
    """
    if field not in FIELD_MAP:
        raise HQLFieldError(field)

    root, section, label = FIELD_MAP[field]

    if root != "statistics":
        return None

    sections = _statistics_sections(raw)
    value = (sections.get(section) or {}).get(label)
    return parse_any_stat(value)


def _normalize_stat_block(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _normalize_stat_block(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_normalize_stat_block(v) for v in value]
    return parse_any_stat(value)


class TickerQuery:
    """
    Query interface for a single ticker.

    Methods
    -------
    raw()         -> dict
    info()        -> dict
    overview()    -> dict
    statistics()  -> dict
    prices()      -> pd.Series
    ohlcv()       -> pd.DataFrame
    dividends()   -> pd.DataFrame
    """

    def __init__(
        self,
        ticker: str,
        cache_repo: CacheRepository,
        price_repo: PriceRepository,
        fx: FXService,
    ) -> None:
        self.ticker = ticker.upper()
        self.cache_repo = cache_repo
        self.price_repo = price_repo
        self.fx = fx

    def raw(self) -> dict:
        """
        Return the exact raw cache payload for the ticker.

        Output schema:
            dict  # exact cache JSON
        """
        return self.cache_repo.get_raw_ticker(self.ticker)

    def info(self) -> dict:
        """
        Return lightweight identity and snapshot metadata.

        Output schema:
        {
            "ticker": str,
            "symbol": str | None,
            "exchange": str | None,
            "sector": str | None,
            "logo_url": str | None,
            "scraped_at": str | None,
            "last_updated": str | None,
            "last_price_aed": float | None,
            "currency": str
        }
        """
        raw = self.raw()
        overview = raw.get("overview") or {}
        stats = overview.get("stats") or {}
        pd0 = _first_purchase_detail(raw) or {}

        prev_close, prev_currency = parse_money_string(stats.get("Previous Close"))
        currency = self.cache_repo.resolve_currency(raw)

        if prev_close is None:
            prev_close = parse_any_stat(stats.get("Previous Close"))
        last_price_aed = self.fx.to_aed(prev_close, prev_currency or currency)

        return {
            "ticker": raw.get("ticker") or self.ticker,
            "symbol": overview.get("symbol"),
            "exchange": overview.get("exchange"),
            "sector": pd0.get("sector"),
            "logo_url": pd0.get("logo_url"),
            "scraped_at": raw.get("scraped_at"),
            "last_updated": raw.get("last_updated"),
            "last_price_aed": last_price_aed,
            "currency": "AED",
        }

    def overview(self) -> dict:
        """
        Return a curated snapshot of overview metrics.

        Output schema:
        {
            "market_cap": float | None,
            "market_cap_change": float | None,
            "revenue_ttm": float | None,
            "revenue_ttm_change": float | None,
            "net_income": float | None,
            "net_income_change": float | None,
            "eps": float | None,
            "eps_change": float | None,
            "shares_out": float | None,
            "pe_ratio": float | None,
            "forward_pe": float | None,
            "dividend_per_share": float | None,
            "dividend_yield": float | None,
            "ex_dividend_date": date | None,
            "earnings_date": date | None,
            "volume": float | None,
            "open": float | None,
            "previous_close": float | None,
            "day_range_low": float | None,
            "day_range_high": float | None,
            "week_52_low": float | None,
            "week_52_high": float | None,
            "beta": float | None,
            "analyst_rating": str | None,
            "price_target": float | None,
            "price_target_upside": float | None,
        }
        """
        raw = self.raw()
        stats = _overview_stats(raw)
        currency = self.cache_repo.resolve_currency(raw)

        market_cap = parse_mixed_stat(stats.get("Market Cap"))
        revenue = parse_mixed_stat(stats.get("Revenue (ttm)"))
        net_income = parse_mixed_stat(stats.get("Net Income"))
        eps = parse_mixed_stat(stats.get("EPS"))
        target = parse_price_target(stats.get("Price Target"))
        open_px = self.fx.to_aed(parse_any_stat(stats.get("Open")), currency)
        prev_close = self.fx.to_aed(
            parse_any_stat(stats.get("Previous Close")), currency
        )
        dlow, dhigh = parse_range(stats.get("Day's Range"))
        wlow, whigh = parse_range(stats.get("52-Week Range"))
        dlow = self.fx.to_aed(dlow, currency)
        dhigh = self.fx.to_aed(dhigh, currency)
        wlow = self.fx.to_aed(wlow, currency)
        whigh = self.fx.to_aed(whigh, currency)

        dividend_raw = str(stats.get("Dividend") or "").strip()
        dividend_per_share = None
        dividend_yield = None
        if dividend_raw:
            # Example: "1.00 (8.49%)"
            left = dividend_raw.split("(")[0].strip()
            dividend_per_share = parse_any_stat(left)
            if "(" in dividend_raw and ")" in dividend_raw:
                inside = dividend_raw.split("(")[1].split(")")[0].strip()
                dividend_yield = parse_any_stat(inside)

        return {
            "market_cap": market_cap["value"],
            "market_cap_change": market_cap["change"],
            "revenue_ttm": revenue["value"],
            "revenue_ttm_change": revenue["change"],
            "net_income": net_income["value"],
            "net_income_change": net_income["change"],
            "eps": eps["value"],
            "eps_change": eps["change"],
            "shares_out": parse_any_stat(stats.get("Shares Out")),
            "pe_ratio": parse_any_stat(stats.get("PE Ratio")),
            "forward_pe": parse_any_stat(stats.get("Forward PE")),
            "dividend_per_share": dividend_per_share,
            "dividend_yield": dividend_yield,
            "ex_dividend_date": parse_date(stats.get("Ex-Dividend Date")),
            "earnings_date": parse_date(stats.get("Earnings Date")),
            "volume": parse_any_stat(stats.get("Volume")),
            "open": open_px,
            "previous_close": prev_close,
            "day_range_low": dlow,
            "day_range_high": dhigh,
            "week_52_low": wlow,
            "week_52_high": whigh,
            "beta": parse_any_stat(stats.get("Beta")),
            "analyst_rating": stats.get("Analysts"),
            "price_target": self.fx.to_aed(target["value"], currency),
            "price_target_upside": target["upside"],
        }

    def statistics(self) -> dict:
        """
        Return the full normalized statistics.sections block.

        Output schema:
            {
                "<Section Name>": {
                    "<Metric Label>": float | str | None,
                    ...
                },
                ...
            }
        """
        raw = self.raw()
        sections = _statistics_sections(raw)
        return _normalize_stat_block(sections)

    def prices(
        self,
        days: int | None = 365,
        start: date | str | None = None,
        end: date | str | None = None,
        granularity: str = "1D",
    ) -> pd.Series:
        """
        Return daily close prices in AED.

        Output schema:
            pd.Series
            - index: pd.DatetimeIndex(tz="Asia/Dubai")
            - name: ticker
            - values: float (AED close price)
        """
        start_date, end_date = _coerce_date_range(days=days, start=start, end=end)
        return self.price_repo.get_ohlcv(
            self.ticker, start_date, end_date, granularity=granularity
        )

    def ohlcv(
        self,
        days: int | None = 90,
        start: date | str | None = None,
        end: date | str | None = None,
        granularity: str = "1D",
    ) -> pd.DataFrame:
        """
        Return daily OHLCV bars in AED.

        Output schema:
            pd.DataFrame
            - index: pd.DatetimeIndex(tz="Asia/Dubai")
            - columns: open, high, low, close, volume
            - OHLC values in AED
        """
        start_date, end_date = _coerce_date_range(days=days, start=start, end=end)
        return self.price_repo.get_ohlcv(
            self.ticker,
            start_date,
            end_date,
            granularity=granularity,
        )

    def dividends(self) -> pd.DataFrame:
        """
        Return dividend history with amount per share converted to AED.

        Output schema:
            pd.DataFrame columns:
                ex_date: date | None
                record_date: date | None
                pay_date: date | None
                amount_per_share_aed: float | None
                currency: str | None
        """
        raw = self.raw()
        rows = (raw.get("dividends") or {}).get("rows") or []
        out = []
        for row in rows:
            amount, currency = parse_money_string(row.get("Cash Amount"))
            amount_aed = self.fx.to_aed(
                amount, currency or self.cache_repo.resolve_currency(raw)
            )
            out.append(
                {
                    "ex_date": parse_date(row.get("Ex-Dividend Date")),
                    "record_date": parse_date(row.get("Record Date")),
                    "pay_date": parse_date(row.get("Pay Date")),
                    "amount_per_share_aed": amount_aed,
                    "currency": "AED",
                }
            )

        return pd.DataFrame(
            out,
            columns=[
                "ex_date",
                "record_date",
                "pay_date",
                "amount_per_share_aed",
                "currency",
            ],
        )

    def financials(self, period: str = "all") -> pd.DataFrame | pd.Series:
        """
        Return normalized financial statement history.

        Parameters
        ----------
        period:
            - "all"     -> full DataFrame
            - "ttm"     -> TTM Series
            - "FY 2025" -> specific fiscal period Series

        Output schema:
            pd.DataFrame or pd.Series
            DataFrame:
                - index: metric names
                - columns: fiscal periods
                - values: parsed float/str/None
        """
        raw = self.raw()
        section = raw.get("financials") or {}
        rows = section.get("rows") or []
        headers = section.get("headers") or []
        df = _tabular_rows_to_df(rows, headers)
        return _coerce_period_df(df, period)

    def ratios(self, period: str = "all") -> pd.DataFrame | pd.Series:
        """
        Return normalized ratios history.

        Parameters
        ----------
        period:
            - "all"       -> full DataFrame
            - "current"   -> Current Series
            - "FY 2025"   -> specific fiscal period Series

        Output schema:
            pd.DataFrame or pd.Series
            DataFrame:
                - index: metric names
                - columns: periods
                - values: parsed float/str/None
        """
        raw = self.raw()
        section = raw.get("ratios") or {}
        rows = section.get("rows") or []
        headers = section.get("headers") or []
        df = _tabular_rows_to_df(rows, headers)
        return _coerce_period_df(df, period)


class TickersQuery:
    """
    Query interface for multiple tickers.

    Methods
    -------
    prices()   -> pd.DataFrame
    compare()  -> pd.DataFrame
    """

    def __init__(
        self,
        tickers: list[str],
        cache_repo: CacheRepository,
        price_repo: PriceRepository,
        fx: FXService,
    ) -> None:
        self.tickers = [t.upper() for t in tickers]
        self.cache_repo = cache_repo
        self.price_repo = price_repo
        self.fx = fx

    def prices(
        self,
        days: int | None = 365,
        start: date | str | None = None,
        end: date | str | None = None,
        granularity: str = "1D",
    ) -> pd.DataFrame:
        """
        Return daily close prices in AED for multiple tickers.

        Output schema:
            pd.DataFrame
            - index: pd.DatetimeIndex(tz="Asia/Dubai")
            - columns: tickers
            - values: float (AED close prices)
        """
        start_date, end_date = _coerce_date_range(days=days, start=start, end=end)
        return self.price_repo.get_multi_close_series(
            self.tickers,
            start_date,
            end_date,
            granularity=granularity,
        )

    def compare(self, *fields: str) -> pd.DataFrame:
        """
        Compare canonical fields across multiple tickers.

        Supported fields are defined in FIELD_MAP, e.g.:
            pe, forward_pe, pb, ps, pfcf,
            roe, roa, roic, roce,
            div_yield, payout_ratio,
            gross_margin, op_margin, net_margin, fcf_margin,
            debt_equity, current_ratio,
            beta, rsi,
            altman_z, piotroski_f, graham_number

        Output schema:
            pd.DataFrame
            - index: ticker
            - columns: requested fields
        """
        fields = [f.strip().lower() for f in fields if str(f).strip()]
        rows = []

        for ticker in self.tickers:
            raw = self.cache_repo.get_raw_ticker(ticker)
            row = {"ticker": ticker}
            for field in fields:
                row[field] = _extract_field_from_raw(raw, field)
            rows.append(row)

        if not rows:
            return pd.DataFrame(columns=fields)

        return pd.DataFrame(rows).set_index("ticker")
