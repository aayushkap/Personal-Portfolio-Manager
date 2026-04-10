# app/services/watchlist.py

from __future__ import annotations

from datetime import date, timedelta
from typing import Optional
import math

import pandas as pd

from app.core.logger import get_logger
from app.services.base import BaseModule

logger = get_logger()


def _safe(v):
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v


def _pct(new: Optional[float], old: Optional[float]) -> Optional[float]:
    if new is None or old is None or old == 0:
        return None
    return round((new - old) / old * 100, 2)


class WatchlistModule(BaseModule):
    def get_watchlist(self, items: list[dict]) -> list[dict]:
        if not items:
            return []

        today = date.today()
        tickers = [i["ticker"] for i in items if i.get("ticker")]
        prices = self.get_price_series(
            tickers, today - timedelta(days=400), today
        ).ffill()

        return [
            row
            for item in items
            if item.get("ticker")
            for row in [self._build_row(item, prices, today)]
            if row
        ]

    def get_watchlist_detail(self, ticker: str, timeframe: str = "1m") -> dict:
        today = date.today()
        return {
            "ticker": ticker,
            "chart": self._build_chart(ticker, timeframe, today),
            "fundamentals": self._build_fundamentals(ticker),
        }

    def _build_row(
        self, item: dict, prices: pd.DataFrame, today: date
    ) -> Optional[dict]:
        ticker = item["ticker"]

        current_price = self.get_latest_price(ticker)
        col = prices.get(ticker) if isinstance(prices, pd.DataFrame) else None

        def _ago(days: int) -> Optional[float]:
            if col is None or col.empty:
                return None
            cutoff = pd.Timestamp(today - timedelta(days=days), tz="Asia/Dubai")
            past = col[col.index <= cutoff]
            return float(past.iloc[-1]) if not past.empty else None

        p1d = _ago(1)
        p1w = _ago(7)
        p1m = _ago(30)
        p3m = _ago(90)
        p6m = _ago(180)
        p1y = _ago(365)

        next_div_date, div_yield = self._next_dividend(ticker)

        meta = self._ticker_meta(ticker)

        return {
            "ticker": ticker,
            "name": item.get("name") or meta.get("name") or ticker,
            "exchange": item.get("exchange") or meta.get("exchange"),
            "sector": item.get("sector") or meta.get("sector"),
            "logo_url": meta.get("logo_url"),
            "notes": item.get("notes"),
            "current_price": _safe(current_price),
            "dod_pct": _safe(_pct(current_price, p1d)),
            "wow_pct": _safe(_pct(current_price, p1w)),
            "mom_pct": _safe(_pct(current_price, p1m)),
            "three_month_pct": _safe(_pct(current_price, p3m)),
            "six_month_pct": _safe(_pct(current_price, p6m)),
            "yoy_pct": _safe(_pct(current_price, p1y)),
            "next_div_date": next_div_date,
            "div_yield": div_yield,
        }

    def _next_dividend(self, ticker: str) -> tuple[Optional[str], Optional[str]]:
        today = date.today()
        upcoming = [
            d for d in self.get_dividends(ticker) if d.ex_date and d.ex_date >= today
        ]
        if not upcoming:
            return None, None

        nxt = min(upcoming, key=lambda d: d.ex_date)

        # Yield from statistics if available
        div_yield = None
        data = self.get_ticker(ticker)
        if data and data.statistics and data.statistics.sections:
            div = data.statistics.sections.dividends_and_yields
            if div:
                raw = getattr(div, "dividend_yield", None)
                div_yield = (
                    str(raw).strip()
                    if raw and str(raw) not in ("", "-", "n/a")
                    else None
                )

        return nxt.ex_date.isoformat(), div_yield

    def _ticker_meta(self, ticker: str) -> dict:
        data = self.get_ticker(ticker)
        if not data or not data.purchase_details:
            return {}
        pd_row = data.purchase_details[0]
        return {
            "name": getattr(pd_row, "name", None) or ticker,
            "sector": getattr(pd_row, "sector", None),
            "exchange": getattr(pd_row, "exchange", None),
            "logo_url": getattr(pd_row, "logo_url", None),
        }

    # Reuse the same chart + fundamentals from HoldingsModule
    # Import and call directly to avoid duplicating the logic
    def _build_chart(self, ticker: str, timeframe: str, today: date) -> list[dict]:
        from app.services.holdings import HoldingsModule

        return HoldingsModule._build_chart(self, ticker, timeframe, today)

    def _build_fundamentals(self, ticker: str) -> dict:
        from app.services.holdings import HoldingsModule

        return HoldingsModule._build_fundamentals(self, ticker)
