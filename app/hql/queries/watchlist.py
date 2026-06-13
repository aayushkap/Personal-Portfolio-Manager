from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import pandas as pd

from app.utils.parsers import parse_date
from app.hql.repositories import CacheRepository, FXService, PriceRepository


class WatchlistQuery:
    """
    Watchlist domain abstraction.

    Provides point-in-time metrics, historical returns, and dividend
    information for a provided list of watchlist items.
    """

    def __init__(
        self,
        cache_repo: CacheRepository,
        price_repo: PriceRepository,
        fx: FXService,
    ) -> None:
        self.cache_repo = cache_repo
        self.price_repo = price_repo
        self.fx = fx

    def _calculate_return(
        self, current: float | None, past: float | None
    ) -> float | None:
        if current is None or past is None or past == 0:
            return None
        return round(((current - past) / past) * 100, 2)

    def screener(
        self, items: list[dict[str, Any]], on: date | None = None
    ) -> pd.DataFrame:
        """
        Calculates performance metrics and metadata for a list of watchlist items.

        Parameters
        ----------
        items : list[dict]
            Raw items, typically containing at least 'ticker', 'notes', and 'criteria'.
        on : date, optional
            Reference date for calculating 'ago' metrics. Defaults to today.

        Returns
        -------
        pd.DataFrame
            Columns include current price, performance windows (1D, 1W, 1M, etc.),
            metadata, and next dividend info.
        """
        if not items:
            return pd.DataFrame()

        today = on or date.today()
        tickers = list({i.get("ticker") for i in items if i.get("ticker")})

        # Fetch exactly 1 year + margin of prices to calculate YoY
        start_date = today - timedelta(days=380)

        # Build a price matrix: Index = dates, Columns = tickers
        price_frames = []
        for ticker in tickers:
            df = self.price_repo.get_ohlcv(ticker, start=start_date, end=today)
            if not df.empty:
                price_frames.append(df["close"].rename(ticker))

        prices = pd.concat(price_frames, axis=1) if price_frames else pd.DataFrame()
        if not prices.empty:
            if prices.index.tz is not None:
                prices.index = prices.index.tz_localize(None)
            prices.index = prices.index.normalize()
            # Forward fill so weekends/holidays have a valid "ago" price
            prices = prices.reindex(pd.date_range(start_date, today, freq="D")).ffill()

        rows = []
        for item in items:
            ticker = item.get("ticker")
            if not ticker:
                continue

            # Base metadata
            try:
                raw = self.cache_repo.get_raw_ticker(ticker)
                # currency = self.cache_repo.resolve_currency(raw)
            except Exception:
                raw = {}
                # currency = "AED"

            # Latest price snapshot
            latest = self.price_repo.get_latest_price(ticker)
            current_price = latest.get("close")  # Already in AED via repo

            # Time-window lookbacks
            col = prices.get(ticker) if ticker in prices.columns else None

            def _price_ago(days: int) -> float | None:
                if col is None or col.empty:
                    return None
                target = pd.Timestamp(today - timedelta(days=days))
                past = col[col.index <= target]
                return float(past.iloc[-1]) if not past.empty else None

            p1d = _price_ago(1)
            p1w = _price_ago(7)
            p1m = _price_ago(30)
            p3m = _price_ago(90)
            p6m = _price_ago(180)
            p1y = _price_ago(365)

            # Dividend info
            div_rows = (raw.get("dividends") or {}).get("rows") or []
            upcoming = []
            for d in div_rows:
                ex_date = parse_date(d.get("Ex-Dividend Date") or d.get("ex_date"))
                if ex_date and ex_date >= today:
                    upcoming.append(ex_date)

            next_div_date = min(upcoming).isoformat() if upcoming else None

            # Get yield from stats
            div_yield = None
            stats = raw.get("statistics") or {}
            yield_str = str(stats.get("dividend_yield", "")).strip()
            if yield_str and yield_str not in ("-", "n/a", "None"):
                div_yield = yield_str

            rows.append(
                {
                    "ticker": ticker,
                    "name": item.get("name")
                    or (raw.get("overview") or {}).get("name")
                    or ticker,
                    "sector": item.get("sector")
                    or (raw.get("overview") or {}).get("sector"),
                    "exchange": item.get("exchange")
                    or (raw.get("overview") or {}).get("exchange"),
                    "notes": item.get("notes"),
                    "criteria": item.get("criteria"),
                    "current_price_aed": current_price,
                    "dod_pct": self._calculate_return(current_price, p1d),
                    "wow_pct": self._calculate_return(current_price, p1w),
                    "mom_pct": self._calculate_return(current_price, p1m),
                    "3m_pct": self._calculate_return(current_price, p3m),
                    "6m_pct": self._calculate_return(current_price, p6m),
                    "yoy_pct": self._calculate_return(current_price, p1y),
                    "next_div_date": next_div_date,
                    "div_yield": div_yield,
                }
            )

        df = pd.DataFrame(rows)
        return df

    def detail(self, ticker: str, timeframe: str = "1m") -> dict:
        """
        Retrieves detailed chart and fundamental data for a single watchlist item.
        """
        # Fundamentals and chart logic will go here.
        # This replaces the need to call into HoldingsModule.
        pass
