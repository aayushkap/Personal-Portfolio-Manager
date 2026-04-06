# app/services/base.py

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd

from app.core.logger import get_logger
from app.data.cache import Cache
from app.data.db import DB
from app.data.schemas import DividendRow, PurchaseDetail, TickerCache
from app.services.filters import PortfolioFilters

logger = get_logger()


class BaseModule:
    """
    Data access foundation for all service modules.

    Provides straightforward methods to read from cache and DB.
    All other modules inherit from this and call these methods directly.

    Cache  - ticker fundamentals, dividends, purchase details (JSON files)
    DB     - OHLC price bars (SQLite / timeseries store)
    """

    def __init__(self, cache: Cache, db: DB) -> None:
        self._cache = cache
        self._db = db

    def get_ticker(self, ticker: str) -> Optional[TickerCache]:
        """Load and validate a ticker's full cache entry. Returns None if not found."""
        raw = self._cache.load(ticker)
        if not raw:
            logger.debug("Cache miss: %s", ticker)
            return None
        return TickerCache.model_validate(raw)

    def get_all_tickers(self) -> list[str]:
        """Return every ticker key present in the cache directory."""
        return [
            path.stem.replace("_", ":", 1).upper()
            for path in Path(self._cache.cache_dir).glob("*.json")
        ]

    def get_dividends(self, ticker: str) -> list[DividendRow]:
        """Return parsed dividend rows for a single ticker."""
        data = self.get_ticker(ticker)
        if not data or not data.dividends:
            return []
        return data.dividends.rows

    def get_transactions(self, ticker: str) -> list[PurchaseDetail]:
        """Return typed purchase details for a single ticker."""
        data = self.get_ticker(ticker)
        if not data:
            return []
        return data.purchase_details

    def get_all_transactions(self) -> pd.DataFrame:
        """
        Collect every purchase_detail across all cached tickers into one DataFrame.

        Columns: ticker, action, trade_date, shares, price, commission,
                 total_cost, signed_shares, platform, sector, exchange, logo_url
        """
        rows = []

        for ticker_key in self.get_all_tickers():
            for detail in self.get_transactions(ticker_key):
                rows.append(
                    {
                        "ticker": detail.ticker,
                        "action": detail.transaction.upper(),
                        "trade_date": detail.purchase_date,
                        "shares": detail.shares,
                        "price": self._strip(detail.cost_per_share),
                        "commission": self._strip(detail.commission_paid),
                        "total_cost": self._strip(detail.total_cost),
                        "platform": detail.platform,
                        "sector": detail.sector,
                        "exchange": detail.exchange,
                        "logo_url": detail.logo_url,
                    }
                )

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows).sort_values("trade_date").reset_index(drop=True)
        df["signed_shares"] = df.apply(
            lambda r: r["shares"] if r["action"] == "BUY" else -r["shares"],
            axis=1,
        )
        return df

    def get_prices(self, ticker: str, start: date, end: date) -> pd.DataFrame:
        rows = self._db.get(ticker, limit=10_000)
        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        df["date"] = (
            pd.to_datetime(df["timestamp"]).dt.tz_convert("Asia/Dubai").dt.normalize()
        )
        df = df[(df["date"].dt.date >= start) & (df["date"].dt.date <= end)]

        if df.empty:
            return pd.DataFrame()

        return df.groupby("date")["close"].last().rename(ticker).to_frame()

    def get_price_series(
        self, tickers: list[str], start: date, end: date
    ) -> pd.DataFrame:
        """
        Close prices for multiple tickers, pivoted into one DataFrame.

        index   = date (trading days only)
        columns = ticker symbols
        values  = close price (NaN where no data)
        """
        frames = []
        for ticker in tickers:
            df = self.get_prices(ticker, start, end)
            if not df.empty:
                frames.append(df[ticker])

        if not frames:
            return pd.DataFrame()

        return pd.concat(frames, axis=1).sort_index()

    def get_latest_price(self, ticker: str) -> Optional[float]:
        """Most recent close price for a ticker. Returns None if unavailable."""
        row = self._db.get_latest(ticker)
        return row["close"] if row else None

    def get_latest_prices(self, tickers: list[str]) -> dict[str, float]:
        """Latest close price for each ticker in the list."""
        return {t: p for t in tickers if (p := self.get_latest_price(t)) is not None}

    def get_holdings(
        self,
        as_of: date,
        tickers: Optional[list[str]] = None,
        transactions: Optional[pd.DataFrame] = None,
    ) -> dict[str, float]:
        """
        Shares held per ticker as of a given date.
        Only returns tickers where shares > 0 (fully sold positions are excluded).
        Pass `transactions` if you already have the DataFrame to avoid a re-read.
        """
        tx = transactions if transactions is not None else self.get_all_transactions()
        if tx.empty:
            return {}

        mask = pd.to_datetime(tx["trade_date"]).dt.date <= as_of
        if tickers:
            mask &= tx["ticker"].isin(tickers)

        held = tx[mask].groupby("ticker")["signed_shares"].sum()
        return held[held > 0].to_dict()

    def apply_filters(
        self, tx: pd.DataFrame, filters: PortfolioFilters
    ) -> pd.DataFrame:
        """Filter a transactions DataFrame by sector, exchange, and/or ticker list."""
        if tx.empty:
            return tx
        if filters.sectors:
            tx = tx[tx["sector"].isin(filters.sectors)]
        if filters.exchanges:
            tx = tx[tx["exchange"].isin(filters.exchanges)]
        if filters.tickers:
            tx = tx[tx["ticker"].isin(filters.tickers)]
        return tx.copy()

    @staticmethod
    def _strip(value: Optional[str]) -> float:
        """Parse 'AED 1,234.56' or '1234.56' into a float."""
        if not value:
            return 0.0
        import re

        cleaned = re.sub(r"[^\d.]", "", str(value))
        return float(cleaned) if cleaned else 0.0
