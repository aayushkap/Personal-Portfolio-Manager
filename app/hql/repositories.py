# app/hql/repositories.py

from __future__ import annotations

from datetime import date
import pandas as pd

from app.data.cache import Cache
from app.data.db import DB
from app.data.fx import load_fx_rates
from app.hql.errors import HQLTickerNotFound
from app.hql.parsers import parse_money_string


class FXService:
    def __init__(self) -> None:
        self._rates = load_fx_rates()

    def to_aed(self, value: float | None, currency: str | None) -> float | None:
        if value is None:
            return None
        return value * self._rates.get(currency or "AED", 1.0)


class CacheRepository:
    def __init__(self, cache: Cache) -> None:
        self._cache = cache

    def get_raw_ticker(self, ticker: str) -> dict:
        data = self._cache.load(ticker)
        if not data:
            raise HQLTickerNotFound(ticker)
        return data

    def list_tickers(self) -> list[str]:
        from pathlib import Path

        return [
            path.stem.replace("_", ":", 1).upper()
            for path in Path(self._cache.cache_dir).glob("*.json")
        ]

    def resolve_currency(self, raw: dict) -> str:
        """
        Infer ticker currency from purchase_details cost strings.
        """
        details = raw.get("purchase_details") or []
        for detail in details:
            _, currency = parse_money_string(detail.get("cost_per_share") or "")
            if currency:
                return currency
            _, currency = parse_money_string(detail.get("total_cost") or "")
            if currency:
                return currency

        return "AED"


class PriceRepository:
    def __init__(self, db: DB, fx: FXService, cache_repo: CacheRepository) -> None:
        self._db = db
        self._fx = fx
        self._cache_repo = cache_repo

    def get_ohlcv(
        self,
        ticker: str,
        start: date,
        end: date,
        granularity: str = "1D",
    ) -> pd.DataFrame:
        rows = self._db.get(ticker, limit=50_000)
        expected = ["open", "high", "low", "close", "volume"]

        if not rows:
            return pd.DataFrame(columns=expected)

        df = pd.DataFrame(rows).copy()
        if df.empty or "timestamp" not in df.columns or "close" not in df.columns:
            return pd.DataFrame(columns=expected)

        if "volume" not in df.columns:
            df["volume"] = 0

        df["ts"] = pd.to_datetime(df["timestamp"]).dt.tz_convert("Asia/Dubai")

        start_ts = pd.Timestamp(start).tz_localize("Asia/Dubai")
        end_ts = (
            pd.Timestamp(end).tz_localize("Asia/Dubai")
            + pd.Timedelta(days=1)
            - pd.Timedelta(seconds=1)
        )

        df = df[(df["ts"] >= start_ts) & (df["ts"] <= end_ts)].copy()
        if df.empty:
            return pd.DataFrame(columns=expected)

        raw = self._cache_repo.get_raw_ticker(ticker)
        currency = self._cache_repo.resolve_currency(raw)

        df["close"] = pd.to_numeric(df["close"], errors="coerce").map(
            lambda v: self._fx.to_aed(v, currency) if pd.notna(v) else pd.NA
        )
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0)

        df = df.sort_values("ts").set_index("ts")

        out = (
            df.resample(granularity)
            .agg(
                open=("close", "first"),
                high=("close", "max"),
                low=("close", "min"),
                close=("close", "last"),
                volume=("volume", "sum"),
            )
            .dropna(subset=["close"])
        )

        return out.sort_index()

    def get_close_series(
        self,
        ticker: str,
        start: date,
        end: date,
        granularity: str = "1D",
    ) -> pd.Series:
        df = self.get_ohlcv(ticker, start, end, granularity=granularity)
        if df.empty:
            return pd.Series(dtype=float, name=ticker)
        s = df["close"].copy()
        s.name = ticker
        return s

    def get_multi_close_series(
        self,
        tickers: list[str],
        start: date,
        end: date,
        granularity: str = "1D",
    ) -> pd.DataFrame:
        frames = []
        for ticker in tickers:
            s = self.get_close_series(ticker, start, end, granularity=granularity)
            if not s.empty:
                frames.append(s)
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, axis=1).sort_index()
