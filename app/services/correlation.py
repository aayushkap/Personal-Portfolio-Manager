# app/services/correlation.py

from __future__ import annotations

from datetime import date, timedelta
from typing import Literal

import numpy as np
import pandas as pd

from app.core.logger import get_logger
from app.services.base import BaseModule
import math

logger = get_logger()

Period = Literal["1m", "3m", "6m", "1y", "3y", "5y"]

_LOOKBACK_DAYS: dict[Period, int] = {
    "1m": 30,
    "3m": 90,
    "6m": 180,
    "1y": 365,
    "3y": 3 * 365,
    "5y": 5 * 365,
}

_MIN_OBSERVATIONS = 20  # minimum overlapping data points to compute a valid correlation


def _safe(v):
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v


class CorrelationModule(BaseModule):
    """
    Pearson correlation matrix on log returns for a set of instruments.

    get_matrix(tickers, period) : {
        period:    "1y",
        start:     "2025-04-07",
        end:       "2026-04-07",
        tickers:   [...],
        matrix:    [ { ticker_a, ticker_b, correlation, observations } ],
        coverage:  { ticker: observation_count }
    }
    """

    _PORTFOLIO_KEY = "PORTFOLIO"

    def get_matrix(self, tickers: list[str], period: Period = "1y") -> dict:
        if len(tickers) < 2:
            return {"error": "At least 2 instruments required."}

        end = date.today()
        start = end - timedelta(days=_LOOKBACK_DAYS[period])

        real_tickers = [t for t in tickers if t != self._PORTFOLIO_KEY]

        # Fetch instrument prices
        prices = (
            self.get_price_series(real_tickers, start, end)
            if real_tickers
            else pd.DataFrame()
        )

        if prices.empty:
            return {"error": "No price data found."}

        prices = prices.dropna(axis=1, how="all")
        found = prices.columns.tolist()
        missing = [t for t in tickers if t not in found]

        if missing:
            logger.warning("No data for: %s", missing)
        if len(found) < 2:
            return {"error": f"Insufficient data. Missing: {missing}"}

        log_returns = np.log(prices / prices.shift(1)).dropna(how="all")
        ticker_trend = {t: self._trend(log_returns[t].dropna()) for t in found}

        matrix = []
        for i, a in enumerate(found):
            for b in found[i + 1 :]:
                pair = log_returns[[a, b]].dropna()
                n = len(pair)
                corr = (
                    round(float(pair[a].corr(pair[b])), 4)
                    if n >= _MIN_OBSERVATIONS
                    else None
                )
                ta, tb = ticker_trend[a], ticker_trend[b]

                matrix.append(
                    {
                        "ticker_a": a,
                        "ticker_b": b,
                        "correlation": _safe(corr),
                        "observations": n,
                        "strength": _label(corr),
                        "ticker_a_return": _safe(ta["return_pct"]),
                        "ticker_b_return": _safe(tb["return_pct"]),
                        "direction_a": ta["direction"],
                        "direction_b": tb["direction"],
                        "co_movement": _co_movement(
                            corr, ta["direction"], tb["direction"]
                        ),
                    }
                )

        coverage = {t: int(log_returns[t].notna().sum()) for t in found}

        return {
            "period": period,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "tickers": found,
            "missing": missing,
            "matrix": matrix,
            "coverage": coverage,
        }

    @staticmethod
    def _trend(s: pd.Series) -> dict:
        """Cumulative price return over the period from log returns."""
        pct = round((float(np.exp(s.sum())) - 1) * 100, 2)
        return {"return_pct": pct, "direction": "up" if pct >= 0 else "down"}


def _co_movement(corr: float | None, dir_a: str, dir_b: str) -> str:
    """
    Combines correlation sign with direction to give a plain-English label.
    e.g. strongly correlated + both up = "rising_together"
    """
    if corr is None:
        return "insufficient_data"
    same_dir = dir_a == dir_b
    if abs(corr) < 0.2:
        return "independent"
    if corr > 0 and same_dir:
        return "rising_together" if dir_a == "up" else "falling_together"
    if corr > 0 and not same_dir:
        return "correlated_but_diverging"
    if corr < 0 and not same_dir:
        return "inverse_moving"
    return "mixed"


def _label(corr: float | None) -> str:
    if corr is None:
        return "insufficient_data"
    a = abs(corr)
    if a >= 0.8:
        return "very_strong"
    if a >= 0.6:
        return "strong"
    if a >= 0.4:
        return "moderate"
    if a >= 0.2:
        return "weak"
    return "negligible"
