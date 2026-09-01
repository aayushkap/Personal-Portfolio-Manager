# app/services/technicals.py

from __future__ import annotations

import re

import pandas as pd

TECHNICAL_CATALOGUE: dict[str, str] = {
    "SMA_20": "Simple Moving Average (20)",
    "SMA_50": "Simple Moving Average (50)",
    "SMA_200": "Simple Moving Average (200)",
    "EMA_20": "Exponential Moving Average (20)",
    "EMA_50": "Exponential Moving Average (50)",
    "EMA_200": "Exponential Moving Average (200)",
}

_KEY_PATTERN = re.compile(r"^(SMA|EMA)_(\d+)$")


def is_technical_key(key: str) -> bool:
    return bool(_KEY_PATTERN.match(key.upper()))


def resolve_technical(key: str, close: pd.Series) -> pd.Series:
    """
    Compute a technical indicator series from a ticker's own close prices.

    The window is N bars of whatever granularity `close` is already sampled
    at (15min/30min/60min/1D) — matching how TradingView/ThinkorSwim/
    MetaTrader recompute indicators over the bars of the currently displayed
    timeframe, not a fixed calendar window. If there aren't enough bars to
    seed the indicator, an empty series is returned so the caller omits it
    rather than plotting an artificially short, over-reactive window.
    """
    match = _KEY_PATTERN.match(key.upper())
    if not match or close.empty:
        return pd.Series(dtype=float, name=key)

    kind, window_str = match.groups()
    window = int(window_str)

    close = close.dropna()
    if len(close) < window:
        return pd.Series(dtype=float, name=key)

    if kind == "SMA":
        series = close.rolling(window=window, min_periods=window).mean()
    else:
        series = _ema(close, window)

    return series.dropna().rename(key)


def _ema(close: pd.Series, window: int) -> pd.Series:
    """
    Standard recursive EMA: seed with the SMA of the first `window` bars,
    then recurse with alpha = 2 / (window + 1).

    pandas' `close.ewm(span=window, adjust=False).mean()` instead weights
    from the series' very first bar (EMA[0] = close[0]) and never applies an
    SMA seed, which diverges from every major charting platform near the
    seed period — the divergence that matters most for EMA_200, where the
    seed window is a large fraction of the visible history.
    """
    seed = close.iloc[:window].mean()
    seed_index = close.index[window - 1]
    seeded_input = pd.concat(
        [pd.Series([seed], index=[seed_index]), close.iloc[window:]]
    )
    return seeded_input.ewm(span=window, adjust=False).mean()
