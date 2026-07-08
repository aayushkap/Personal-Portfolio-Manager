# app/services/overlays.py

from __future__ import annotations

from typing import Callable

import pandas as pd

from app.core.logger import get_logger
from app.config import BENCHMARKS
from app.services.base import BaseModule
from app.services.filters import PortfolioFilters
from app.utils.time_utils import DUBAI_TZ

logger = get_logger()

OVERLAY_CATALOGUE: dict[str, str] = {
    "SMA": "Simple Moving Average (portfolio value)",
    "PORTFOLIO_VALUE": "Total Portfolio Market Value",
    "TWR": "Time-Weighted Return (%)",
    "DART": "Dividend-Adjusted Return Trajectory (AED)",
    "COMPOUND_4": "Expected Growth at 4% Annual (Compounded Daily)",
    "COMPOUND_8": "Expected Growth at 8% Annual (Compounded Daily)",
    **{k: v["label"] for k, v in BENCHMARKS.items()},
}


def _idx_to_dubai(index: pd.DatetimeIndex) -> pd.DatetimeIndex:
    """Normalize any DatetimeIndex to tz-aware Asia/Dubai."""
    if index.tz is None:
        return index.tz_localize(DUBAI_TZ)
    return index.tz_convert(DUBAI_TZ)


def _series_ts_to_dubai(s: pd.Series) -> pd.Series:
    """Normalize a datetime Series to tz-aware Asia/Dubai."""
    if s.dt.tz is None:
        return s.dt.tz_localize(DUBAI_TZ).dt.normalize()
    return s.dt.tz_convert(DUBAI_TZ).dt.normalize()


def _to_dubai_ts(value) -> pd.Timestamp:
    """Normalize any scalar date/datetime/Timestamp (naive or tz-aware,
    any timezone) into a tz-aware Asia/Dubai Timestamp."""
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize(DUBAI_TZ)
    return ts.tz_convert(DUBAI_TZ)


class OverlayResolver:
    def __init__(self, base: BaseModule) -> None:
        self._base = base
        self._map: dict[str, Callable[[PortfolioFilters], pd.Series]] = {
            "SMA": self._sma,
            "PORTFOLIO_VALUE": self._portfolio_value,
            "TWR": self._twr,
            "DART": self._dart,
            "COMPOUND_4": self._compound_4,
            "COMPOUND_8": self._compound_8,
        }
        for ticker_key in BENCHMARKS:
            self._map[ticker_key] = self._make_benchmark_resolver(ticker_key)

    def resolve(self, key: str, filters: PortfolioFilters) -> pd.Series:
        fn = self._map.get(key.upper())
        if fn is None:
            logger.warning("Unknown overlay key: %s", key)
            return pd.Series(dtype=float, name=key)
        try:
            return fn(filters)
        except Exception as exc:
            logger.error("Overlay %s failed: %s", key, exc)
            return pd.Series(dtype=float, name=key)

    def resolve_many(
        self, keys: list[str], filters: PortfolioFilters
    ) -> dict[str, list[dict]]:
        return {key: _to_records(self.resolve(key, filters)) for key in keys}

    @staticmethod
    def catalogue() -> list[dict]:
        return [k for k in OVERLAY_CATALOGUE]

    # Helpers

    def _portfolio_value_series(self, filters: PortfolioFilters) -> pd.Series:
        """
        Returns market_value_aed from portfolio().value() as a tz-aware Series.
        This is the single source of truth for portfolio market value — replaces
        get_portfolio_price_series() which was the source of the zero artifacts.
        """
        p = self._base.hql.portfolio()
        df = p.value(
            start_date=filters.date_range.start,
            end_date=filters.date_range.end,
        )
        if df.empty:
            return pd.Series(dtype=float)

        s = df["market_value_aed"]
        s.index = _idx_to_dubai(s.index)
        return s

    def _transactions(self, filters: PortfolioFilters) -> pd.DataFrame:
        """
        Returns filtered transactions from HQL.
        Replaces get_all_transactions() + apply_filters().
        """
        p = self._base.hql.portfolio()
        tx = p.transactions()
        if tx.empty:
            return tx

        tx = tx.copy()
        tx["date_parsed"] = pd.to_datetime(tx["date"], errors="coerce").dt.date

        # start = filters.date_range.start
        end = filters.date_range.end

        # Always include transactions before the window for correct cost basis
        # (TWR and compound need inception-to-date transactions)
        mask = tx["date_parsed"] <= end
        if filters.tickers:
            mask &= tx["ticker"].isin(filters.tickers)
        if hasattr(filters, "sectors") and filters.sectors:
            mask &= tx["sector"].isin(filters.sectors)

        return tx[mask].copy()

    def _price_matrix(self, tickers: list[str], start, end) -> pd.DataFrame:
        frames = []
        for ticker in tickers:
            try:
                t = self._base.hql.ticker(ticker)
                result = t.prices(start=start, end=end)
                if result is None or (hasattr(result, "empty") and result.empty):
                    continue
                # prices() returns a DataFrame from get_ohlcv — extract close
                if isinstance(result, pd.DataFrame):
                    if "close" not in result.columns:
                        continue
                    s = result["close"].rename(ticker)
                else:
                    # genuine Series path
                    s = result.rename(ticker)
                frames.append(s)
            except Exception as e:
                logger.error("Failed to fetch prices for %s: %s", ticker, e)
                continue

        if not frames:
            return pd.DataFrame()

        prices = pd.concat(frames, axis=1)
        prices.index = _idx_to_dubai(prices.index).normalize()
        prices = prices.sort_index()
        prices = prices.groupby(level=0).last()
        last_real_date = prices.dropna(how="all").index.max()
        if pd.isna(last_real_date):
            return pd.DataFrame()

        calendar = pd.date_range(
            start=prices.index.min(),
            end=min(_to_dubai_ts(end).normalize(), last_real_date),
            freq="D",
        )

        prices = prices.reindex(calendar).ffill().dropna(how="all")
        return prices

    # Overlay implementations
    def _make_benchmark_resolver(
        self, ticker: str
    ) -> Callable[[PortfolioFilters], pd.Series]:
        def _resolve(filters: PortfolioFilters) -> pd.Series:
            try:
                df = self._base.get_prices(
                    ticker,
                    filters.date_range.start,
                    filters.date_range.end,
                )
                if df is None or df.empty:
                    return pd.Series(dtype=float, name=ticker)

                # get_prices returns a single-column DataFrame named `ticker`
                s = df[ticker]
                s.index = _idx_to_dubai(s.index)

                calendar = pd.date_range(
                    start=_to_dubai_ts(filters.date_range.start),
                    end=_to_dubai_ts(filters.date_range.end),
                    freq="D",
                )
                s = s.reindex(calendar).ffill()
                return s
            except Exception as e:
                logger.error("Benchmark resolver failed for %s: %s", ticker, e)
                return pd.Series(dtype=float, name=ticker)

        return _resolve

    def _portfolio_value(self, filters: PortfolioFilters) -> pd.Series:
        return self._portfolio_value_series(filters).rename("PORTFOLIO_VALUE")

    def _sma(self, filters: PortfolioFilters) -> pd.Series:
        portfolio = self._portfolio_value_series(filters)
        if portfolio.empty:
            return pd.Series(dtype=float, name="SMA")

        days = (filters.date_range.end - filters.date_range.start).days
        window = max(5, days // 10)

        return portfolio.rolling(window=window, min_periods=window).mean().rename("SMA")

    def _twr(self, filters: PortfolioFilters) -> pd.Series:
        tx = self._transactions(filters)
        if tx.empty:
            logger.warning("TWR: no transactions found for given filters")
            return pd.Series(dtype=float, name="TWR")

        first_tx_date = tx["date_parsed"].min()
        tickers = tx["ticker"].unique().tolist()

        prices = self._price_matrix(tickers, first_tx_date, filters.date_range.end)
        if prices.empty:
            logger.warning("TWR: price matrix empty for tickers=%s", tickers)
            return pd.Series(dtype=float, name="TWR")

        trading_days = prices.index

        tx_copy = tx.copy()
        sign = tx_copy["transaction"].str.lower().map({"buy": 1, "sell": -1}).fillna(0)
        tx_copy["net_shares"] = tx_copy["shares"].fillna(0) * sign
        tx_copy["ts"] = _series_ts_to_dubai(
            pd.to_datetime(tx_copy["date"], errors="coerce")
        )

        # Snap each transaction to the NEXT available trading day instead of
        # dropping it on reindex. This fixes weekend/holiday transactions
        # silently vanishing from the share count.
        tx_copy["ts_snapped"] = tx_copy["ts"].apply(
            lambda t: (
                trading_days[trading_days >= t].min()
                if (trading_days >= t).any()
                else trading_days.max()
            )
        )

        daily_changes = (
            tx_copy.groupby(["ts_snapped", "ticker"])["net_shares"]
            .sum()
            .unstack(fill_value=0)
        )
        daily_shares = daily_changes.reindex(trading_days, fill_value=0).cumsum()

        common = daily_shares.columns.intersection(prices.columns)
        if common.empty:
            logger.error(
                "TWR: no overlap between transaction tickers %s and price tickers %s",
                list(daily_shares.columns),
                list(prices.columns),
            )
            return pd.Series(dtype=float, name="TWR")

        portfolio_mv = (daily_shares[common] * prices[common]).sum(axis=1, min_count=1)
        portfolio_mv = portfolio_mv.fillna(0.0)

        if (portfolio_mv == 0).all():
            logger.error(
                "TWR: portfolio_mv is all zeros — check ticker key format mismatch"
            )
            return pd.Series(dtype=float, name="TWR")

        tx_copy["flow"] = tx_copy["total_cost_aed"].fillna(0) * sign
        cash_flows = (
            tx_copy.groupby("ts_snapped")["flow"]
            .sum()
            .reindex(trading_days, fill_value=0.0)
        )

        twr_factors = pd.Series(1.0, index=trading_days)
        prev_value = portfolio_mv.iloc[0]

        for i in range(1, len(trading_days)):
            end_val = portfolio_mv.iloc[i]
            cf = cash_flows.iloc[i]

            if prev_value > 0:
                twr_factors.iloc[i] = (end_val - cf) / prev_value
            elif end_val > 0 and cf > 0:
                # First funded day: adding capital starts the track record,
                # it is not itself performance.
                twr_factors.iloc[i] = 1.0
            else:
                twr_factors.iloc[i] = 1.0

            prev_value = end_val

        wealth_index = twr_factors.cumprod()

        window_start = _to_dubai_ts(filters.date_range.start)
        window_end = _to_dubai_ts(filters.date_range.end)
        window_mask = trading_days >= window_start

        if not window_mask.any():
            logger.warning(
                "TWR: window_start=%s is after last trading day=%s",
                window_start,
                trading_days.max(),
            )
            return pd.Series(dtype=float, name="TWR")

        window_start_val = portfolio_mv[window_mask].iloc[0]
        twr_window = wealth_index[window_mask] / wealth_index[window_mask].iloc[0]
        twr_aed = window_start_val * twr_window

        calendar_days = pd.date_range(start=twr_aed.index[0], end=window_end, freq="D")
        twr_aed = twr_aed.reindex(calendar_days).ffill()

        return twr_aed.rename("TWR")

    def _dart(self, filters: PortfolioFilters) -> pd.Series:
        twr = self._twr(filters)
        if twr.empty:
            return pd.Series(dtype=float, name="DART")

        # Use portfolio().dividends() — already has total_aed + pay_date resolved
        p = self._base.hql.portfolio()
        divs_df = p.dividends()

        if divs_df.empty:
            return twr.rename("DART")

        received = divs_df[divs_df["status"] == "received"].copy()
        if received.empty:
            return twr.rename("DART")

        trading_days = twr.index  # tz-aware Asia/Dubai

        received["ts"] = _series_ts_to_dubai(
            pd.to_datetime(received["pay_date"], errors="coerce")
        )
        daily_divs = (
            received.groupby("ts")["total_aed"]
            .sum()
            .reindex(trading_days, fill_value=0.0)
        )

        dart = (twr + daily_divs.cumsum()).combine_first(twr)
        return dart.rename("DART")

    def _compound_4(self, filters: PortfolioFilters) -> pd.Series:
        return self._compound_at_rate(filters, annual_rate=0.04, name="COMPOUND_4")

    def _compound_8(self, filters: PortfolioFilters) -> pd.Series:
        return self._compound_at_rate(filters, annual_rate=0.08, name="COMPOUND_8")

    def _compound_at_rate(
        self, filters: PortfolioFilters, annual_rate: float, name: str
    ) -> pd.Series:
        tx = self._transactions(filters)
        if tx.empty:
            return pd.Series(dtype=float, name=name)

        tx_copy = tx.copy()
        tx_copy["ts"] = _series_ts_to_dubai(
            pd.to_datetime(tx_copy["date"], errors="coerce")
        )

        # Buys add capital, sells return capital — net per day
        tx_copy["signed_cost"] = tx_copy.apply(
            lambda r: (
                r["total_cost_aed"]
                if str(r["transaction"]).lower() == "buy"
                else -r["total_cost_aed"]
            ),
            axis=1,
        )
        deposits = tx_copy.groupby("ts")["signed_cost"].sum()
        # Drop any days where net is zero or negative (full sell-off of same-day buy)
        deposits = deposits[deposits != 0]

        if deposits.empty:
            return pd.Series(dtype=float, name=name)

        calendar_days = pd.date_range(
            start=_to_dubai_ts(filters.date_range.start),
            end=_to_dubai_ts(filters.date_range.end),
            freq="D",
        )

        daily_rate = annual_rate / 365.0
        result = pd.Series(0.0, index=calendar_days, name=name)

        for day in calendar_days:
            total = 0.0
            for deposit_date, amount in deposits.items():
                if deposit_date <= day:
                    days_held = (day - deposit_date).days
                    total += amount * ((1 + daily_rate) ** days_held)
            result[day] = total

        return result


# Utilities
def _to_records(s: pd.Series) -> list[dict]:
    return [{"date": str(idx.date()), "value": _safe(v)} for idx, v in s.items()]


def _safe(v: float) -> float | None:
    import math

    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return round(v, 4)
