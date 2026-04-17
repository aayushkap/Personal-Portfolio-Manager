# app/services/overlays.py

from __future__ import annotations

from typing import Callable

import pandas as pd

from app.core.logger import get_logger
from app.config import BENCHMARKS
from app.services.base import BaseModule
from app.services.filters import DateRange, PortfolioFilters
from app.utils.fin import parse_money

logger = get_logger()


# Overlay metadata — what the API returns in /metadata
OVERLAY_CATALOGUE: dict[str, str] = {
    "SMA": "Simple Moving Average (portfolio value)",
    "PORTFOLIO_VALUE": "Total Portfolio Market Value",
    "TWR": "Time-Weighted Return (%)",
    "DART": "Dividend-Adjusted Return Trajectory (AED)",
    "COMPOUND_4": "Expected Growth at 4% Annual (Compounded Daily)",
    "COMPOUND_8": "Expected Growth at 8% Annual (Compounded Daily)",
    **{k: v["label"] for k, v in BENCHMARKS.items()},
}


class OverlayResolver:
    """
    Resolves an overlay key + filters into a dated pd.Series.

    Usage:
        resolver = OverlayResolver(base_module)
        series   = resolver.resolve("SMA", filters)

    Returns pd.Series indexed by tz-aware Asia/Dubai dates.
    Returns empty Series if the key is unknown or data is unavailable.
    """

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

        for ticker_key, meta in BENCHMARKS.items():
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

    def _make_benchmark_resolver(
        self, ticker: str
    ) -> Callable[[PortfolioFilters], pd.Series]:
        """Returns a resolver function for a benchmark ticker."""

        def _resolve(filters: PortfolioFilters) -> pd.Series:
            prices = self._base.get_prices(
                ticker,
                filters.date_range.start,
                filters.date_range.end,
            )
            if prices.empty:
                return pd.Series(dtype=float, name=ticker)
            # get_prices returns a DataFrame with one column named `ticker`
            return prices[ticker].rename(ticker)

        return _resolve

    def resolve_many(
        self,
        keys: list[str],
        filters: PortfolioFilters,
    ) -> dict[str, list[dict]]:
        """Resolve a list of overlay keys → {key: [{date, value}, ...]}"""
        return {key: _to_records(self.resolve(key, filters)) for key in keys}

    @staticmethod
    def catalogue() -> list[dict]:
        return [k for k, v in OVERLAY_CATALOGUE.items()]

    # Implementations
    def _portfolio_value(self, filters: PortfolioFilters) -> pd.Series:
        """Total market value of all held positions per trading day."""
        return self._base.get_portfolio_price_series(filters).rename("PORTFOLIO_VALUE")

    def _sma(self, filters: PortfolioFilters) -> pd.Series:
        """SMA of portfolio market value. Window auto-scaled to ~10% of period."""
        portfolio = self._base.get_portfolio_price_series(filters)
        if portfolio.empty:
            return pd.Series(dtype=float, name="SMA")

        days = (filters.date_range.end - filters.date_range.start).days
        window = max(5, days // 10)

        return portfolio.rolling(window=window, min_periods=window).mean().rename("SMA")

    def _twr(self, filters: PortfolioFilters) -> pd.Series:
        tx = self._base.apply_filters(self._base.get_all_transactions(), filters)
        if tx.empty:
            return pd.Series(dtype=float, name="TWR")

        # Always compute from first transaction — cash flows before the window matter
        first_tx_date = pd.to_datetime(tx["trade_date"]).dt.date.min()
        inception_filters = PortfolioFilters(
            date_range=DateRange(start=first_tx_date, end=filters.date_range.end),
            tickers=filters.tickers,
            sectors=filters.sectors,
        )

        tickers = tx["ticker"].unique().tolist()
        prices = self._base.get_price_series(
            tickers,
            inception_filters.date_range.start,
            inception_filters.date_range.end,
        )
        if prices.empty:
            return pd.Series(dtype=float, name="TWR")

        trading_days = prices.index
        holdings = self._base._holdings_matrix(tx, trading_days)
        common = holdings.columns.intersection(prices.columns)
        portfolio_mv = (holdings[common] * prices[common]).sum(axis=1)

        tx_copy = tx.copy()
        tx_copy["ts"] = (
            pd.to_datetime(tx_copy["trade_date"])
            .dt.tz_localize("Asia/Dubai")
            .dt.normalize()
        )
        tx_copy["flow"] = tx_copy.apply(
            lambda r: r["total_cost"] if r["action"] == "BUY" else -r["total_cost"],
            axis=1,
        )
        cash_flows = (
            tx_copy.groupby("ts")["flow"].sum().reindex(trading_days, fill_value=0.0)
        )

        twr_factors = pd.Series(1.0, index=trading_days)
        prev_value = portfolio_mv.iloc[0]

        for i in range(1, len(trading_days)):
            cf = cash_flows.iloc[i]
            end_val = portfolio_mv.iloc[i]
            base = prev_value + cf
            twr_factors.iloc[i] = end_val / base if base != 0 else 1.0
            prev_value = end_val

        twr_cumulative = (twr_factors.cumprod() - 1) * 100

        # Slice to the requested window
        window_mask = trading_days >= pd.Timestamp(
            filters.date_range.start, tz="Asia/Dubai"
        )
        twr_window = twr_cumulative[window_mask]

        # Rebase to 0 at window start, anchor to portfolio value at that point
        twr_window = twr_window - twr_window.iloc[0]
        window_start_val = portfolio_mv[window_mask].iloc[0]
        twr_aed = window_start_val * (1 + twr_window / 100)

        return twr_aed.rename("TWR")

    def _dart(self, filters: PortfolioFilters) -> pd.Series:
        twr = self._twr(filters)
        if twr.empty:
            return pd.Series(dtype=float, name="DART")

        tx = self._base.apply_filters(self._base.get_all_transactions(), filters).copy()
        if tx.empty:
            return twr.rename("DART")

        tickers = tx["ticker"].unique().tolist()
        trading_days = twr.index  # tz-aware Asia/Dubai DatetimeIndex

        # Build a daily dividend series over the TWR window
        div_rows = []
        for ticker_key in tickers:
            for div in self._base.get_dividends(ticker_key):
                if not div.pay_date or not div.cash_amount:
                    continue
                # Shares held on ex_date determines how much you received
                shares = self._base.get_holdings(div.ex_date, [ticker_key], tx).get(
                    ticker_key, 0.0
                )
                if shares <= 0:
                    continue
                amount, currency = parse_money(div.cash_amount)
                aed_amount = amount * self._base.fx.get(currency, 1.0) * shares
                div_rows.append({"pay_date": div.pay_date, "amount": aed_amount})

        if not div_rows:
            return twr.rename("DART")

        div_df = pd.DataFrame(div_rows)
        div_df["ts"] = (
            pd.to_datetime(div_df["pay_date"])
            .dt.tz_localize("Asia/Dubai")
            .dt.normalize()
        )

        daily_divs = (
            div_df.groupby("ts")["amount"].sum().reindex(trading_days, fill_value=0.0)
        )

        dart = (twr + daily_divs.cumsum()).combine_first(twr)
        return dart.rename("DART")

    def _compound_4(self, filters: PortfolioFilters) -> pd.Series:
        """
        Theoretical portfolio value if every deposit compounded at 4% annually.
        Steps up on each actual cash deposit, compounds daily from that deposit's date.
        Useful as a 'lazy money' baseline — am I beating a savings account?
        """
        return self._compound_at_rate(filters, annual_rate=0.04, name="COMPOUND_4")

    def _compound_8(self, filters: PortfolioFilters) -> pd.Series:
        """
        Theoretical portfolio value if every deposit compounded at 8% annually.
        Steps up on each actual cash deposit, compounds daily from that deposit's date.
        Useful as a 'lazy money' baseline — am I beating a savings account?
        """
        return self._compound_at_rate(filters, annual_rate=0.08, name="COMPOUND_8")

    def _compound_at_rate(
        self, filters: PortfolioFilters, annual_rate: float, name: str
    ) -> pd.Series:
        tx = self._base.apply_filters(self._base.get_all_transactions(), filters).copy()
        if tx.empty:
            return pd.Series(dtype=float, name=name)

        # Build the trading day index from actual price data
        tickers = tx["ticker"].unique().tolist()
        prices = self._base.get_price_series(
            tickers, filters.date_range.start, filters.date_range.end
        )
        if prices.empty:
            return pd.Series(dtype=float, name=name)

        trading_days = prices.index  # tz-aware Asia/Dubai DatetimeIndex

        # Only BUY transactions contribute cash inflows
        buys = tx[tx["action"] == "BUY"].copy()
        buys["ts"] = (
            pd.to_datetime(buys["trade_date"])
            .dt.tz_localize("Asia/Dubai")
            .dt.normalize()
        )
        # Aggregate total cost per deposit date (multiple buys same day = one deposit)
        deposits = buys.groupby("ts")["total_cost"].sum()

        # For each trading day, sum the compounded value of every prior deposit
        daily_rate = annual_rate / 365.0
        result = pd.Series(0.0, index=trading_days, name=name)

        for day in trading_days:
            total = 0.0
            for deposit_date, amount in deposits.items():
                if deposit_date <= day:
                    days_held = (day - deposit_date).days
                    total += amount * ((1 + daily_rate) ** days_held)
            result[day] = total

        return result


# Utility
def _to_records(s: pd.Series) -> list[dict]:
    return [{"date": str(idx.date()), "value": _safe(v)} for idx, v in s.items()]


def _safe(v: float) -> float | None:
    import math

    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return round(v, 4)
