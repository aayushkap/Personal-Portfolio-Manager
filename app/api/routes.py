from __future__ import annotations


from fastapi import APIRouter
from app.core.logger import get_logger


import numpy as np
import pandas as pd
from datetime import date, timedelta
from dataclasses import dataclass, field
from app.utils.filters import (
    get_all_transactions,
    get_holdings_on_date,
    get_price_series,
    get_dividends_received,
    get_dividend_events,
)

logger = get_logger()
router = APIRouter()


@dataclass
class OverviewFilters:
    start_date: date = field(default_factory=lambda: date.today() - timedelta(days=365))
    end_date: date = field(default_factory=date.today)
    sectors: list[str] | None = None  # None = all
    exchanges: list[str] | None = None  # None = both DFM + ADX


def _apply_tx_filters(tx: pd.DataFrame, filters: OverviewFilters) -> pd.DataFrame:
    """Filter transactions down to the requested sector/exchange scope."""
    if filters.sectors:
        tx = tx[tx["sector"].isin(filters.sectors)]
    if filters.exchanges:
        tx = tx[tx["exchange"].isin(filters.exchanges)]
    return tx


def get_portfolio_value_series(filters: OverviewFilters) -> pd.DataFrame:
    """
    Core chart data. Returns one row per trading day with:
      date, market_value, total_return_value, total_invested, sma_20, sma_50

    - market_value:       shares × close (no dividends)
    - total_return_value: market_value + cumulative dividends received
    - total_invested:     cumulative net cash deployed (step function)
    """
    tx = get_all_transactions()
    tx = _apply_tx_filters(tx, filters)
    if tx.empty:
        return pd.DataFrame()

    tickers = tx["ticker"].unique().tolist()

    #  Price series (pivoted: date × ticker → close)
    prices = get_price_series(tickers, filters.start_date, filters.end_date)
    if prices.empty:
        return pd.DataFrame()

    trading_days = prices.index  # only actual trading days in range

    #  Market value per day
    mv_rows = []
    for day in trading_days:
        holdings = get_holdings_on_date(day.date(), tickers=tickers, transactions=tx)
        day_value = sum(
            shares * prices.loc[day, ticker]
            for ticker, shares in holdings.items()
            if ticker in prices.columns
        )
        mv_rows.append({"date": day, "market_value": day_value})

    result = pd.DataFrame(mv_rows).set_index("date")

    #  Cumulative dividends received
    divs = get_dividends_received(
        filters.start_date, filters.end_date, tickers=tickers, transactions=tx
    )
    if not divs.empty:
        divs["date"] = pd.to_datetime(divs["pay_date"])
        daily_divs = (
            divs.groupby("date")["total_received_aed"]
            .sum()
            .reindex(trading_days, fill_value=0)
        )
        result["cumulative_dividends"] = daily_divs.cumsum()
    else:
        result["cumulative_dividends"] = 0.0

    result["total_return_value"] = (
        result["market_value"] + result["cumulative_dividends"]
    )

    #  Total invested (step function)
    # Signed: buys add total_cost, sells subtract cost_basis
    tx_in_range = tx[
        (tx["trade_date"] >= filters.start_date)
        & (tx["trade_date"] <= filters.end_date)
    ].copy()
    tx_in_range["cash_flow"] = tx_in_range.apply(
        lambda r: r["total_cost_aed"] if r["action"] == "BUY" else -r["total_cost_aed"],
        axis=1,
    )
    tx_in_range["date"] = pd.to_datetime(tx_in_range["trade_date"])
    daily_cf = (
        tx_in_range.groupby("date")["cash_flow"]
        .sum()
        .reindex(trading_days, fill_value=0)
    )
    # Seed with invested amount already deployed BEFORE the filter window starts
    seed = _get_invested_before(tx, filters.start_date)
    result["total_invested"] = seed + daily_cf.cumsum()

    #  Moving averages on market_value
    result["sma_20"] = result["market_value"].rolling(20, min_periods=1).mean()
    result["sma_50"] = result["market_value"].rolling(50, min_periods=1).mean()

    #  Daily TWR factor
    # Sub-period return, adjusted for cash flows: R_t = (V_t - V_{t-1} - CF_t) / V_{t-1}
    result["prev_mv"] = result["market_value"].shift(1)
    result["cf"] = daily_cf

    denom = result["prev_mv"].replace(0, np.nan)
    result["twr_factor"] = (result["market_value"] - result["cf"]) / denom

    # kill NaN / inf / -inf
    result["twr_factor"] = (
        result["twr_factor"].replace([np.inf, -np.inf], np.nan).fillna(1.0)
    )

    result["twr_cumulative"] = result["twr_factor"].cumprod() - 1
    result["twr_cumulative"] = (
        result["twr_cumulative"].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    )

    return result.reset_index()


def _get_invested_before(tx: pd.DataFrame, cutoff: date) -> float:
    """Sum of all cash flows before the filter window start date."""
    before = tx[tx["trade_date"] < cutoff]
    if before.empty:
        return 0.0
    buys = before[before["action"] == "BUY"]["total_cost_aed"].sum()
    sells = before[before["action"] == "SELL"]["total_cost_aed"].sum()
    return buys - sells


def get_portfolio_kpis(filters: OverviewFilters) -> dict:
    """
    All KPI strip values. Calls get_portfolio_value_series internally
    and derives the KPI numbers from the result + current state.
    """
    series = get_portfolio_value_series(filters)
    if series.empty:
        return {}

    latest = series.iloc[-1]
    prev_day = series.iloc[-2] if len(series) > 1 else latest
    prev_week = series.iloc[-6] if len(series) > 5 else series.iloc[0]
    prev_month = series.iloc[-22] if len(series) > 21 else series.iloc[0]
    start = series.iloc[0]

    current_mv = latest["market_value"]
    total_invested = latest["total_invested"]
    total_return_value = latest["total_return_value"]
    cumulative_divs = latest["cumulative_dividends"]

    def safe_pct(new, old):
        return ((new - old) / old * 100) if old else 0.0

    return {
        "market_value_aed": round(current_mv, 2),
        "total_invested_aed": round(total_invested, 2),
        "unrealised_pnl_aed": round(current_mv - total_invested, 2),
        "unrealised_pnl_pct": round(safe_pct(current_mv, total_invested), 2),
        "total_return_aed": round(total_return_value - total_invested, 2),
        "total_return_pct": round(safe_pct(total_return_value, total_invested), 2),
        "cumulative_dividends_aed": round(cumulative_divs, 2),
        "twr_pct": round(latest["twr_cumulative"] * 100, 2),
        # Growth chips
        "dod_pct": round(safe_pct(current_mv, prev_day["market_value"]), 2),
        "wow_pct": round(safe_pct(current_mv, prev_week["market_value"]), 2),
        "mom_pct": round(safe_pct(current_mv, prev_month["market_value"]), 2),
        "yoy_pct": round(safe_pct(current_mv, start["market_value"]), 2),
        "inception_return_pct": round(latest["twr_cumulative"] * 100, 2),
        # Sparkline data (last 30 points)
        "sparkline_market_value": series["market_value"].tail(30).tolist(),
        "sparkline_total_return": series["total_return_value"].tail(30).tolist(),
    }


def get_next_dividend(filters: OverviewFilters) -> dict | None:
    """Next upcoming dividend across all holdings."""
    tx = get_all_transactions()
    tx = _apply_tx_filters(tx, filters)
    if tx.empty:
        return None

    tickers = tx["ticker"].unique().tolist()
    today = date.today()
    divs = get_dividend_events(tickers)
    upcoming = divs[divs["ex_date"] > today].sort_values("ex_date")

    for _, row in upcoming.iterrows():
        holdings = get_holdings_on_date(today, tickers=[row["ticker"]], transactions=tx)
        shares = holdings.get(row["ticker"], 0)
        if shares > 0:
            return {
                "ticker": row["ticker"],
                "ex_date": str(row["ex_date"]),
                "pay_date": str(row["pay_date"]),
                "amount_per_share": row["amount_aed"],
                "expected_total_aed": round(shares * row["amount_aed"], 2),
                "days_to_ex": (row["ex_date"] - today).days,
            }
    return None


@router.post("/test")
async def test(filters: OverviewFilters):
    return {
        "next_dividend": get_next_dividend(filters),
        "kpis": get_portfolio_kpis(filters),
    }
