# app/services/performance/risk_reward.py

from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd

from app.services.performance.constants import MIN_HISTORY_DAYS, yield_on_cost_by_ticker


def build_risk_reward(
    p,
    risk,
    holdings_end: pd.DataFrame,
    start: date,
    end: date,
    include_dividends: bool,
    x_axis: str,
    y_axis: str,
    blended_daily_returns: pd.Series | None,
    sector_by_ticker: pd.Series,
) -> dict:
    """
    Section 4 -- Risk/Reward Explorer. One dot per currently-open position;
    every metric collapses the date range to a single cross-sectional number.
    Beta reuses the benchmark exposure already computed for Section 2 rather
    than re-fetching benchmark prices. Positions under ~20 trading days of
    history are still plotted but flagged `insufficient_history` so the
    frontend can gray/asterisk instead of showing a falsely-confident number.
    """
    if holdings_end.empty:
        return empty_risk_reward(x_axis, y_axis)

    tickers = holdings_end["ticker"].tolist()
    total_market_value = float(holdings_end["market_value_aed"].sum())
    weights = {
        row["ticker"]: (
            row["market_value_aed"] / total_market_value if total_market_value else 0.0
        )
        for _, row in holdings_end.iterrows()
    }

    returns_matrix = p.returns_matrix(tickers, start, end)
    history_days = {
        t: int(returns_matrix[t].dropna().shape[0]) if t in returns_matrix else 0
        for t in tickers
    }
    twr_by_ticker = {
        t: p.twr(start, end, tickers=[t], include_dividends=include_dividends)
        for t in tickers
    }

    covariance = risk.covariance_matrix_annualized(returns_matrix)
    weight_series = pd.Series(weights)
    contribution = risk.risk_contribution(weight_series, covariance)
    portfolio_vol = risk.portfolio_volatility_annualized(weight_series, covariance)
    yoc_by_ticker = yield_on_cost_by_ticker(p)

    metrics_by_ticker = {
        ticker: _position_metrics(
            p,
            risk,
            ticker,
            start,
            end,
            returns_matrix,
            twr_by_ticker,
            blended_daily_returns,
            contribution,
            portfolio_vol,
            weights,
            yoc_by_ticker,
        )
        for ticker in tickers
    }

    return _assemble(
        tickers,
        metrics_by_ticker,
        weights,
        history_days,
        sector_by_ticker,
        x_axis,
        y_axis,
    )


def _position_metrics(
    p,
    risk,
    ticker: str,
    start: date,
    end: date,
    returns_matrix: pd.DataFrame,
    twr_by_ticker: dict,
    blended_daily_returns: pd.Series | None,
    contribution: pd.Series,
    portfolio_vol: float | None,
    weights: dict,
    yoc_by_ticker: dict,
) -> dict:
    asset_returns = (
        returns_matrix[ticker] if ticker in returns_matrix else pd.Series(dtype=float)
    )
    price_series = p.price_repo.get_ohlcv(ticker, start, end).get(
        "close", pd.Series(dtype=float)
    )

    vol = risk.volatility_annualized(asset_returns)
    drawdown = risk.max_drawdown(price_series)
    beta = (
        risk.beta(asset_returns, blended_daily_returns)
        if blended_daily_returns is not None and not blended_daily_returns.empty
        else None
    )
    twr = twr_by_ticker.get(ticker, {})
    twr_pct = twr.get("total_return")
    annualized_twr_pct = twr.get("annualized_return")
    annualized_return_fraction = (
        annualized_twr_pct / 100.0 if annualized_twr_pct is not None else None
    )
    downside_dev = risk.downside_deviation_annualized(asset_returns)
    sharpe = risk.sharpe_ratio(annualized_return_fraction, vol)
    sortino = risk.sortino_ratio(annualized_return_fraction, downside_dev)

    return {
        "twr_pct": twr_pct,
        "volatility_annualized_pct": round(vol * 100, 2) if vol is not None else None,
        "max_drawdown_pct": round(drawdown * 100, 2) if drawdown is not None else None,
        "beta": round(beta, 3) if beta is not None else None,
        "sharpe_ratio": round(sharpe, 3) if sharpe is not None else None,
        "sortino_ratio": round(sortino, 3) if sortino is not None else None,
        "risk_contribution_pct": (
            round(float(contribution.get(ticker, 0.0)) / portfolio_vol * 100, 2)
            if portfolio_vol
            else None
        ),
        "weight_pct": round(weights.get(ticker, 0.0) * 100, 2),
        "yield_on_cost_pct": yoc_by_ticker.get(ticker),
    }


def _assemble(
    tickers, metrics_by_ticker, weights, history_days, sector_by_ticker, x_axis, y_axis
) -> dict:
    positions = []
    x_values: list[float] = []
    y_values: list[float] = []
    for ticker in tickers:
        x_value = metrics_by_ticker.get(ticker, {}).get(x_axis)
        y_value = metrics_by_ticker.get(ticker, {}).get(y_axis)
        if x_value is None or y_value is None:
            continue
        x_values.append(x_value)
        y_values.append(y_value)
        positions.append(
            {
                "ticker": ticker,
                "sector": sector_by_ticker.get(ticker),
                "weight_pct": round(weights.get(ticker, 0.0) * 100, 2),
                "x_value": x_value,
                "y_value": y_value,
                "insufficient_history": history_days.get(ticker, 0) < MIN_HISTORY_DAYS,
                "history_days": history_days.get(ticker, 0),
                "metrics": metrics_by_ticker[ticker],
            }
        )

    diagonal_slope = None
    ratios = [y / x for x, y in zip(x_values, y_values) if x]
    if ratios:
        diagonal_slope = round(sum(ratios) / len(ratios), 4)

    median_x = round(float(np.median(x_values)), 4) if x_values else None
    median_y = round(float(np.median(y_values)), 4) if y_values else None

    return {
        "x_axis": x_axis,
        "y_axis": y_axis,
        "reference_lines": {
            "diagonal_slope": diagonal_slope,
            "median_x": median_x,
            "median_y": median_y,
        },
        "positions": positions,
    }


def empty_risk_reward(x_axis: str, y_axis: str) -> dict:
    return {
        "x_axis": x_axis,
        "y_axis": y_axis,
        "reference_lines": {"diagonal_slope": None, "median_x": None, "median_y": None},
        "positions": [],
    }
