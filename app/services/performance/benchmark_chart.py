# app/services/performance/benchmark_chart.py

from __future__ import annotations

from datetime import date

import pandas as pd

from app.config import BENCHMARKS
from app.core.logger import get_logger
from app.services.performance.constants import mapped_benchmark

logger = get_logger()


def build_benchmark_chart(
    p,
    start: date,
    end: date,
    indices: list[str],
    benchmark_mode: str,
    include_dividends: bool,
    exchange_by_ticker: pd.Series,
) -> dict:
    """
    Section 2 -- Weighted Benchmark Chart.

    Benchmark weights are locked to the portfolio's exchange allocation at
    the *start* of the window (GIPS-style), not re-derived daily -- otherwise
    the benchmark itself becomes a moving target. Alpha is simple excess
    return (portfolio TWR - blended benchmark TWR), not Jensen's alpha, since
    a single-beta regression doesn't cleanly apply across five exchanges with
    no common market proxy.

    Returns the public chart payload plus two private keys (`_blended_daily_returns`,
    `_index_daily_returns`) the caller pops off before responding -- Section 4's
    beta calculation reuses whichever benchmark exposure is currently selected
    here instead of re-fetching benchmark prices.
    """
    portfolio_with_div = p.twr(start, end, include_dividends=True)
    portfolio_without_div = p.twr(start, end, include_dividends=False)
    selected_portfolio_twr = (
        portfolio_with_div if include_dividends else portfolio_without_div
    )

    exchange_to_index = {
        ex: mapped_benchmark(ex) for ex in exchange_by_ticker.dropna().unique()
    }
    weights = p.benchmark_weights(on=start, exchange_to_index=exchange_to_index)

    if benchmark_mode == "single" and indices:
        locked_weights = {indices[0]: 1.0}
    else:
        selected_weights = {k: v for k, v in weights.items() if k in indices}
        total_selected = sum(selected_weights.values())
        locked_weights = (
            {k: v / total_selected for k, v in selected_weights.items()}
            if total_selected > 0
            else {}
        )

    per_index, index_daily_returns, blended_growth, realized_weight = _load_indices(
        p, start, end, indices, locked_weights
    )

    blended_chart = None
    alpha_pct = None
    blended_daily_returns = None
    if blended_growth is not None and realized_weight > 0:
        blended_cumulative_pct = (blended_growth - 1.0) * 100.0
        blended_chart = {
            "dates": [d.date().isoformat() for d in blended_cumulative_pct.index],
            "cumulative_return_pct": [
                round(float(v), 4) for v in blended_cumulative_pct.values
            ],
        }
        if selected_portfolio_twr.get("total_return") is not None:
            alpha_pct = round(
                selected_portfolio_twr["total_return"]
                - float(blended_cumulative_pct.iloc[-1]),
                4,
            )

        blended_daily_returns = blended_growth.pct_change(fill_method=None).dropna()

    return {
        "dates": portfolio_with_div["dates"],
        "portfolio_twr_with_dividends_pct": portfolio_with_div["cumulative_return_pct"],
        "portfolio_twr_without_dividends_pct": portfolio_without_div[
            "cumulative_return_pct"
        ],
        "blended_benchmark": blended_chart,
        "per_index": per_index,
        "weights_locked_at_start": locked_weights,
        "alpha_pct": alpha_pct,
        "_blended_daily_returns": blended_daily_returns,
        "_index_daily_returns": index_daily_returns,
    }


def _load_indices(
    p,
    start: date,
    end: date,
    indices: list[str],
    locked_weights: dict[str, float],
) -> tuple[dict, dict, pd.Series | None, float]:
    per_index: dict[str, dict] = {}
    index_daily_returns: dict[str, pd.Series] = {}
    growth_by_index: dict[str, pd.Series] = {}
    realized_weight = 0.0

    for index_key in indices:
        try:
            series = p.price_repo.get_ohlcv(index_key, start, end)["close"]
        except Exception as exc:
            logger.warning("Unable to load benchmark index %s: %s", index_key, exc)
            continue
        if series.empty:
            continue
        if series.index.tz is not None:
            series.index = series.index.tz_localize(None)
        series.index = series.index.normalize()
        series = series.groupby(level=0).last().sort_index()
        if series.empty or series.iloc[0] <= 0:
            continue

        growth = series / series.iloc[0]
        cumulative_pct = (growth - 1.0) * 100.0
        per_index[index_key] = {
            "label": BENCHMARKS.get(index_key, {}).get("label", index_key),
            "dates": [d.date().isoformat() for d in cumulative_pct.index],
            "cumulative_return_pct": [
                round(float(v), 4) for v in cumulative_pct.values
            ],
        }
        index_daily_returns[index_key] = series.pct_change(fill_method=None).dropna()

        weight = locked_weights.get(index_key, 0.0)
        if weight:
            realized_weight += weight
            growth_by_index[index_key] = growth

    blended_growth: pd.Series | None = None
    if growth_by_index and realized_weight > 0:
        # Use the latest close on regional holidays, but do not begin the blend
        # until every weighted index has an observed base price. Treating a
        # missing index as zero would create artificial benchmark crashes.
        growth_frame = pd.DataFrame(growth_by_index).sort_index().ffill().dropna()
        if not growth_frame.empty:
            normalized_weights = pd.Series(
                {
                    index_key: locked_weights[index_key] / realized_weight
                    for index_key in growth_frame.columns
                }
            )
            blended_growth = growth_frame.mul(normalized_weights, axis=1).sum(axis=1)

    return per_index, index_daily_returns, blended_growth, realized_weight


def empty_benchmark_chart() -> dict:
    return {
        "dates": [],
        "portfolio_twr_with_dividends_pct": [],
        "portfolio_twr_without_dividends_pct": [],
        "blended_benchmark": None,
        "per_index": {},
        "weights_locked_at_start": {},
        "alpha_pct": None,
    }
