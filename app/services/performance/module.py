# app/services/performance/module.py

from __future__ import annotations

from typing import Literal

from app.services.base import BaseModule
from app.services.filters import PortfolioFilters
from app.services.performance.benchmark_chart import (
    build_benchmark_chart,
    empty_benchmark_chart,
)
from app.services.performance.constants import DEFAULT_BENCHMARK_INDEX, mapped_benchmark
from app.services.performance.positions import build_position_table
from app.services.performance.risk_reward import build_risk_reward, empty_risk_reward
from app.services.performance.verdict import build_verdict, empty_verdict


class PerformanceModule(BaseModule):
    """
    Backs the Portfolio Performance page. One consolidated response covers
    all four sections (verdict bar, weighted benchmark chart, position table,
    risk/reward explorer) so they can never disagree about the underlying
    date range, dividend treatment, or benchmark mapping. See
    app/services/performance/{verdict,benchmark_chart,positions,risk_reward}.py
    for each section's calculation.
    """

    def get_performance(
        self,
        filters: PortfolioFilters,
        *,
        include_dividends: bool = True,
        benchmark_mode: Literal["blended", "single"] = "blended",
        benchmark_indices: list[str] | None = None,
        x_axis: str = "volatility_annualized_pct",
        y_axis: str = "twr_pct",
    ) -> dict:
        p = self.hql.portfolio()
        risk = self.hql.risk()

        start = filters.date_range.start
        end = filters.date_range.end
        tx = p.transactions()
        requested_indices = [i for i in (benchmark_indices or []) if i]
        if requested_indices:
            indices = requested_indices
        elif benchmark_mode == "blended" and not tx.empty:
            indices = sorted(
                {mapped_benchmark(exchange) for exchange in tx["exchange"]}
            )
        else:
            indices = [DEFAULT_BENCHMARK_INDEX]
        if benchmark_mode == "single":
            indices = indices[:1]

        filters_applied = {
            "date_range": {"start": start.isoformat(), "end": end.isoformat()},
            "include_dividends": include_dividends,
            "benchmark": {"mode": benchmark_mode, "indices": indices},
        }

        if tx.empty:
            return {
                "filters_applied": filters_applied,
                "verdict": empty_verdict(start.isoformat(), end.isoformat()),
                "benchmark_chart": empty_benchmark_chart(),
                "positions": [],
                "risk_reward": empty_risk_reward(x_axis, y_axis),
            }

        exchange_by_ticker = tx.drop_duplicates("ticker").set_index("ticker")[
            "exchange"
        ]
        sector_by_ticker = tx.drop_duplicates("ticker").set_index("ticker")["sector"]
        holdings_end = p.holdings(on=end)

        verdict = build_verdict(p, start, end, include_dividends)

        benchmark_chart = build_benchmark_chart(
            p,
            start,
            end,
            indices,
            benchmark_mode,
            include_dividends,
            exchange_by_ticker,
        )
        blended_daily_returns = benchmark_chart.pop("_blended_daily_returns", None)
        benchmark_chart.pop("_index_daily_returns", None)

        positions = build_position_table(
            p,
            tx,
            holdings_end,
            start,
            end,
            include_dividends,
            exchange_by_ticker,
            sector_by_ticker,
        )

        risk_reward = build_risk_reward(
            p,
            risk,
            holdings_end,
            start,
            end,
            include_dividends,
            x_axis,
            y_axis,
            blended_daily_returns,
            sector_by_ticker,
        )

        return {
            "filters_applied": filters_applied,
            "verdict": verdict,
            "benchmark_chart": benchmark_chart,
            "positions": positions,
            "risk_reward": risk_reward,
        }
