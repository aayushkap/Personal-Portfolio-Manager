# app/services/performance/verdict.py

from __future__ import annotations

from datetime import date, timedelta


def build_verdict(p, start: date, end: date, include_dividends: bool) -> dict:
    """
    Section 1 -- Verdict Bar.

    Total return is TWR (sub-period, geometrically linked), not a simple
    (end-start)/start calc, so buys/sells mid-period never get mistaken for
    market performance. Realized/unrealized P&L are plain discrete sums, no
    TWR needed since they're not rates.
    """
    twr = p.twr(start, end, include_dividends=include_dividends)

    period_days = max(1, (end - start).days + 1)
    prior_end = start - timedelta(days=1)
    prior_start = prior_end - timedelta(days=period_days - 1)
    prior_twr = p.twr(prior_start, prior_end, include_dividends=include_dividends)

    realized_df = p.realized_pnl(start, end)
    realized_aed = (
        float(realized_df["realized_aed"].sum()) if not realized_df.empty else 0.0
    )
    prior_realized_df = p.realized_pnl(prior_start, prior_end)
    prior_realized_aed = (
        float(prior_realized_df["realized_aed"].sum())
        if not prior_realized_df.empty
        else 0.0
    )

    holdings_end = p.holdings(on=end)
    unrealized_aed = (
        float((holdings_end["market_value_aed"] - holdings_end["cost_basis_aed"]).sum())
        if not holdings_end.empty
        else 0.0
    )
    holdings_prior = p.holdings(on=prior_end)
    prior_unrealized_aed = (
        float(
            (
                holdings_prior["market_value_aed"] - holdings_prior["cost_basis_aed"]
            ).sum()
        )
        if not holdings_prior.empty
        else 0.0
    )

    return {
        "total_return_pct": twr.get("total_return"),
        "total_return_prior_period_pct": prior_twr.get("total_return"),
        "realized_pnl_aed": round(realized_aed, 2),
        "realized_pnl_prior_period_aed": round(prior_realized_aed, 2),
        "unrealized_pnl_aed": round(unrealized_aed, 2),
        "unrealized_pnl_prior_period_aed": round(prior_unrealized_aed, 2),
        "period_start": start.isoformat(),
        "period_end": end.isoformat(),
        "prior_period_start": prior_start.isoformat(),
        "prior_period_end": prior_end.isoformat(),
    }


def empty_verdict(start_iso: str, end_iso: str) -> dict:
    return {
        "total_return_pct": None,
        "total_return_prior_period_pct": None,
        "realized_pnl_aed": 0.0,
        "realized_pnl_prior_period_aed": 0.0,
        "unrealized_pnl_aed": 0.0,
        "unrealized_pnl_prior_period_aed": 0.0,
        "period_start": start_iso,
        "period_end": end_iso,
        "prior_period_start": None,
        "prior_period_end": None,
    }
