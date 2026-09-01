# app/services/performance/positions.py

from __future__ import annotations

from datetime import date

import pandas as pd


def build_position_table(
    p,
    tx: pd.DataFrame,
    holdings_end: pd.DataFrame,
    start: date,
    end: date,
    include_dividends: bool,
    exchange_by_ticker: pd.Series,
    sector_by_ticker: pd.Series,
) -> list[dict]:
    """
    Section 3 -- Position Table. Both open and closed positions, all-time.

    unrealized_pnl_pct is a plain (current-cost)/cost calc -- no TWR needed,
    it's a single holding period with no interim cash flows to correct for.
    twr_pct reuses the same sub-period/geometric-link method as the verdict
    bar, applied per-ticker -- this is what makes a position bought in
    several lots (e.g. three separate EMAAR buys) resolve correctly.
    """
    total_market_value = (
        float(holdings_end["market_value_aed"].sum()) if not holdings_end.empty else 0.0
    )
    open_tickers = set(holdings_end["ticker"]) if not holdings_end.empty else set()
    all_tickers = tx["ticker"].unique().tolist()

    realized_df = p.realized_pnl(start, end)
    realized_by_ticker = (
        realized_df.set_index("ticker")["realized_aed"].to_dict()
        if not realized_df.empty
        else {}
    )

    divs_by_ticker: dict[str, float] = {}
    if include_dividends:
        divs_df = p.dividends()
        if not divs_df.empty:
            received = divs_df[
                (divs_df["status"] == "received")
                & (divs_df["pay_date"] >= start)
                & (divs_df["pay_date"] <= end)
            ]
            divs_by_ticker = received.groupby("ticker")["total_aed"].sum().to_dict()

    holdings_by_ticker = (
        holdings_end.set_index("ticker").to_dict("index")
        if not holdings_end.empty
        else {}
    )

    rows = []
    for ticker in all_tickers:
        is_open = ticker in open_tickers
        holding = holdings_by_ticker.get(ticker, {})
        market_value = float(holding.get("market_value_aed", 0.0))
        cost_basis = float(holding.get("cost_basis_aed", 0.0))

        unrealized_pct = (
            round((market_value - cost_basis) / cost_basis * 100, 2)
            if is_open and cost_basis
            else None
        )
        weight_pct = (
            round(market_value / total_market_value * 100, 2)
            if is_open and total_market_value
            else 0.0
        )

        twr = p.twr(start, end, tickers=[ticker], include_dividends=include_dividends)

        if ticker in realized_by_ticker:
            realized_aed = round(realized_by_ticker[ticker], 2)
        elif not is_open:
            realized_aed = 0.0
        else:
            realized_aed = None

        row = {
            "ticker": ticker,
            "sector": sector_by_ticker.get(ticker),
            "exchange": exchange_by_ticker.get(ticker),
            "is_open": is_open,
            "unrealized_pnl_pct": unrealized_pct,
            "twr_pct": twr.get("total_return"),
            "weight_pct": weight_pct,
            "realized_pnl_aed": realized_aed,
        }
        if include_dividends:
            row["dividends_received_aed"] = round(divs_by_ticker.get(ticker, 0.0), 2)

        rows.append(row)

    return rows
