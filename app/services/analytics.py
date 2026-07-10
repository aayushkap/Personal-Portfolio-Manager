# app/services/analytics.py

from __future__ import annotations

import calendar
from datetime import date, timedelta
from typing import Literal

import pandas as pd

from app.core.logger import get_logger
from app.services.base import BaseModule

logger = get_logger()


def _quarter_label(d: date) -> str:
    return f"Q{(d.month - 1) // 3 + 1} {d.year}"


def _quarter_bounds(d: date) -> tuple[date, date]:
    start_month = ((d.month - 1) // 3) * 3 + 1
    end_month = start_month + 2
    return (
        date(d.year, start_month, 1),
        date(d.year, end_month, calendar.monthrange(d.year, end_month)[1]),
    )


class AnalyticsModule(BaseModule):
    """
    Three sub-modules:
      get_pnl()        — P&L per position (price or total return)
      get_allocation() — portfolio weights by position / sector / exchange
      get_income()     — dividend income, yield, and calendar
    """

    def get_pnl(
        self,
        mode: Literal["price_return", "total"] = "total",
    ) -> dict:
        p = self.hql.portfolio()

        holdings_df = p.holdings()
        if holdings_df.empty:
            return {"mode": mode, "positions": [], "summary": None}

        tx_df = p.transactions()
        if tx_df.empty:
            return {"mode": mode, "positions": [], "summary": None}

        divs_df = p.dividends()
        received_divs = (
            divs_df[divs_df["status"] == "received"]
            if not divs_df.empty
            else pd.DataFrame()
        )
        cum_divs_by_ticker = (
            received_divs.groupby("ticker")["total_aed"].sum().to_dict()
            if not received_divs.empty
            else {}
        )

        # Build per-ticker cost info from transactions
        tx_df = tx_df.copy()
        tx_df["tx_lower"] = tx_df["transaction"].str.lower()
        buys = tx_df[tx_df["tx_lower"] == "buy"]
        sells = tx_df[tx_df["tx_lower"] == "sell"]

        shares_bought = buys.groupby("ticker")["shares"].sum()
        total_cost = buys.groupby("ticker")["total_cost_aed"].sum()
        shares_sold = sells.groupby("ticker")["shares"].sum()
        sell_proceeds = sells.groupby("ticker")["total_cost_aed"].sum()

        # sector/exchange meta — first transaction per ticker
        # meta_cols = ["ticker", "sector", "exchange"]
        ticker_meta = tx_df.drop_duplicates("ticker").set_index("ticker")[
            ["sector", "exchange"]
        ]

        positions = []
        for _, row in holdings_df.iterrows():
            ticker = row["ticker"]

            sb = shares_bought.get(ticker, 0.0)
            tc = total_cost.get(ticker, 0.0)
            ss = shares_sold.get(ticker, 0.0)
            sp = sell_proceeds.get(ticker, 0.0)

            shares_held = float(row["shares"])
            if shares_held <= 0:
                continue

            avg_cost = tc / sb if sb else 0.0
            current_price = float(row["last_price_aed"])
            market_value = float(row["market_value_aed"])
            cost_basis = float(row["cost_basis_aed"])

            unrealized = market_value - cost_basis
            realized = sp - (ss * avg_cost)
            divs = cum_divs_by_ticker.get(ticker, 0.0)

            price_return = unrealized + realized
            total_return = price_return + divs
            return_aed = total_return if mode == "total" else price_return
            return_pct = round(return_aed / tc * 100, 2) if tc else 0.0

            meta = ticker_meta.loc[ticker] if ticker in ticker_meta.index else {}

            positions.append(
                {
                    "ticker": ticker,
                    "sector": meta.get("sector") if hasattr(meta, "get") else None,
                    "exchange": meta.get("exchange") if hasattr(meta, "get") else None,
                    "shares_held": round(shares_held, 4),
                    "avg_cost": round(avg_cost, 4),
                    "current_price": round(current_price, 4),
                    "cost_basis": round(cost_basis, 2),
                    "market_value": round(market_value, 2),
                    "unrealized": round(unrealized, 2),
                    "realized": round(realized, 2),
                    "dividends": round(divs, 2),
                    "return_aed": round(return_aed, 2),
                    "return_pct": return_pct,
                }
            )

        positions.sort(key=lambda x: x["return_aed"])

        total_invested = sum(p["cost_basis"] for p in positions)
        total_market = sum(p["market_value"] for p in positions)
        total_ret = sum(p["return_aed"] for p in positions)

        return {
            "mode": mode,
            "positions": positions,
            "summary": {
                "total_invested": round(total_invested, 2),
                "total_market_value": round(total_market, 2),
                "total_return": round(total_ret, 2),
                "total_return_pct": (
                    round(total_ret / total_invested * 100, 2)
                    if total_invested
                    else 0.0
                ),
            },
        }

    def get_allocation(
        self,
        by: Literal["position", "sector", "exchange"] = "position",
    ) -> dict:
        p = self.hql.portfolio()
        result = p.allocation(by=by)
        # portfolio().allocation() already returns the exact output schema
        return result

    def get_income(self) -> dict:
        p = self.hql.portfolio()

        divs_df = p.dividends()
        if divs_df.empty:
            return self._empty_income()

        tx_df = p.transactions()
        if tx_df.empty:
            return self._empty_income()

        holdings_df = p.holdings()

        today = date.today()
        year_start = date(today.year, 1, 1)
        q_start, q_end = _quarter_bounds(today)
        one_year_ago = today - timedelta(days=365)

        ticker_meta = (
            tx_df.drop_duplicates("ticker").set_index("ticker")[["sector"]]
            if "sector" in tx_df.columns
            else pd.DataFrame()
        )

        events = []
        ytd_total = 0.0
        q_total = 0.0

        for _, div in divs_df.iterrows():
            ticker = div["ticker"]
            ex_date = div["ex_date"]
            pay_date = div["pay_date"]
            amount_per_share = div["amount_per_share_aed"]
            shares = div["shares_held"]
            total_aed = div["total_aed"]
            status = div["status"]

            if not amount_per_share or not shares or shares <= 0:
                continue

            ref_date = pay_date or ex_date
            if ref_date is None:
                continue

            if status == "received":
                event_status = "received"
            elif ex_date and ex_date <= today:
                event_status = "entitled"
            elif ref_date and (ref_date - today).days <= 30:
                event_status = "soon"
            else:
                event_status = "upcoming"

            sector = (
                ticker_meta.loc[ticker, "sector"]
                if ticker in ticker_meta.index
                else None
            )

            amount = round(float(total_aed), 2)

            events.append(
                {
                    "ticker": ticker,
                    "sector": sector,
                    "ex_date": ex_date.isoformat() if ex_date else None,
                    "pay_date": pay_date.isoformat() if pay_date else None,
                    "amount_per_share": round(float(amount_per_share), 4),
                    "shares": round(float(shares), 6),
                    "amount": amount,
                    "status": event_status,
                }
            )

            if pay_date and year_start <= pay_date <= today:
                ytd_total += amount
            if ref_date and q_start <= ref_date <= q_end:
                q_total += amount

        # All-time metrics: unbounded on both sides, no capital-pool
        # mismatch. Deliberately includes exited positions' dividends and
        # their original cost — this is a lifetime income-efficiency figure,
        # not a "current yield" figure, so mixing exited + held is correct here.
        total_cost_aed = tx_df[tx_df["transaction"].str.lower() == "buy"][
            "total_cost_aed"
        ].sum()

        eligible_tickers = set(
            divs_df["ticker"].dropna().astype(str).str.lower().unique()
        )

        eligible_buy_mask = tx_df["transaction"].fillna("").str.lower().eq(
            "buy"
        ) & tx_df["ticker"].fillna("").str.lower().isin(eligible_tickers)

        total_cost_eligible_aed = tx_df.loc[eligible_buy_mask, "total_cost_aed"].sum()

        total_received = sum(e["amount"] for e in events if e["status"] == "received")

        yoc_alltime = (
            round(total_received / total_cost_aed * 100, 2) if total_cost_aed else 0.0
        )
        yoc_eligible_alltime = (
            round(total_received / total_cost_eligible_aed * 100, 2)
            if total_cost_eligible_aed
            else 0.0
        )

        # Trailing 12m metrics: held positions ONLY, on both sides.
        # A sold position's cost basis no longer exists in the book, so it
        # cannot appear in a "current yield" denominator — and its dividends
        # are excluded from this ratio's numerator to match.
        if "shares" in holdings_df.columns and "cost_basis_aed" in holdings_df.columns:
            held_df = holdings_df.loc[
                holdings_df["shares"] > 0, ["ticker", "cost_basis_aed"]
            ]
        else:
            held_df = pd.DataFrame(columns=["ticker", "cost_basis_aed"])

        held_tickers = set(held_df["ticker"])
        cost_basis_held = held_df["cost_basis_aed"].sum()

        cost_basis_held_eligible = held_df.loc[
            held_df["ticker"].str.lower().isin(eligible_tickers), "cost_basis_aed"
        ].sum()

        trailing_divs_held = sum(
            e["amount"]
            for e in events
            if e["status"] == "received"
            and e["ticker"] in held_tickers
            and e["pay_date"]
            and one_year_ago <= pd.to_datetime(e["pay_date"]).date() <= today
        )

        yoc_trailing_12m = (
            round(trailing_divs_held / cost_basis_held * 100, 2)
            if cost_basis_held
            else 0.0
        )
        yoc_eligible_trailing_12m = (
            round(trailing_divs_held / cost_basis_held_eligible * 100, 2)
            if cost_basis_held_eligible
            else 0.0
        )

        events.sort(key=lambda x: x["pay_date"] or x["ex_date"] or "")

        return {
            "summary": {
                "total_received_alltime": round(total_received, 2),
                "ytd_received": round(ytd_total, 2),
                "yoc_alltime_pct": yoc_alltime,
                "yoc_trailing_12m_pct": yoc_trailing_12m,
                "yoc_eligible_alltime_pct": yoc_eligible_alltime,
                "yoc_eligible_12m_pct": yoc_eligible_trailing_12m,
                "current_quarter": _quarter_label(today),
                "quarter_projected": round(q_total, 2),
            },
            "events": events[-8:],
        }

    @staticmethod
    def _empty_income() -> dict:
        today = date.today()
        return {
            "summary": {
                "total_received_alltime": 0.0,
                "ytd_received": 0.0,
                "yoc_alltime_pct": 0.0,
                "yoc_trailing_12m_pct": 0.0,
                "current_quarter": _quarter_label(today),
                "quarter_projected": 0.0,
            },
            "events": [],
        }
