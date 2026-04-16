# app/services/analytics.py

from __future__ import annotations

import calendar
from datetime import date, timedelta
from typing import Literal
import pandas as pd


from app.core.logger import get_logger
from app.services.base import BaseModule
from app.utils.fin import parse_money

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
        tx = self.get_all_transactions()
        if tx.empty:
            return {"mode": mode, "positions": [], "summary": None}

        today = date.today()
        tickers = tx["ticker"].unique().tolist()
        ticker_currency = (
            tx.drop_duplicates("ticker").set_index("ticker")["currency"].to_dict()
        )
        raw_prices = self.get_latest_prices(tickers)
        prices = {
            t: p * self.fx.get(ticker_currency.get(t, "AED"), 1.0)
            for t, p in raw_prices.items()
        }
        positions = []

        for ticker in tickers:
            t_tx = tx[tx["ticker"] == ticker].sort_values("trade_date")
            buys = t_tx[t_tx["action"] == "BUY"]
            sells = t_tx[t_tx["action"] == "SELL"]

            shares_bought = buys["shares"].sum()
            total_cost = buys["total_cost"].sum()
            shares_sold = sells["shares"].sum()
            sell_proceeds = sells["total_cost"].sum()
            shares_held = max(shares_bought - shares_sold, 0.0)

            avg_cost = total_cost / shares_bought if shares_bought else 0.0
            current_price = prices.get(ticker, 0.0)
            market_value = shares_held * current_price
            cost_basis = shares_held * avg_cost

            unrealized = market_value - cost_basis
            realized = sell_proceeds - (shares_sold * avg_cost)
            divs = self._total_dividends_received([ticker], t_tx, today)

            price_return = unrealized + realized
            total_return = price_return + divs
            return_aed = total_return if mode == "total" else price_return
            return_pct = round(return_aed / total_cost * 100, 2) if total_cost else 0.0

            meta = t_tx.iloc[0]
            positions.append(
                {
                    "ticker": ticker,
                    "sector": meta["sector"],
                    "exchange": meta["exchange"],
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
        total_return = sum(p["return_aed"] for p in positions)

        return {
            "mode": mode,
            "positions": positions,
            "summary": {
                "total_invested": round(total_invested, 2),
                "total_market_value": round(total_market, 2),
                "total_return": round(total_return, 2),
                "total_return_pct": (
                    round(total_return / total_invested * 100, 2)
                    if total_invested
                    else 0.0
                ),
            },
        }

    def get_allocation(
        self,
        by: Literal["position", "sector", "exchange"] = "position",
    ) -> dict:
        tx = self.get_all_transactions()
        if tx.empty:
            return {"by": by, "total_market_value": 0.0, "allocations": []}

        holdings = self.get_holdings(date.today(), transactions=tx)
        if not holdings:
            return {"by": by, "total_market_value": 0.0, "allocations": []}

        ticker_currency = (
            tx.drop_duplicates("ticker").set_index("ticker")["currency"].to_dict()
        )
        raw_prices = self.get_latest_prices(list(holdings.keys()))
        prices = {
            t: p * self.fx.get(ticker_currency.get(t, "AED"), 1.0)
            for t, p in raw_prices.items()
        }

        mv_ticker = {t: round(s * prices.get(t, 0.0), 2) for t, s in holdings.items()}
        total_mv = sum(mv_ticker.values())
        if not total_mv:
            return {"by": by, "total_market_value": 0.0, "allocations": []}

        if by == "position":
            buckets = {t: mv for t, mv in mv_ticker.items()}
        else:
            col = "sector" if by == "sector" else "exchange"
            meta = tx.drop_duplicates("ticker").set_index("ticker")[col].to_dict()
            buckets: dict[str, float] = {}
            for ticker, mv in mv_ticker.items():
                label = meta.get(ticker) or "Unknown"
                buckets[label] = buckets.get(label, 0.0) + mv

        allocations = sorted(
            [
                {
                    "label": label,
                    "market_value": round(mv, 2),
                    "weight_pct": round(mv / total_mv * 100, 2),
                }
                for label, mv in buckets.items()
            ],
            key=lambda x: -x["weight_pct"],
        )

        return {
            "by": by,
            "total_market_value": round(total_mv, 2),
            "allocations": allocations,
        }

    def get_income(self) -> dict:
        tx = self.get_all_transactions()
        if tx.empty:
            return self._empty_income()

        today = date.today()
        year_start = date(today.year, 1, 1)
        q_start, q_end = _quarter_bounds(today)
        tickers = tx["ticker"].unique().tolist()
        events = []
        ytd_total = 0.0
        q_total = 0.0

        for ticker_key in tickers:
            t_tx = tx[tx["ticker"] == ticker_key]
            meta = t_tx.iloc[0]
            divs = self.get_dividends(ticker_key)

            for div in divs:
                if not div.cash_amount:
                    continue

                per_share, currency = parse_money(div.cash_amount)
                if not per_share:
                    continue
                per_share = per_share * self.fx.get(currency, 1.0)

                ref_date = div.pay_date or div.ex_date
                if not ref_date:
                    continue

                shares = self.get_holdings(div.ex_date, [ticker_key], t_tx).get(
                    ticker_key, 0.0
                )
                if shares <= 0:
                    continue

                amount = round(shares * per_share, 2)
                days_away = (ref_date - today).days

                if div.pay_date and div.pay_date <= today:
                    status = "received"
                elif div.ex_date and div.ex_date <= today:
                    status = "entitled"
                elif days_away <= 30:
                    status = "soon"
                else:
                    status = "upcoming"

                events.append(
                    {
                        "ticker": ticker_key,
                        "sector": meta["sector"],
                        "ex_date": div.ex_date.isoformat() if div.ex_date else None,
                        "pay_date": div.pay_date.isoformat() if div.pay_date else None,
                        "amount_per_share": per_share,
                        "shares": shares,
                        "amount": amount,
                        "status": status,
                    }
                )

                if div.pay_date and year_start <= div.pay_date <= today:
                    ytd_total += amount
                if q_start <= ref_date <= q_end:
                    q_total += amount

        total_cost = tx[tx["action"] == "BUY"]["total_cost"].sum()
        total_received = sum(e["amount"] for e in events if e["status"] == "received")
        yoc_alltime = round(total_received / total_cost * 100, 2) if total_cost else 0.0

        # Trailing 12M YoC:
        # dividends received in last 365 days / total invested in last 365 days
        one_year_ago = today - timedelta(days=365)

        trailing_divs = sum(
            e["amount"]
            for e in events
            if e["status"] == "received"
            and e["pay_date"]
            and one_year_ago <= pd.to_datetime(e["pay_date"]).date() <= today
        )

        invested_last_12m = tx[
            (tx["action"] == "BUY")
            & (pd.to_datetime(tx["trade_date"]).dt.date >= one_year_ago)
            & (pd.to_datetime(tx["trade_date"]).dt.date <= today)
        ]["total_cost"].sum()

        yoc_trailing_12m = (
            round(trailing_divs / invested_last_12m * 100, 2)
            if invested_last_12m
            else 0.0
        )

        events.sort(key=lambda x: x["pay_date"] or x["ex_date"] or "")

        return {
            "summary": {
                "total_received_alltime": round(total_received, 2),
                "ytd_received": round(ytd_total, 2),
                "yoc_alltime_pct": yoc_alltime,
                "yoc_trailing_12m_pct": yoc_trailing_12m,
                "current_quarter": _quarter_label(today),
                "quarter_projected": round(q_total, 2),
            },
            "events": events,
        }

    @staticmethod
    def _empty_income() -> dict:
        today = date.today()
        q_start, q_end = _quarter_bounds(today)
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
