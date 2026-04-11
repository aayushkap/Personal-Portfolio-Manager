# app/services/overview.py

from __future__ import annotations

from datetime import date
from typing import Optional

import pandas as pd

from app.core.logger import get_logger
from app.services.base import BaseModule
from app.services.filters import PortfolioFilters
from app.utils.fin import parse_money, safe_float


logger = get_logger()


class OverviewModule(BaseModule):
    """
    Overview page data.

    get_overview(filters) : {
        summary:  { total_invested, price_return, total_return, ... },
        trend:    [ { date, total_invested, market_value, total_return }, ... ],
        events:   [ { date, type, ticker, amount, description }, ... ]
    }
    """

    def get_overview(
        self,
        filters: PortfolioFilters,
        include_events: bool = False,
    ) -> dict:
        tx = self.apply_filters(self.get_all_transactions(), filters)

        if tx.empty:
            return {"summary": None, "trend": [], "events": []}

        tickers = tx["ticker"].unique().tolist()

        trend = self._build_trend(
            tx, tickers, filters, overlays=getattr(filters, "overlays", [])
        )
        summary = self._build_summary(tx, tickers, trend)
        events = self._build_events(tx, tickers, filters) if include_events else []

        return {"summary": summary, "trend": trend, "events": events}

    def _build_trend(
        self,
        tx: pd.DataFrame,
        tickers: list[str],
        filters: PortfolioFilters,
        overlays: list[str],
    ) -> list[dict]:
        prices = self.get_price_series(
            tickers, filters.date_range.start, filters.date_range.end
        )
        if prices.empty:
            return []

        trading_days = prices.index

        # Holdings matrix: rows = trading days, cols = tickers, values = shares held
        holdings = self._holdings_matrix(tx, trading_days)
        common = holdings.columns.intersection(prices.columns)
        market_value = (holdings[common] * prices[common]).sum(axis=1)

        # Total invested — cumulative cash deployed, seeded before window if needed
        total_invested = self._invested_series(tx, trading_days)

        # Cumulative dividends — seeded from inception so past dividends show correctly
        cum_dividends = self._dividends_series(tickers, tx, trading_days)

        trend_df = pd.DataFrame(
            {
                "date": trading_days.strftime("%Y-%m-%d"),
                "total_invested": total_invested.round(2),
                "market_value": market_value.round(2),
                "total_return": (market_value + cum_dividends).round(2),
            }
        )

        trend_df = trend_df.replace([float("inf"), float("-inf")], None)

        return [
            {k: safe_float(v) for k, v in row.items()}
            for row in trend_df.to_dict(orient="records")
        ]

    def _build_summary(
        self,
        tx: pd.DataFrame,
        tickers: list[str],
        trend: list[dict],
    ) -> Optional[dict]:
        if not trend:
            return None

        latest = trend[-1]
        market_value = latest["market_value"] or 0.0
        total_inv = latest["total_invested"] or 0.0
        total_ret = latest["total_return"] or 0.0

        # Cumulative dividends received all-time
        today = date.today()
        all_divs = self._total_dividends_received(tickers, tx, today)

        price_return = round(market_value - total_inv, 2)
        total_return = round(total_ret - total_inv, 2)

        def _pct(gain: float, base: float) -> float:
            return round(gain / base * 100, 2) if base else 0.0

        return {
            "total_invested": round(total_inv, 2),
            "market_value": round(market_value, 2),
            "price_return": price_return,
            "price_return_pct": _pct(price_return, total_inv),
            "cumulative_divs": round(all_divs, 2),
            "total_return": total_return,
            "total_return_pct": _pct(total_return, total_inv),
        }

    def _build_events(
        self,
        tx: pd.DataFrame,
        tickers: list[str],
        filters: PortfolioFilters,
    ) -> list[dict]:
        events = []
        start, end = filters.date_range.start, filters.date_range.end

        # Buys / Sells
        window = tx[
            (pd.to_datetime(tx["trade_date"]).dt.date >= start)
            & (pd.to_datetime(tx["trade_date"]).dt.date <= end)
        ]
        for _, row in window.iterrows():
            events.append(
                {
                    "date": row["trade_date"].isoformat(),
                    "type": row["action"],
                    "ticker": row["ticker"],
                    "amount": round(row["total_cost"], 2),
                    "description": f"{row['action']} {int(row['shares'])} shares",
                }
            )

        # Dividends
        for ticker_key in tickers:
            for div in self.get_dividends(ticker_key):
                if not div.ex_date or div.ex_date < start or div.ex_date > end:
                    continue
                holdings = self.get_holdings(div.ex_date, [ticker_key], tx)
                shares = holdings.get(ticker_key, 0.0)
                if shares <= 0:
                    continue
                amount = (
                    round(shares * float(div.cash_amount.split()[0]), 2)
                    if div.cash_amount
                    else 0.0
                )
                event_date = div.pay_date or div.ex_date
                events.append(
                    {
                        "date": event_date.isoformat(),
                        "type": (
                            "DIVIDEND_RECEIVED" if div.pay_date else "DIVIDEND_UPCOMING"
                        ),
                        "ticker": ticker_key,
                        "amount": amount,
                        "description": f"{'Received' if div.pay_date else 'Expected'} AED {amount}",
                    }
                )

        return sorted(events, key=lambda x: x["date"])

    # Private helpers
    def _invested_series(
        self, tx: pd.DataFrame, trading_days: pd.DatetimeIndex
    ) -> pd.Series:
        tx = tx.copy()
        tx["date"] = (
            pd.to_datetime(tx["trade_date"]).dt.tz_localize("Asia/Dubai").dt.normalize()
        )
        tx["flow"] = tx.apply(
            lambda r: r["total_cost"] if r["action"] == "BUY" else -r["total_cost"],
            axis=1,
        )

        daily = tx.groupby("date")["flow"].sum().sort_index().cumsum()
        return daily.reindex(trading_days, method="ffill").fillna(0.0)

    def _dividends_series(
        self, tickers: list[str], tx: pd.DataFrame, trading_days: pd.DatetimeIndex
    ) -> pd.Series:
        records = []
        for ticker_key in tickers:
            for div in self.get_dividends(ticker_key):
                if not div.pay_date or not div.cash_amount:
                    continue
                shares = self.get_holdings(div.ex_date, [ticker_key], tx).get(
                    ticker_key, 0.0
                )
                if shares <= 0:
                    continue
                amount, currency = parse_money(div.cash_amount)
                records.append(
                    {
                        "date": pd.Timestamp(div.pay_date, tz="Asia/Dubai"),
                        "amount": amount * self.fx.get(currency, 1.0) * shares,
                    }
                )

        if not records:
            return pd.Series(0.0, index=trading_days)

        daily = (
            pd.DataFrame(records).groupby("date")["amount"].sum().sort_index().cumsum()
        )
        return daily.reindex(trading_days, method="ffill").fillna(0.0)
