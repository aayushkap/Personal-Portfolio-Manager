# app/services/overview.py

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from app.core.logger import get_logger
from app.services.base import BaseModule
from app.services.filters import PortfolioFilters
from app.services.overlays import OverlayResolver

from app.utils.parsers import parse_date

logger = get_logger()


class OverviewModule(BaseModule):
    def _resolve_tickers(
        self,
        tickers: list[str] | None,
        sectors: list[str] | None,
    ) -> list[str] | None:
        """Resolve sectors → tickers and merge with any explicit ticker filter."""
        if not sectors:
            return tickers or None

        tx = self.hql.portfolio().transactions()
        if tx.empty:
            return tickers or None

        sector_tickers = set(
            tx[tx["sector"].str.lower().isin([s.lower() for s in sectors])]["ticker"]
        )

        if tickers:
            # Intersection: must satisfy both ticker AND sector filter
            return list(sector_tickers & set(tickers)) or None

        return list(sector_tickers) or None

    def get_overview(
        self,
        filters: PortfolioFilters,
        include_events: bool = False,
    ) -> dict:
        result = self._get_overview(
            start_date=filters.date_range.start,
            end_date=filters.date_range.end,
            include_events=include_events,
            tickers=filters.tickers,
            sectors=filters.sectors,
        )

        # Overlays stay in the service layer as they pull external ticker
        # data that doesn't belong inside the portfolio domain
        if filters.overlays and result["trend"]:
            resolved = OverlayResolver(self).resolve_many(filters.overlays, filters)
            trend_by_date = {row["date"]: row for row in result["trend"]}
            for key, records in resolved.items():
                for r in records:
                    if r["date"] in trend_by_date:
                        trend_by_date[r["date"]][key.lower()] = r["value"]
        return result

    def _get_overview(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
        include_events: bool = False,
        tickers: list[str] | None = None,
        sectors: list[str] | None = None,
    ) -> dict:
        """
            Returns a structured overview of portfolio performance.

            Combines the value() time series, a summary snapshot from the latest
            row, and optionally a chronological list of transaction and dividend
            events within the window.

            Parameters
        -
            start_date : date, optional
                Start of the trend window. Defaults to one year before end_date.
            end_date : date, optional
                End of the window. Defaults to today.
            include_events : bool
                If True, includes a sorted list of buy/sell/dividend events.

            Returns
        -
            dict
                {
                    "summary": {
                        "total_invested":       float,
                        "market_value":         float,
                        "price_return":         float,
                        "price_return_pct":      float,
                        "cumulative_divs":   float,
                        "realized_pnl":      float,
                        "total_return":      float,
                        "total_return_pct":      float,
                    } | None,
                    "trend": [
                        {
                            "date":               str (YYYY-MM-DD),
                            "total_invested": float,
                            "market_value":   float,
                            "total_return":    float,
                        },
                        ...
                    ],
                    "events": [
                        {
                            "date":        str (YYYY-MM-DD),
                            "type":        "BUY" | "SELL" | "DIVIDEND_RECEIVED" | "DIVIDEND_PENDING",
                            "ticker":      str,
                            "amount":  float,
                            "description": str,
                        },
                        ...
                    ],
                }
        """
        end_date = parse_date(end_date) or date.today()
        if end_date > date.today():
            end_date = date.today()
        start_date = parse_date(start_date) or (end_date - timedelta(days=365))

        p = self.hql.portfolio()

        tickers = self._resolve_tickers(tickers, sectors)

        trend_df = p.value(
            start_date=start_date, end_date=end_date, tickers=tickers or None
        )

        if trend_df.empty:
            return {"summary": None, "trend": [], "events": []}

        # Trend
        trend = [
            {
                "date": ts.strftime("%Y-%m-%d"),
                "total_invested": round(row["total_invested_aed"], 2),
                "market_value": round(row["market_value_aed"], 2),
                "total_return": round(row["total_value_aed"], 2),
            }
            for ts, row in trend_df.iterrows()
        ]

        # Summary
        latest = trend_df.iloc[-1]
        total_inv = float(latest["total_invested_aed"])
        market_val = float(latest["market_value_aed"])
        total_val = float(latest["total_value_aed"])

        # Cumulative received dividends all-time (not just the window)
        divs_df = p.dividends()
        cum_divs = (
            float(divs_df.loc[divs_df["status"] == "received", "total_aed"].sum())
            if not divs_df.empty
            else 0.0
        )

        # Realized P&L = total_value - market_value - cumulative divs
        realized_pnl = round(total_val - market_val - cum_divs, 2)

        price_return = round(market_val - total_inv, 2)
        total_return = round(total_val - total_inv, 2)

        def _pct(gain: float, base: float) -> float:
            return round(gain / base * 100, 2) if base > 0 else 0.0

        summary = {
            "total_invested": round(total_inv, 2),
            "market_value": round(market_val, 2),
            "price_return": price_return,
            "price_return_pct": _pct(price_return, total_inv),
            "cumulative_divs": round(cum_divs, 2),
            "realized_pnl": realized_pnl,
            "total_return": total_return,
            "total_return_pct": _pct(total_return, total_inv),
        }

        # Events (optional)
        events = []
        if include_events:
            tx = p.transactions()
            tx_window = tx[
                pd.to_datetime(tx["date"]).dt.date.between(start_date, end_date)
            ]
            for _, row in tx_window.iterrows():
                tx_type = (row["transaction"] or "").strip().upper()
                events.append(
                    {
                        "date": (
                            row["date"].isoformat()
                            if hasattr(row["date"], "isoformat")
                            else str(row["date"])
                        ),
                        "type": tx_type,
                        "ticker": row["ticker"],
                        "amount": round(float(row["total_cost"] or 0), 2),
                        "description": f"{tx_type.title()} {int(row['shares'] or 0)} shares @ AED {row['price']:.2f}",
                    }
                )

            if not divs_df.empty:
                window_divs = divs_df[
                    divs_df["pay_date"].apply(
                        lambda d: d is not None and start_date <= d <= end_date
                    )
                ]
                for _, div in window_divs.iterrows():
                    event_date = div["pay_date"] or div["ex_date"]
                    events.append(
                        {
                            "date": event_date.isoformat(),
                            "type": (
                                "DIVIDEND_RECEIVED"
                                if div["status"] == "received"
                                else "DIVIDEND_PENDING"
                            ),
                            "ticker": div["ticker"],
                            "amount": round(float(div["total_aed"]), 2),
                            "description": f"{'Received' if div['status'] == 'received' else 'Expected'} AED {div['total_aed']:.2f}",
                        }
                    )

            events.sort(key=lambda x: x["date"])

        return {"summary": summary, "trend": trend, "events": events}
