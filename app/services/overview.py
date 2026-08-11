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
        breakdown: bool = False,
        period_returns: bool = False,
    ) -> dict:
        result = self._get_overview(
            start_date=filters.date_range.start,
            end_date=filters.date_range.end,
            include_events=include_events,
            tickers=filters.tickers,
            sectors=filters.sectors,
            period_returns=period_returns,
        )

        if not result["trend"]:
            return result

        trend_by_date = {row["date"]: row for row in result["trend"]}

        # Overlays
        if filters.overlays:
            resolved = OverlayResolver(self).resolve_many(filters.overlays, filters)
            for key, records in resolved.items():
                for r in records:
                    if r["date"] in trend_by_date:
                        trend_by_date[r["date"]][key.lower()] = r["value"]

        # Breakdown: per-ticker market_value injected as ticker-keyed dynamic series
        if breakdown and filters.tickers:
            for ticker in filters.tickers:
                ticker_df = self.hql.portfolio().value(
                    start_date=filters.date_range.start,
                    end_date=filters.date_range.end,
                    tickers=[ticker],
                )
                if ticker_df.empty:
                    continue
                for ts, row in ticker_df.iterrows():
                    date_str = ts.strftime("%Y-%m-%d")
                    if date_str in trend_by_date:
                        trend_by_date[date_str][ticker] = round(
                            float(row["market_value_aed"]), 2
                        )

            # Strip the static aggregate lines so the frontend only renders per-ticker series
            for row in result["trend"]:
                row.pop("total_invested", None)
                row.pop("market_value", None)
                row.pop("total_return", None)

        return result

    def _get_overview(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
        include_events: bool = False,
        tickers: list[str] | None = None,
        sectors: list[str] | None = None,
        period_returns: bool = False,
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
            period_returns : bool
                If True, summary return fields cover only start_date through
                end_date. If False, they remain cumulative through end_date. 
                Uses the Modified Dietz method to calculate the effective capital base for the period.

            Returns
        -
            dict
                {
                    "summary": {
                        "total_invested":       float,
                        "ending_total_invested": float,  # period_returns only
                        "market_value":         float,
                        "ending_market_value":  float,  # period_returns only
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

        # A period return needs the last valuation before start_date as its
        # opening balance. value() normally trims that row from its result, so
        # load from inception only for the opt-in period calculation.
        value_start = start_date
        if period_returns:
            scoped_tx = p.transactions()
            if tickers and not scoped_tx.empty:
                scoped_tx = scoped_tx[scoped_tx["ticker"].isin(tickers)]
            if not scoped_tx.empty:
                first_tx = pd.to_datetime(scoped_tx["date"], errors="coerce").min()
                if pd.notna(first_tx):
                    value_start = min(start_date, first_tx.date())

        value_df = p.value(
            start_date=value_start, end_date=end_date, tickers=tickers or None
        )
        trend_df = value_df[value_df.index.date >= start_date]

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

        # Received dividends, optionally scoped to the requested period.
        divs_df = p.dividends()
        if tickers and not divs_df.empty:
            divs_df = divs_df[divs_df["ticker"].isin(tickers)]
        received_divs = (
            divs_df[divs_df["status"] == "received"].copy()
            if not divs_df.empty
            else divs_df
        )

        latest_date = trend_df.index[-1].date()

        def _dividends_through(cutoff: date, *, inclusive: bool = True) -> float:
            if received_divs.empty:
                return 0.0
            pay_dates = pd.to_datetime(
                received_divs["pay_date"], errors="coerce"
            ).dt.date
            mask = pay_dates <= cutoff if inclusive else pay_dates < cutoff
            return float(received_divs.loc[mask, "total_aed"].sum())

        cum_divs = _dividends_through(latest_date)

        # These are cumulative values by default, preserving the existing API.
        realized_pnl = total_val - market_val - cum_divs
        price_return = market_val - total_inv
        total_return = total_val - total_inv
        return_base = total_inv

        if period_returns:
            before_start = value_df[value_df.index.date < start_date]
            opening = before_start.iloc[-1] if not before_start.empty else None

            opening_inv = (
                float(opening["total_invested_aed"]) if opening is not None else 0.0
            )
            opening_market = (
                float(opening["market_value_aed"]) if opening is not None else 0.0
            )
            opening_total = (
                float(opening["total_value_aed"]) if opening is not None else 0.0
            )
            opening_divs = _dividends_through(start_date, inclusive=False)
            opening_realized = opening_total - opening_market - opening_divs

            # Subtract each component's opening cumulative value. This makes
            # Jan-to-Mar report only the P&L generated during Jan-to-Mar.
            cum_divs -= opening_divs
            realized_pnl -= opening_realized
            price_return -= opening_market - opening_inv
            total_return -= opening_total - opening_inv

            # Modified Dietz capital base: opening market value plus each cash
            # flow weighted by how long it was invested during the period.
            # This keeps the percentage meaningful when buys/sells occur inside
            # the selected range.
            return_base = opening_market
            period_days = max((latest_date - start_date).days, 1)

            period_tx = scoped_tx.copy()
            if not period_tx.empty:
                period_tx["_date"] = pd.to_datetime(
                    period_tx["date"], errors="coerce"
                ).dt.date
                period_tx = period_tx[
                    period_tx["_date"].between(start_date, latest_date)
                ]
                signs = (
                    period_tx["transaction"]
                    .str.lower()
                    .map({"buy": 1.0, "sell": -1.0})
                    .fillna(0.0)
                )
                amounts = pd.to_numeric(
                    period_tx["total_cost_aed"], errors="coerce"
                ).fillna(0.0)
                for flow_date, flow in zip(period_tx["_date"], amounts * signs):
                    weight = max((latest_date - flow_date).days, 0) / period_days
                    return_base += float(flow) * weight

            if not received_divs.empty:
                pay_dates = pd.to_datetime(
                    received_divs["pay_date"], errors="coerce"
                ).dt.date
                period_divs = received_divs[pay_dates.between(start_date, latest_date)]
                for pay_date, amount in zip(
                    pd.to_datetime(period_divs["pay_date"], errors="coerce").dt.date,
                    pd.to_numeric(period_divs["total_aed"], errors="coerce").fillna(
                        0.0
                    ),
                ):
                    weight = max((latest_date - pay_date).days, 0) / period_days
                    return_base -= float(amount) * weight

        realized_pnl = round(realized_pnl, 2)
        price_return = round(price_return, 2)
        cum_divs = round(cum_divs, 2)
        # Build the displayed total from the displayed components so the
        # response always reconciles exactly to the cent.
        total_return = round(price_return + cum_divs + realized_pnl, 2)

        def _pct(gain: float, base: float) -> float:
            return round(gain / base * 100, 2) if base > 0 else 0.0

        summary_total_inv = round(return_base, 2)
        summary_market_val = (
            summary_total_inv + price_return if period_returns else market_val
        )
        summary = {
            # In period mode this is the effective capital used as the return
            # denominator, so the displayed amount reconciles directly with
            # total_return_pct. The cumulative ending balance remains available
            # separately for consumers that need the end-of-period snapshot.
            "total_invested": summary_total_inv,
            "market_value": round(summary_market_val, 2),
            "price_return": price_return,
            "price_return_pct": _pct(price_return, return_base),
            "cumulative_divs": cum_divs,
            "realized_pnl": realized_pnl,
            "total_return": total_return,
            "total_return_pct": _pct(total_return, return_base),
        }
        if period_returns:
            summary["ending_total_invested"] = round(total_inv, 2)
            summary["ending_market_value"] = round(market_val, 2)

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
