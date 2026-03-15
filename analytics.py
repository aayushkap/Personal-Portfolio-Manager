# analytics.py

import os
import json
from datetime import datetime, date, timedelta
from typing import Any, Optional
from cache_manager import CacheManager
from portfolio_snapshot import PortfolioSnapshotter


class PortfolioAnalytics:
    """
    Reads from cache (fundamentals + OHLC) and snapshot CSV.
    Produces all metrics and chart-ready data structures.
    """

    # Known sector map — extend as needed
    SECTOR_MAP = {
        "DEWA": "Utilities",
        "EMAAR": "Real Estate",
        "DUBAIRESI": "Real Estate",
        "FAB": "Banking",
        "MASQ": "Banking",
        "IHC": "Conglomerate",
        "ADNOCGAS": "Energy",
    }

    def __init__(self):
        self.cache = CacheManager()
        self.snapshotter = PortfolioSnapshotter()

    #  Helpers
    def _load_fund(self, ticker_key: str) -> Optional[dict]:
        return self.cache.load_fundamentals(ticker_key)

    def _load_ohlc(self, ticker_key: str) -> list[dict]:
        return self.cache.load_ohlc(ticker_key)

    def _all_tickers(self) -> list[str]:
        cache_dir = self.cache.cache_dir
        return [
            name.replace("_", ":", 1)
            for name in os.listdir(cache_dir)
            if os.path.isdir(os.path.join(cache_dir, name))
        ]

    def _current_price(self, ticker_key: str) -> Optional[float]:
        """Latest close from OHLC — most recent bar."""
        ohlc = self._load_ohlc(ticker_key)
        if not ohlc:
            return None
        return float(ohlc[-1].get("close", 0) or 0)

    def _parse_price_string(self, s: str) -> Optional[float]:
        """'AED 3.61' or '3.61' → 3.61"""
        if not s:
            return None
        import re

        m = re.search(r"[\d,]+\.?\d*", str(s).replace(",", ""))
        return float(m.group()) if m else None

    #  Per-ticker metrics
    def ticker_summary(self, ticker_key: str) -> dict:
        fund = self._load_fund(ticker_key)
        if not fund:
            return {"ticker": ticker_key, "error": "no cache"}

        purchases = fund.get("purchases", [])
        summary = fund.get("purchases_summary", {})
        total_shares = summary.get("total_shares", 0)
        total_cost = summary.get("total_cost_aed", 0)
        avg_cost = summary.get("avg_cost_per_share_aed")

        current_price = self._current_price(ticker_key)
        market_value = (current_price * total_shares) if current_price else None
        unrealized_pnl = (market_value - total_cost) if market_value else None
        unrealized_pct = (
            (unrealized_pnl / total_cost * 100)
            if (unrealized_pnl is not None and total_cost)
            else None
        )

        symbol = ticker_key.split(":")[1]
        sector = self.SECTOR_MAP.get(symbol, "Other")

        # Next dividend from purchases sheet
        next_div_dates = [
            p["next_dividend_date"]
            for p in purchases
            if p.get("next_dividend_date") and p["next_dividend_date"] != "N/A"
        ]
        next_div_amounts = [
            p["next_dividend_amount_aed"]
            for p in purchases
            if p.get("next_dividend_amount_aed")
        ]
        total_next_div = sum(next_div_amounts) if next_div_amounts else None

        return {
            "ticker": ticker_key,
            "symbol": symbol,
            "sector": sector,
            "total_shares": total_shares,
            "avg_cost_aed": avg_cost,
            "total_cost_aed": total_cost,
            "current_price": current_price,
            "market_value_aed": market_value,
            "unrealized_pnl_aed": unrealized_pnl,
            "unrealized_pnl_pct": (
                round(unrealized_pct, 2) if unrealized_pct is not None else None
            ),
            "sector": sector,
            "next_dividend_date": next_div_dates[0] if next_div_dates else None,
            "next_dividend_aed": round(total_next_div, 2) if total_next_div else None,
            "lots": len(purchases),
        }

    #  Portfolio-wide metrics
    def portfolio_summary(self) -> dict:
        tickers = self._all_tickers()
        summaries = [self.ticker_summary(t) for t in tickers]
        valid = [s for s in summaries if "error" not in s and s["market_value_aed"]]

        total_value = sum(s["market_value_aed"] for s in valid)
        total_invested = sum(s["total_cost_aed"] for s in valid)
        total_pnl = total_value - total_invested
        total_pnl_pct = (total_pnl / total_invested * 100) if total_invested else 0

        # Sector allocation
        sector_alloc = {}
        for s in valid:
            sec = s["sector"]
            sector_alloc[sec] = sector_alloc.get(sec, 0) + s["market_value_aed"]

        # Upcoming dividends
        upcoming_dividends = [
            {
                "ticker": s["ticker"],
                "date": s["next_dividend_date"],
                "amount_aed": s["next_dividend_aed"],
            }
            for s in valid
            if s["next_dividend_aed"]
        ]
        total_upcoming_div = sum(
            d["amount_aed"] for d in upcoming_dividends if d["amount_aed"]
        )

        return {
            "as_of": datetime.utcnow().isoformat(),
            "total_market_value_aed": round(total_value, 2),
            "total_invested_aed": round(total_invested, 2),
            "total_unrealized_pnl_aed": round(total_pnl, 2),
            "total_unrealized_pnl_pct": round(total_pnl_pct, 2),
            "num_positions": len(valid),
            "sector_allocation": {k: round(v, 2) for k, v in sector_alloc.items()},
            "upcoming_dividends": upcoming_dividends,
            "total_upcoming_div_aed": round(total_upcoming_div, 2),
            "positions": summaries,
        }

    #  Trend calculations (from snapshot CSV)
    def trend_metrics(self) -> dict:
        rows = self.snapshotter.load_history()
        if not rows:
            return {}

        def _pct_change(newer: dict, older: dict) -> Optional[float]:
            v1 = float(older["total_market_value_aed"])
            v2 = float(newer["total_market_value_aed"])
            if not v1:
                return None
            return round((v2 - v1) / v1 * 100, 2)

        latest = rows[-1]

        def _row_n_days_ago(n: int) -> Optional[dict]:
            target = (date.today() - timedelta(days=n)).isoformat()
            # Find closest row on or before target
            candidates = [r for r in rows if r["date"] <= target]
            return candidates[-1] if candidates else None

        dod = _row_n_days_ago(1)
        wow = _row_n_days_ago(7)
        mom = _row_n_days_ago(30)
        q = _row_n_days_ago(90)
        ytd_r = next(
            (r for r in rows if r["date"] >= f"{date.today().year}-01-01"), None
        )

        return {
            "current_value_aed": float(latest["total_market_value_aed"]),
            "twr_total_pct": float(latest["twr_pct"]),
            "unrealized_pnl_pct": float(latest["unrealized_pnl_pct"]),
            "dod_pct": _pct_change(latest, dod) if dod else None,
            "wow_pct": _pct_change(latest, wow) if wow else None,
            "mom_pct": _pct_change(latest, mom) if mom else None,
            "3m_pct": _pct_change(latest, q) if q else None,
            "ytd_pct": _pct_change(latest, ytd_r) if ytd_r else None,
        }

    #  Chart data
    def chart_ohlc_with_dividends(self, ticker_key: str) -> dict:
        """Candlestick series + dividend event markers for one ticker."""
        ohlc = self._load_ohlc(ticker_key)
        fund = self._load_fund(ticker_key)

        dividends = []
        if fund:
            for row in fund.get("dividends", {}).get("rows", []):
                ex_date = row.get("Ex-Dividend Date")
                amount = row.get("Cash Amount")
                if ex_date and amount:
                    dividends.append({"date": ex_date, "amount": amount})

        # Also mark purchase dates as vertical lines
        purchases = [
            {
                "date": p["purchase_date"],
                "shares": p["shares"],
                "price": p["cost_per_share_aed"],
            }
            for p in (fund.get("purchases") or [])
            if p.get("purchase_date")
        ]

        return {
            "ticker": ticker_key,
            "ohlc": ohlc,
            "dividends": dividends,
            "purchases": purchases,
        }

    def chart_portfolio_value_history(self) -> list[dict]:
        """Time series of portfolio value + TWR for the value-over-time chart."""
        return self.snapshotter.load_history()

    def chart_sector_allocation(self) -> dict:
        summary = self.portfolio_summary()
        return summary["sector_allocation"]

    def chart_pnl_per_ticker(self) -> list[dict]:
        """Sorted list for horizontal bar chart."""
        tickers = self._all_tickers()
        data = []
        for t in tickers:
            s = self.ticker_summary(t)
            if "error" not in s and s.get("unrealized_pnl_pct") is not None:
                data.append(
                    {
                        "ticker": s["symbol"],
                        "pnl_pct": s["unrealized_pnl_pct"],
                        "pnl_aed": s["unrealized_pnl_aed"],
                    }
                )
        return sorted(data, key=lambda x: x["pnl_pct"])

    def chart_cost_vs_value(self) -> list[dict]:
        """Grouped bar: cost basis vs current value per ticker."""
        tickers = self._all_tickers()
        return [
            {
                "ticker": s["symbol"],
                "cost_aed": s["total_cost_aed"],
                "value_aed": s["market_value_aed"],
            }
            for t in tickers
            if "error" not in (s := self.ticker_summary(t)) and s["market_value_aed"]
        ]


obj = PortfolioAnalytics()
print(obj.portfolio_summary())
print(obj.chart_cost_vs_value())
