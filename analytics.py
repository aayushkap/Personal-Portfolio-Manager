# analytics.py

import os
import json
import math
from datetime import datetime, date, timedelta
from typing import Any, Optional
from collections import defaultdict
from cache_manager import CacheManager
from portfolio_snapshot import PortfolioSnapshotter
import httpx
import io
from PIL import Image
from colorthief import ColorThief
from typing import Optional


def brand_color_from_url(url: str) -> Optional[str]:
    """
    Download image from URL, extract dominant non-white/black/grey color.
    Returns hex string e.g. '#e8821a', or None on failure.
    """
    try:
        r = httpx.get(
            url,
            timeout=10,
            headers={"User-Agent": "Mozilla/5.0"},
            follow_redirects=True,
        )
        r.raise_for_status()

        # Flatten transparency onto white before analysis
        img = Image.open(io.BytesIO(r.content)).convert("RGBA")
        bg = Image.new("RGBA", img.size, (255, 255, 255, 255))
        bg.paste(img, mask=img.split()[3])

        buf = io.BytesIO()
        bg.convert("RGB").save(buf, format="PNG")
        buf.seek(0)

        palette = ColorThief(buf).get_palette(color_count=8, quality=2)

        for r, g, b in palette:
            is_white = r > 220 and g > 220 and b > 220
            is_black = r < 35 and g < 35 and b < 35
            is_grey = max(r, g, b) - min(r, g, b) < 25
            if not is_white and not is_black and not is_grey:
                return f"#{r:02x}{g:02x}{b:02x}"

        # Fallback: return dominant even if grey
        r, g, b = palette[0]
        return f"#{r:02x}{g:02x}{b:02x}"

    except Exception as e:
        print(f"  [brand_color] Failed for {url}: {e}")
        return None


class PortfolioAnalytics:

    def __init__(self):
        self.cache = CacheManager()
        self.snapshotter = PortfolioSnapshotter()

    # INTERNAL HELPERS
    def _load_fund(self, ticker_key: str) -> Optional[dict]:
        return self.cache.load_fundamentals(ticker_key)

    def _load_ohlc(self, ticker_key: str) -> list[dict]:
        return self.cache.load_ohlc(ticker_key)

    def _all_tickers(self) -> list[str]:
        return [
            name.replace("_", ":", 1)
            for name in os.listdir(self.cache.cache_dir)
            if os.path.isdir(os.path.join(self.cache.cache_dir, name))
        ]

    def _parse_aed(self, val: Any) -> Optional[float]:
        if val is None or val == "":
            return None
        import re

        m = re.search(r"[\d]+\.?\d*", str(val).replace(",", ""))
        return float(m.group()) if m else None

    def _parse_date(self, s: str) -> Optional[date]:
        if not s:
            return None
        for fmt in ("%b %d, %Y", "%m/%d/%Y", "%Y-%m-%d", "%B %d, %Y"):
            try:
                return datetime.strptime(s.strip(), fmt).date()
            except ValueError:
                continue
        return None

    def _parse_pct(self, s: str) -> Optional[float]:
        """'21.23%' → 21.23"""
        if not s:
            return None
        try:
            return float(str(s).replace("%", "").replace(",", "").strip())
        except ValueError:
            return None

    #  OHLC resampling

    def _raw_bars(self, ticker_key: str) -> list[dict]:
        """Raw 15-min OHLC bars, sorted ascending."""
        return self._load_ohlc(ticker_key)  # already sorted in cache_manager

    #  Technical indicators (from daily closes)

    def _technicals(self, ticker_key: str) -> dict:
        bars = self._raw_bars(ticker_key)
        if len(bars) < 2:
            return {}

        closes = [b["close"] for b in bars]
        n = len(closes)

        BARS_PER_YEAR = 26 * 252

        # 52-week window
        w52 = closes[-min(BARS_PER_YEAR, n) :]
        high_52w = max(d["high"] for d in bars[-min(BARS_PER_YEAR, n) :])
        low_52w = min(d["low"] for d in bars[-min(BARS_PER_YEAR, n) :])
        current = closes[-1]

        # Moving averages
        def sma(period: int) -> Optional[float]:
            if n < period:
                return None
            return round(sum(closes[-period:]) / period, 4)

        ma20 = sma(20)
        ma50 = sma(50)
        ma200 = sma(200)

        # Daily returns
        returns = [(closes[i] - closes[i - 1]) / closes[i - 1] for i in range(1, n)]

        # Annualized volatility (BARS_PER_YEAR trading days)
        vol_window = returns[-min(BARS_PER_YEAR, len(returns)) :]
        mean_r = sum(vol_window) / len(vol_window) if vol_window else 0
        variance = (
            sum((r - mean_r) ** 2 for r in vol_window) / len(vol_window)
            if vol_window
            else 0
        )
        volatility_ann = round(math.sqrt(variance) * math.sqrt(BARS_PER_YEAR) * 100, 2)

        # Max drawdown (full history)
        peak = closes[0]
        max_dd = 0.0
        for c in closes:
            if c > peak:
                peak = c
            dd = (c - peak) / peak
            if dd < max_dd:
                max_dd = dd

        # RSI-14
        rsi = None
        if len(returns) >= 14:
            gains = [r for r in returns[-14:] if r > 0]
            losses = [-r for r in returns[-14:] if r < 0]
            avg_g = sum(gains) / 14
            avg_l = sum(losses) / 14
            if avg_l == 0:
                rsi = 100.0
            else:
                rs = avg_g / avg_l
                rsi = round(100 - 100 / (1 + rs), 1)

        # Trend signal (simple MA crossover)
        trend = "neutral"
        if ma50 and ma200:
            trend = "bullish" if ma50 > ma200 else "bearish"
        elif ma20 and closes[-1]:
            trend = "bullish" if closes[-1] > ma20 else "bearish"

        return {
            "current_price": round(current, 4),
            "high_52w": round(high_52w, 4),
            "low_52w": round(low_52w, 4),
            "pct_from_52w_high": round((current - high_52w) / high_52w * 100, 2),
            "pct_from_52w_low": round((current - low_52w) / low_52w * 100, 2),
            "ma_20": ma20,
            "ma_50": ma50,
            "ma_200": ma200,
            "above_ma50": (current > ma50) if ma50 else None,
            "above_ma200": (current > ma200) if ma200 else None,
            "rsi_14": rsi,
            "volatility_ann_pct": volatility_ann,
            "max_drawdown_pct": round(max_dd * 100, 2),
            "trend_signal": trend,
            "trading_days_data": n,
        }

    #  Dividend history helpers

    def _historical_dividends(self, ticker_key: str) -> list[dict]:
        """
        Returns parsed dividend history from scrape.
        [{"ex_date": date, "amount_per_share": float, "pay_date": date|None}]
        """
        fund = self._load_fund(ticker_key)
        if not fund:
            return []
        rows = fund.get("dividends", {}).get("rows", [])
        result = []
        for row in rows:
            amount = self._parse_aed(row.get("Cash Amount"))
            ex_date = self._parse_date(row.get("Ex-Dividend Date", ""))
            pay_date = self._parse_date(row.get("Pay Date", ""))
            if amount and ex_date:
                result.append(
                    {
                        "ex_date": ex_date,
                        "amount_per_share": amount,
                        "pay_date": pay_date,
                    }
                )
        return sorted(result, key=lambda x: x["ex_date"])

    # analytics.py — _dividends_received()

    def _dividends_received(self, ticker_key: str) -> dict:
        fund = self._load_fund(ticker_key)
        if not fund:
            return {"total_aed": 0.0, "events": []}

        purchases = fund.get("purchases", [])
        div_history = self._historical_dividends(ticker_key)
        events = []
        total = 0.0
        today = date.today()

        for div in div_history:
            # ── Only count dividends whose ex-date has strictly passed ──────
            # Ex-date = today means you're entitled but cash hasn't arrived yet
            if div["ex_date"] >= today:
                continue

            shares_on_exdate = sum(
                p["shares"]
                for p in purchases
                if p.get("purchase_date")
                and self._parse_date(p["purchase_date"])
                and self._parse_date(p["purchase_date"]) <= div["ex_date"]
            )
            if shares_on_exdate > 0:
                amount = round(shares_on_exdate * div["amount_per_share"], 2)
                total += amount
                events.append(
                    {
                        "ex_date": div["ex_date"].isoformat(),
                        "amount_per_share": div["amount_per_share"],
                        "shares_entitled": shares_on_exdate,
                        "total_aed": amount,
                    }
                )

        return {"total_aed": round(total, 2), "events": events}

    #
    # POSITION-LEVEL ANALYTICS
    #

    def ticker_summary(self, ticker_key: str) -> dict:
        fund = self._load_fund(ticker_key)
        if not fund:
            return {"ticker": ticker_key, "error": "no cache"}

        purchases = fund.get("purchases", [])
        ps = fund.get("purchases_summary", {})
        total_shares = ps.get("total_shares", 0)
        total_cost = ps.get("total_cost_aed", 0)
        avg_cost = ps.get("avg_cost_per_share_aed")

        # Sector from sheet (dynamic — no hardcode)
        sector = purchases[0].get("sector") if purchases else None
        platform = ", ".join({p["platform"] for p in purchases if p.get("platform")})
        logo_url = purchases[0].get("logo_url") if purchases else None

        brand_color = None
        if logo_url:
            brand_color = brand_color_from_url(logo_url)

        # Earliest purchase date
        purchase_dates = [
            self._parse_date(p["purchase_date"])
            for p in purchases
            if p.get("purchase_date")
        ]
        first_purchase = min(purchase_dates).isoformat() if purchase_dates else None
        days_held = (
            (date.today() - min(purchase_dates)).days if purchase_dates else None
        )

        # Price metrics
        tech = self._technicals(ticker_key)
        current_price = tech.get("current_price") or avg_cost
        price_is_live = tech.get("trading_days_data", 0) > 0

        market_value = round(current_price * total_shares, 2) if current_price else None
        unrealized_pnl = round(market_value - total_cost, 2) if market_value else None
        unrealized_pct = (
            round(unrealized_pnl / total_cost * 100, 2)
            if (unrealized_pnl is not None and total_cost)
            else None
        )

        # Dividend income
        div_data = self._dividends_received(ticker_key)
        div_received_aed = div_data["total_aed"]

        # Total return (price P&L + dividends received)
        total_return_aed = round((unrealized_pnl or 0) + div_received_aed, 2)
        total_return_pct = (
            round(total_return_aed / total_cost * 100, 2) if total_cost else None
        )

        # Yield on cost
        div_history = self._historical_dividends(ticker_key)
        # Forward: annualize last 12 months of dividends per share
        one_year_ago = date.today() - timedelta(days=365)
        trailing_dps = sum(
            d["amount_per_share"] for d in div_history if d["ex_date"] >= one_year_ago
        )
        yield_on_cost = (
            round(trailing_dps / avg_cost * 100, 2)
            if (avg_cost and trailing_dps)
            else None
        )

        # Scrape-derived fundamentals
        stats = fund.get("overview", {}).get("stats", {})
        stat_sec = fund.get("statistics", {}).get("all_stats", {})

        def _s(key):
            return stats.get(key) or stat_sec.get(key)

        return_metrics = fund.get("statistics", {}).get("return_metrics", {})

        # Contribution to portfolio (filled in by portfolio_summary)
        symbol = ticker_key.split(":")[1]

        return {
            "ticker": ticker_key,
            "symbol": symbol,
            "exchange": ticker_key.split(":")[0],
            "sector": sector,
            "logo_url": logo_url,
            "brand_color": brand_color,
            "platform": platform,
            # Position sizing
            "total_shares": total_shares,
            "num_lots": len(purchases),
            "first_purchase_date": first_purchase,
            "days_held": days_held,
            # Cost
            "avg_cost_aed": avg_cost,
            "total_cost_aed": total_cost,
            # Market
            "current_price": current_price,
            "price_is_live": price_is_live,
            "market_value_aed": market_value,
            # Price return
            "unrealized_pnl_aed": unrealized_pnl,
            "unrealized_pnl_pct": unrealized_pct,
            # Dividend income
            "dividends_received_aed": div_received_aed,
            "dividend_events": div_data["events"],
            "yield_on_cost_pct": yield_on_cost,
            # Total return (price + dividends)
            "total_return_aed": total_return_aed,
            "total_return_pct": total_return_pct,
            # Technicals (from OHLC)
            "technicals": tech,
            # Scraped fundamentals
            "fundamentals": {
                "market_cap": _s("Market Cap"),
                "pe_ratio": _s("P/E Ratio"),
                "pb_ratio": _s("P/B Ratio"),
                "ev_ebitda": _s("EV/EBITDA"),
                "revenue_ttm": _s("Revenue (ttm)"),
                "net_income_ttm": _s("Net Income"),
                "eps": _s("EPS (ttm)"),
                "dividend_yield": _s("Dividend Yield"),
                "beta": _s("Beta"),
                "shares_outstanding": _s("Shares Out"),
                "roe": return_metrics.get("Return on Equity (ROE)"),
                "roa": return_metrics.get("Return on Assets (ROA)"),
                "roic": return_metrics.get("Return on Invested Capital (ROIC)"),
            },
            # Portfolio weight — placeholder, filled by portfolio_summary
            "portfolio_weight_pct": None,
            "portfolio_contribution_pnl_pct": None,
        }

    # PORTFOLIO-LEVEL ANALYTICS
    def portfolio_summary(self) -> dict:
        tickers = self._all_tickers()
        positions = [self.ticker_summary(t) for t in tickers]
        valid = [p for p in positions if "error" not in p and p.get("total_cost_aed")]

        total_value = sum(p["market_value_aed"] or p["total_cost_aed"] for p in valid)
        total_invested = sum(p["total_cost_aed"] for p in valid)
        total_pnl = sum(p["unrealized_pnl_aed"] or 0 for p in valid)
        total_pnl_pct = (
            round(total_pnl / total_invested * 100, 2) if total_invested else 0
        )

        total_div_received = sum(p["dividends_received_aed"] for p in valid)
        total_return_aed = round(total_pnl + total_div_received, 2)
        total_return_pct = (
            round(total_return_aed / total_invested * 100, 2) if total_invested else 0
        )

        # Fill portfolio weight + contribution per position
        for p in valid:
            mv = p["market_value_aed"] or p["total_cost_aed"] or 0
            p["portfolio_weight_pct"] = (
                round(mv / total_value * 100, 2) if total_value else 0
            )
            p["portfolio_contribution_pnl_pct"] = (
                round((p["unrealized_pnl_aed"] or 0) / total_invested * 100, 2)
                if total_invested
                else 0
            )

        #  Allocation breakdowns
        def _alloc(key_fn):
            out = defaultdict(float)
            for p in valid:
                out[key_fn(p) or "Unknown"] += p["market_value_aed"] or 0
            return {k: round(v, 2) for k, v in sorted(out.items(), key=lambda x: -x[1])}

        sector_alloc = _alloc(lambda p: p["sector"])
        exchange_alloc = _alloc(lambda p: p["exchange"])
        platform_alloc = _alloc(lambda p: p["platform"])

        #  Risk metrics
        weights = (
            [(p["market_value_aed"] or 0) / total_value for p in valid]
            if total_value
            else []
        )

        # HHI: 0 = perfectly spread, 1 = all in one stock
        hhi = round(sum(w**2 for w in weights), 4) if weights else None

        # Portfolio weighted volatility
        w_vol = (
            sum(
                w * (p["technicals"].get("volatility_ann_pct") or 0)
                for w, p in zip(weights, valid)
            )
            if weights
            else 0
        )

        # Largest single position
        largest = (
            max(valid, key=lambda p: p["market_value_aed"] or 0) if valid else None
        )

        #  Income summary
        # Next payouts (from purchases sheet — only source for forward dividends)
        upcoming = []
        for p in valid:
            fund = self._load_fund(p["ticker"])
            for purchase in fund.get("purchases") or []:
                nd = purchase.get("next_dividend_date")
                na = purchase.get("next_dividend_amount_aed")
                if nd and nd != "N/A" and na:
                    upcoming.append(
                        {
                            "ticker": p["ticker"],
                            "date": nd,
                            "amount_aed": na,
                        }
                    )

        # Deduplicate by ticker (sum if multiple lots)
        upcoming_by_ticker: dict[str, dict] = {}
        for u in upcoming:
            k = u["ticker"]
            if k not in upcoming_by_ticker:
                upcoming_by_ticker[k] = u.copy()
            else:
                upcoming_by_ticker[k]["amount_aed"] = round(
                    (upcoming_by_ticker[k]["amount_aed"] or 0) + (u["amount_aed"] or 0),
                    2,
                )
        upcoming_clean = sorted(
            upcoming_by_ticker.values(), key=lambda x: str(x["date"])
        )

        # Trailing 12M yield on cost (portfolio level)
        total_trailing_div = sum(
            (p["yield_on_cost_pct"] or 0) / 100 * p["total_cost_aed"] for p in valid
        )
        portfolio_yield_on_cost = (
            round(total_trailing_div / total_invested * 100, 2) if total_invested else 0
        )

        return {
            "as_of": datetime.utcnow().isoformat(),
            #  Returns
            "returns": {
                "total_market_value_aed": round(total_value, 2),
                "total_invested_aed": round(total_invested, 2),
                "price_return_aed": round(total_pnl, 2),
                "price_return_pct": total_pnl_pct,
                "dividends_received_aed": round(total_div_received, 2),
                "total_return_aed": total_return_aed,
                "total_return_pct": total_return_pct,
            },
            #  Risk
            "risk": {
                "concentration_hhi": hhi,
                "concentration_label": (
                    "high"
                    if hhi and hhi > 0.25
                    else "moderate" if hhi and hhi > 0.15 else "diversified"
                ),
                "weighted_volatility_pct": round(w_vol, 2),
                "largest_position": largest["ticker"] if largest else None,
                "largest_position_weight_pct": (
                    largest["portfolio_weight_pct"] if largest else None
                ),
                "num_positions": len(valid),
                "num_sectors": len(sector_alloc),
            },
            #  Income
            "income": {
                "dividends_received_aed": round(total_div_received, 2),
                "portfolio_yield_on_cost_pct": portfolio_yield_on_cost,
                "upcoming_dividends": upcoming_clean,
                "total_upcoming_aed": round(
                    sum(u["amount_aed"] for u in upcoming_clean if u["amount_aed"]), 2
                ),
            },
            #  Allocation
            "allocation": {
                "by_sector": sector_alloc,
                "by_exchange": exchange_alloc,
                "by_platform": platform_alloc,
            },
            "positions": valid,
        }

    # TREND METRICS (snapshot CSV)
    def trend_metrics(self) -> dict:
        rows = self.snapshotter.load_history()
        if not rows:
            return {}

        def _pct(newer, older):
            v1 = float(older["total_market_value_aed"])
            v2 = float(newer["total_market_value_aed"])
            return round((v2 - v1) / v1 * 100, 2) if v1 else None

        def _row_ago(n):
            target = (date.today() - timedelta(days=n)).isoformat()
            candidates = [r for r in rows if r["date"] <= target]
            return candidates[-1] if candidates else None

        latest = rows[-1]
        dod, wow, mom, q3, h6 = (_row_ago(n) for n in (1, 7, 30, 90, 180))
        ytd = next((r for r in rows if r["date"] >= f"{date.today().year}-01-01"), None)

        return {
            "dod_pct": _pct(latest, dod) if dod else None,
            "wow_pct": _pct(latest, wow) if wow else None,
            "mom_pct": _pct(latest, mom) if mom else None,
            "3m_pct": _pct(latest, q3) if q3 else None,
            "6m_pct": _pct(latest, h6) if h6 else None,
            "ytd_pct": _pct(latest, ytd) if ytd else None,
            "twr_pct": float(latest["twr_pct"]),
            "since_inception": {
                "days": (date.today() - date.fromisoformat(rows[0]["date"])).days,
                "price_pct": _pct(latest, rows[0]),
                "twr_pct": float(latest["twr_pct"]),
            },
        }

    # CHART DATA
    def _chart_portfolio_value(self) -> dict:
        """
        Two toggleable series:
          price_return  — raw snapshot market value
          total_return  — market value + cumulative dividends received up to that date
        """
        rows = self.snapshotter.load_history()
        tickers = self._all_tickers()

        # Build per-ticker cumulative dividend timeline
        # {ticker: [{date, cumulative_aed_per_share}]}
        all_div_events: list[dict] = []
        for t in tickers:
            fund = self._load_fund(t)
            if not fund:
                continue
            shares = fund.get("purchases_summary", {}).get("total_shares", 0)
            for ev in self._dividends_received(t)["events"]:
                all_div_events.append(
                    {
                        "date": ev["ex_date"],
                        "amount_aed": ev["total_aed"],
                    }
                )

        # Sort dividend events
        all_div_events.sort(key=lambda x: x["date"])

        price_series = []
        total_series = []
        cum_div = 0.0

        for row in rows:
            d = row["date"]
            # Add dividends that landed on or before this snapshot date
            while all_div_events and all_div_events[0]["date"] <= d:
                cum_div += all_div_events.pop(0)["amount_aed"]

            market_val = float(row["total_market_value_aed"])
            price_series.append({"date": d, "value_aed": market_val})
            total_series.append(
                {"date": d, "value_aed": round(market_val + cum_div, 2)}
            )

        return {
            "price_return_series": price_series,
            "total_return_series": total_series,
            "note": "Toggle between series in UI. total_return adds cumulative dividends received to market value.",
        }

    def _chart_ohlc(self, ticker_key: str) -> dict:
        bars = self._raw_bars(ticker_key)
        fund = self._load_fund(ticker_key)

        purchases = fund.get("purchases") or []
        purchase_dates = [
            self._parse_date(p["purchase_date"])
            for p in purchases
            if p.get("purchase_date")
        ]
        holding_start = min(purchase_dates) if purchase_dates else None

        div_markers = [
            {
                "date": d["ex_date"].isoformat(),
                "amount_per_share": d["amount_per_share"],
            }
            for d in self._historical_dividends(ticker_key)
            if holding_start and d["ex_date"] >= holding_start
        ]

        purchase_markers = [
            {
                "date": p["purchase_date"],
                "shares": p["shares"],
                "price": p["cost_per_share_aed"],
                "platform": p.get("platform"),
            }
            for p in purchases
            if p.get("purchase_date")
        ]

        filtered_bars = [
            b
            for b in bars
            if holding_start is None or b["datetime"][:10] >= holding_start.isoformat()
        ]

        return {
            "ticker": ticker_key,
            "holding_since": holding_start.isoformat() if holding_start else None,
            "granularity": "15min",
            "bars": filtered_bars,  # ← was daily_ohlc
            "dividend_markers": div_markers,
            "purchase_markers": purchase_markers,
        }

    # MAIN ENTRY POINT
    def run(self) -> dict:
        summary = self.portfolio_summary()
        trends = self.trend_metrics()

        # Daily snapshot
        last = self.snapshotter._last_row()
        prev_invested = float(last["total_invested_aed"]) if last else 0.0
        cash_flow_today = max(
            0.0, summary["returns"]["total_invested_aed"] - prev_invested
        )
        snapshot = self.snapshotter.record(
            market_value=summary["returns"]["total_market_value_aed"],
            total_invested=summary["returns"]["total_invested_aed"],
            cash_flow_today=cash_flow_today,
        )

        return {
            #  Core
            "summary": summary,
            "trends": trends,
            "snapshot": snapshot,
            #  Charts
            "charts": {
                "portfolio_value": self._chart_portfolio_value(),
                "pnl_per_ticker": sorted(
                    [
                        {
                            "ticker": p["symbol"],
                            "pnl_pct": p["unrealized_pnl_pct"],
                            "pnl_aed": p["unrealized_pnl_aed"],
                            "total_ret_pct": p["total_return_pct"],
                            "weight_pct": p["portfolio_weight_pct"],
                        }
                        for p in summary["positions"]
                        if p.get("unrealized_pnl_pct") is not None
                    ],
                    key=lambda x: x["pnl_pct"],
                ),
                "cost_vs_value": [
                    {
                        "ticker": p["symbol"],
                        "cost_aed": p["total_cost_aed"],
                        "value_aed": p["market_value_aed"],
                        "div_aed": p["dividends_received_aed"],
                    }
                    for p in summary["positions"]
                    if p.get("market_value_aed")
                ],
                "sector_allocation": summary["allocation"]["by_sector"],
                "exchange_allocation": summary["allocation"]["by_exchange"],
                "platform_allocation": summary["allocation"]["by_platform"],
                "ohlc": {t: self._chart_ohlc(t) for t in self._all_tickers()},
            },
        }


# if __name__ == "__main__":
#     results = PortfolioAnalytics().run()
#     with open("analytics_output.json", "w") as f:
#         json.dump(results, f, indent=2, default=str)
#     ret = results["summary"]["returns"]
#     print(
#         f"\nValue:        AED {ret['total_market_value_aed']:>10,.2f}"
#         f"\nPrice Return: AED {ret['price_return_aed']:>+10,.2f}  ({ret['price_return_pct']:+.2f}%)"
#         f"\nDividends:    AED {ret['dividends_received_aed']:>10,.2f}"
#         f"\nTotal Return: AED {ret['total_return_aed']:>+10,.2f}  ({ret['total_return_pct']:+.2f}%)"
#     )
