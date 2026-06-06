# app/services/holdings.py

from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

import pandas as pd

from app.core.logger import get_logger
from app.services.base import BaseModule
from app.services.filters import PortfolioFilters
from app.utils.fin import safe_float as _safe

logger = get_logger()
pd.set_option("display.max_rows", None)


class HoldingsModule(BaseModule):
    def get_holdings_list(self, filters: PortfolioFilters) -> list[dict]:
        p = self.hql.portfolio()

        holdings = p.holdings()
        if holdings.empty:
            return []

        # Dividends grouped by ticker (received only, all-time)
        divs_df = p.dividends()
        cum_divs_by_ticker: dict[str, float] = {}
        if not divs_df.empty:
            received = divs_df[divs_df["status"] == "received"]
            cum_divs_by_ticker = received.groupby("ticker")["total_aed"].sum().to_dict()

        # Price history for DoD / MoM / 3M and sparkline
        today = date.today()
        tickers = holdings["ticker"].tolist()

        price_frames = []
        for ticker in tickers:
            t = self.hql.ticker(ticker)
            history = t.prices(start=today - timedelta(days=120), end=today)
            if history is None or history.empty:
                continue

            if isinstance(history, pd.DataFrame):
                if "close" not in history.columns:
                    continue
                history = history["close"]

            price_frames.append(history.rename(ticker))

        prices = pd.concat(price_frames, axis=1) if price_frames else pd.DataFrame()

        if not prices.empty:
            if prices.index.tz is not None:
                prices.index = prices.index.tz_localize(None)
            prices.index = prices.index.normalize()
            prices = prices.reindex(
                pd.date_range(today - timedelta(days=120), today, freq="D")
            ).ffill()

        results = []
        for _, row in holdings.iterrows():
            ticker = row["ticker"]
            card = self._build_card(
                ticker=ticker,
                shares=float(row["shares"]),
                cost_basis=float(row["cost_basis_aed"]),
                market_value=float(row["market_value_aed"]),
                current_price=float(row["last_price_aed"]),
                cum_divs=cum_divs_by_ticker.get(ticker, 0.0),
                prices=prices,
                today=today,
            )
            if card:
                results.append(card)

        return sorted(results, key=lambda x: x["total_value"] or 0, reverse=True)

    def get_holding_detail(
        self,
        ticker: str,
        timeframe: str = "1m",
        filters: Optional[PortfolioFilters] = None,
    ) -> dict:
        p = self.hql.portfolio()
        today = date.today()

        return {
            "ticker": ticker,
            "chart": self._build_chart(ticker, timeframe, today),
            "transactions": self._build_transactions(ticker, today, p),
            "fundamentals": self._build_fundamentals(ticker),
        }

    # Card builder
    def _build_card(
        self,
        ticker: str,
        shares: float,
        cost_basis: float,
        market_value: float,
        current_price: float,
        cum_divs: float,
        prices: pd.DataFrame,
        today: date,
    ) -> Optional[dict]:
        if shares <= 0:
            return None

        # Price changes
        col = prices.get(ticker) if ticker in prices.columns else None

        def _price_ago(days: int) -> Optional[float]:
            if col is None or col.empty:
                return None
            target = pd.Timestamp(today - timedelta(days=days))
            past = col[col.index <= target]
            return float(past.iloc[-1]) if not past.empty else None

        def _pct(new, old):
            if new is None or old is None or old == 0:
                return None
            return round((new - old) / old * 100, 2)

        total_return = round(market_value + cum_divs - cost_basis, 2)
        yoc = round(cum_divs / cost_basis * 100, 2) if cost_basis > 0 else 0.0

        t = self.hql.ticker(ticker)
        info = t.info()

        return {
            "ticker": ticker,
            "name": info.get("name") or ticker,
            "sector": info.get("sector"),
            "exchange": info.get("exchange"),
            "logo_url": info.get("logo_url"),
            "shares": round(shares, 6),
            "current_price": _safe(current_price),
            "cost_basis": _safe(cost_basis),
            "total_value": _safe(market_value),
            "total_return": _safe(total_return),
            "total_return_pct": _safe(_pct(market_value + cum_divs, cost_basis)),
            "dod_pct": _safe(_pct(current_price, _price_ago(1))),
            "mom_pct": _safe(_pct(current_price, _price_ago(30))),
            "three_month_pct": _safe(_pct(current_price, _price_ago(90))),
            "cumulative_divs": round(cum_divs, 2),
            "yoc_pct": _safe(yoc),
            "sparkline": self._build_sparkline(ticker, prices, today),
        }

    # Detail builders
    def _build_chart(
        self,
        ticker: str,
        timeframe: str,
        today: date,
    ) -> list[dict]:
        _TIMEFRAME = {
            "1d": {"granularity": "15min", "days_back": 1},
            "1w": {"granularity": "30min", "days_back": 7},
            "1m": {"granularity": "60min", "days_back": 30},
            "3m": {"granularity": "1D", "days_back": 90},
            "6m": {"granularity": "1D", "days_back": 180},
            "1y": {"granularity": "1D", "days_back": 365},
            "5y": {"granularity": "1D", "days_back": 365 * 5},
            "all": {"granularity": "1D", "days_back": None},
        }

        config = _TIMEFRAME.get(timeframe, _TIMEFRAME["1m"])
        days_back = config["days_back"]
        start = (today - timedelta(days=days_back)) if days_back else date(2000, 1, 1)

        # FIX: Instantiate the ticker object, then call ohlcv on it
        t = self.hql.ticker(ticker)
        ohlcv = t.ohlcv(
            start=start,
            end=today,
            granularity=config["granularity"],
        )

        if ohlcv is None or (hasattr(ohlcv, "empty") and ohlcv.empty):
            return []

        is_intraday = config["granularity"] != "1D"
        date_fmt = "%Y-%m-%dT%H:%M" if is_intraday else "%Y-%m-%d"

        return [
            {
                "date": ts.strftime(date_fmt),
                "close": _safe(round(float(row["close"]), 4)),
                "volume": int(row["volume"]) if pd.notna(row["volume"]) else None,
            }
            for ts, row in ohlcv.iterrows()
        ]

    def _build_transactions(
        self,
        ticker: str,
        today: date,
        p,
    ) -> list[dict]:
        records = []

        tx = p.transactions()
        ticker_tx = tx[tx["ticker"] == ticker].sort_values("date")

        for _, row in ticker_tx.iterrows():
            tx_type = (row["transaction"] or "").strip().upper()
            records.append(
                {
                    "date": (
                        row["date"].isoformat()
                        if hasattr(row["date"], "isoformat")
                        else str(row["date"])
                    ),
                    "type": tx_type,
                    "shares": round(float(row["shares"] or 0), 6),
                    "price": _safe(round(float(row["price_aed"] or 0), 4)),
                    "total": _safe(round(float(row["total_cost_aed"] or 0), 2)),
                }
            )

        # Received dividends
        divs = p.dividends()
        if not divs.empty:
            ticker_divs = divs[
                (divs["ticker"] == ticker) & (divs["status"] == "received")
            ]
            for _, div in ticker_divs.iterrows():
                event_date = div["pay_date"] or div["ex_date"]
                if event_date is None or event_date > today:
                    continue
                records.append(
                    {
                        "date": event_date.isoformat(),
                        "type": "DIVIDEND",
                        "shares": round(float(div["shares_held"]), 6),
                        "price": _safe(round(float(div["amount_per_share_aed"]), 4)),
                        "total": _safe(round(float(div["total_aed"]), 2)),
                    }
                )

        return sorted(records, key=lambda x: x["date"])

    def _build_fundamentals(self, ticker: str) -> dict:
        result = {}
        t = self.hql.ticker(ticker)  # per-ticker object for all per-ticker methods

        ov = t.overview()  # ticker method, not portfolio
        if ov:
            result["snapshot"] = {
                k: v
                for k, v in {
                    "market_cap": ov.get("market_cap"),
                    "pe_ratio": ov.get("pe_ratio"),
                    "forward_pe": ov.get("forward_pe"),
                    "eps": ov.get("eps"),
                    "beta": ov.get("beta"),
                    "volume": ov.get("volume"),
                    "week_52_low": ov.get("week_52_low"),
                    "week_52_high": ov.get("week_52_high"),
                    "dividend_yield": ov.get("dividend_yield"),
                    "ex_dividend_date": ov.get("ex_dividend_date"),
                    "earnings_date": ov.get("earnings_date"),
                    "analyst_rating": ov.get("analyst_rating"),
                    "price_target": ov.get("price_target"),
                }.items()
                if v is not None
            }

        stats = t.statistics()  # ticker method
        if stats:

            def _s(section, key):
                return stats.get(section, {}).get(key)

            result["valuation"] = {
                k: v
                for k, v in {
                    "pe_ratio": _s("Valuation Ratios", "pe_ratio"),
                    "forward_pe": _s("Valuation Ratios", "forward_pe"),
                    "ps_ratio": _s("Valuation Ratios", "ps_ratio"),
                    "pb_ratio": _s("Valuation Ratios", "pb_ratio"),
                    "peg_ratio": _s("Valuation Ratios", "peg_ratio"),
                    "p_fcf": _s("Valuation Ratios", "P/FCF Ratio"),
                }.items()
                if v is not None
            }

            result["efficiency"] = {
                k: v
                for k, v in {
                    "roe": _s("Financial Efficiency", "roe"),
                    "roa": _s("Financial Efficiency", "roa"),
                    "roic": _s("Financial Efficiency", "roic"),
                    "roce": _s("Financial Efficiency", "roce"),
                    "wacc": _s("Financial Efficiency", "wacc"),
                }.items()
                if v is not None
            }

            result["margins"] = {
                k: v
                for k, v in {
                    "gross_margin": _s("Margins", "Gross Margin"),
                    "operating_margin": _s("Margins", "Operating Margin"),
                    "profit_margin": _s("Margins", "Profit Margin"),
                    "fcf_margin": _s("Margins", "FCF Margin"),
                }.items()
                if v is not None
            }

            result["balance_sheet"] = {
                k: v
                for k, v in {
                    "total_assets": _s("Balance Sheet", "Total Assets"),
                    "total_debt": _s("Balance Sheet", "Total Debt"),
                    "net_cash": _s("Balance Sheet", "Net Cash"),
                    "book_value": _s("Balance Sheet", "Equity (Book Value)"),
                    "working_capital": _s("Balance Sheet", "Working Capital"),
                }.items()
                if v is not None
            }

            result["financial_position"] = {
                k: v
                for k, v in {
                    "current_ratio": _s("Financial Position", "Current Ratio"),
                    "debt_equity": _s("Financial Position", "Debt / Equity"),
                    "debt_ebitda": _s("Financial Position", "Debt / EBITDA"),
                    "interest_coverage": _s("Financial Position", "Interest Coverage"),
                }.items()
                if v is not None
            }

            result["scores"] = {
                k: v
                for k, v in {
                    "altman_z": _s("Scores", "Altman Z-Score"),
                    "piotroski_f": _s("Scores", "Piotroski F-Score"),
                    "graham_number": _s("Fair Value", "Graham Number"),
                    "graham_upside": _s("Fair Value", "Graham Upside"),
                }.items()
                if v is not None
            }

            result["price_stats"] = {
                k: v
                for k, v in {
                    "beta": _s("Stock Price Statistics", "Beta (5Y)"),
                    "52w_change": _s("Stock Price Statistics", "52-Week Price Change"),
                    "sma_50": _s("Stock Price Statistics", "50-Day Moving Average"),
                    "sma_200": _s("Stock Price Statistics", "200-Day Moving Average"),
                    "rsi": _s(
                        "Stock Price Statistics", "Relative Strength Index (RSI)"
                    ),
                }.items()
                if v is not None
            }

        result["growth_trends"] = _parse_growth_trends(t.financials())  # ticker method
        result["ratio_trends"] = _parse_ratio_trends(t.ratios())  # ticker method

        return result

    def _build_sparkline(
        self,
        ticker: str,
        prices: pd.DataFrame,
        today: date,
    ) -> list[dict]:
        if ticker not in prices.columns:
            return []
        cutoff = pd.Timestamp(today - timedelta(days=30))
        series = prices[ticker][prices.index >= cutoff].dropna()
        return [
            {"date": ts.strftime("%Y-%m-%d"), "close": round(float(v), 4)}
            for ts, v in series.items()
        ]


# Module-level helpers (financials / ratios parsing unchanged)
def _clean_str(v) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return None if s in ("", "-", "n/a", "N/A", "None") else s


def _parse_growth_trends(financials_df: pd.DataFrame) -> dict:
    WANTED = {
        "Revenue": "revenue",
        "Revenue Growth (YoY)": "revenue_growth",
        "Net Income": "net_income",
        "Net Income Growth": "net_income_growth",
        "Operating Income": "operating_income",
        "EBITDA": "ebitda",
        "Free Cash Flow": "fcf",
        "EPS (Diluted)": "eps_diluted",
        "Profit Margin": "profit_margin",
        "EBITDA Margin": "ebitda_margin",
        "Free Cash Flow Margin": "fcf_margin",
        "Dividend Per Share": "dividend_per_share",
    }
    if financials_df is None or (
        hasattr(financials_df, "empty") and financials_df.empty
    ):
        return {}
    out = {}
    for label, key in WANTED.items():
        if label in financials_df.index:
            row = financials_df.loc[label]
            out[key] = {
                col: _clean_str(val)
                for col, val in row.items()
                if str(col).startswith("FY") and val not in (None, "-", "")
            }
    return {k: v for k, v in out.items() if v}


def _parse_ratio_trends(ratios_df: pd.DataFrame) -> dict:
    WANTED = {
        "PE Ratio": "pe_ratio",
        "PS Ratio": "ps_ratio",
        "PB Ratio": "pb_ratio",
        "EV/EBITDA Ratio": "ev_ebitda",
        "Return on Equity (ROE)": "roe",
        "Return on Assets (ROA)": "roa",
        "Debt / Equity Ratio": "debt_equity",
        "Current Ratio": "current_ratio",
        "Dividend Yield": "dividend_yield",
        "Payout Ratio": "payout_ratio",
    }
    if ratios_df is None or (hasattr(ratios_df, "empty") and ratios_df.empty):
        return {}
    out = {}
    for label, key in WANTED.items():
        if label in ratios_df.index:
            row = ratios_df.loc[label]
            out[key] = {
                col: _clean_str(val)
                for col, val in row.items()
                if col not in (None, "") and val not in (None, "-", "")
            }
    return {k: v for k, v in out.items() if v}
