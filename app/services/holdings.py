# app/services/holdings.py

from __future__ import annotations

from datetime import date, timedelta
from typing import Optional
import math

import pandas as pd

from app.core.logger import get_logger
from app.services.base import BaseModule
from app.services.filters import PortfolioFilters

logger = get_logger()


def _safe(v):
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v


def _pct_change(new: Optional[float], old: Optional[float]) -> Optional[float]:
    if new is None or old is None or old == 0:
        return None
    return round((new - old) / old * 100, 2)


class HoldingsModule(BaseModule):
    # PUBLIC: Holdings list (cards)
    def get_holdings_list(self, filters: PortfolioFilters) -> list[dict]:
        tx = self.apply_filters(self.get_all_transactions(), filters)
        if tx.empty:
            return []

        today = date.today()
        tickers = self._active_tickers(tx, today)
        if not tickers:
            return []

        # Batch-fetch prices for sparkline window + change periods (90d)
        lookback_start = today - timedelta(days=120)
        prices = self.get_price_series(tickers, lookback_start, today).ffill()

        results = []
        for ticker in tickers:
            card = self._build_card(ticker, tx, prices, today)
            if card:
                results.append(card)

        return sorted(results, key=lambda x: x["total_value"] or 0, reverse=True)

    # PUBLIC: Holding detail (on card click)
    def get_holding_detail(
        self,
        ticker: str,
        timeframe: str = "1m",  # "1d" | "1w" | "1m" | "3m" | "all"
        filters: Optional[PortfolioFilters] = None,
    ) -> dict:
        tx_all = self.get_all_transactions()
        tx = tx_all[tx_all["ticker"] == ticker].copy()
        if tx.empty:
            return {}

        today = date.today()

        return {
            "ticker": ticker,
            "chart": self._build_chart(ticker, timeframe, today),
            "transactions": self._build_transactions(ticker, tx, today),
            "fundamentals": self._build_fundamentals(ticker),
        }

    # Card builder
    def _build_card(
        self,
        ticker: str,
        tx: pd.DataFrame,
        prices: pd.DataFrame,
        today: date,
    ) -> Optional[dict]:
        ticker_tx = tx[tx["ticker"] == ticker]

        # Shares (net buys - sells)
        net_shares = ticker_tx["signed_shares"].sum()
        if net_shares <= 0:
            return None  # fully exited position

        # Cost basis (cash deployed net of sells)
        bought = ticker_tx[ticker_tx["action"] == "BUY"]["total_cost"].sum()
        sold = ticker_tx[ticker_tx["action"] == "SELL"]["total_cost"].sum()
        cost_basis = round(bought - sold, 2)

        # Current price + market value
        current_price = self.get_latest_price(ticker)
        total_value = round(net_shares * current_price, 2) if current_price else None

        # Price changes: DoD, MoM, 3M
        price_col = prices.get(ticker) if isinstance(prices, pd.DataFrame) else None

        def _price_n_days_ago(n: int) -> Optional[float]:
            if price_col is None or price_col.empty:
                return None
            cutoff = pd.Timestamp(today - timedelta(days=n), tz="Asia/Dubai")
            past = price_col[price_col.index <= cutoff]
            return float(past.iloc[-1]) if not past.empty else None

        price_today = current_price
        price_yesterday = _price_n_days_ago(1)
        price_1m = _price_n_days_ago(30)
        price_3m = _price_n_days_ago(90)

        dod = _pct_change(price_today, price_yesterday)
        mom = _pct_change(price_today, price_1m)
        m3 = _pct_change(price_today, price_3m)

        # Cumulative dividends (all-time for this ticker)
        cum_divs = self._ticker_dividends_received(ticker, ticker_tx, today)

        # Total return = (market value + dividends) - cost basis
        total_return = (
            round(total_value + cum_divs - cost_basis, 2)
            if total_value is not None
            else None
        )

        # YoC: all-time dividends / cost basis * 100
        yoc = round(cum_divs / cost_basis * 100, 2) if cost_basis > 0 else 0.0

        # Sparkline: last 1M of closes (simple list)
        sparkline = self._build_sparkline(ticker, prices, today)

        # Meta from cache
        meta = self._ticker_meta(ticker)

        return {
            "ticker": ticker,
            "name": meta.get("name", ticker),
            "sector": meta.get("sector"),
            "exchange": meta.get("exchange"),
            "logo_url": meta.get("logo_url"),
            "shares": round(net_shares, 6),
            "current_price": _safe(current_price),
            "cost_basis": _safe(cost_basis),
            "total_value": _safe(total_value),
            "total_return": _safe(total_return),
            "total_return_pct": _safe(
                _pct_change(total_value + cum_divs if total_value else None, cost_basis)
            ),
            "dod_pct": _safe(dod),
            "mom_pct": _safe(mom),
            "three_month_pct": _safe(m3),
            "cumulative_divs": round(cum_divs, 2),
            "yoc_pct": _safe(yoc),
            "sparkline": sparkline,
        }

    # Detail builders
    def _build_chart(
        self,
        ticker: str,
        timeframe: str,
        today: date,
    ) -> list[dict]:
        """Return LCV (last/close/volume) bars for the given timeframe."""

        _CHART_CONFIG = {
            "1d": {"interval_mins": 15, "days_back": 1},
            "1w": {"interval_mins": 30, "days_back": 7},
            "1m": {"interval_mins": 60, "days_back": 30},  # daily
            "3m": {"interval_mins": 1440, "days_back": 90},
            "all": {"interval_mins": 1440, "days_back": None},  # full history
        }

        config = _CHART_CONFIG.get(timeframe, _CHART_CONFIG["1m"])
        interval = config["interval_mins"]
        days_back = config["days_back"]
        start = (today - timedelta(days=days_back)) if days_back else date(2000, 1, 1)

        rows = self._db.get(ticker, limit=50_000)
        if not rows:
            return []

        df = pd.DataFrame(rows)
        df["ts"] = pd.to_datetime(df["timestamp"]).dt.tz_convert("Asia/Dubai")
        df = df[(df["ts"].dt.date >= start) & (df["ts"].dt.date <= today)]
        df = df.sort_values("ts")

        if df.empty:
            return []

        if interval == 1440:
            # Daily — one bar per session (last close of day)
            df["bucket"] = df["ts"].dt.normalize()
        else:
            # Intraday — floor to interval boundary
            df["bucket"] = df["ts"].dt.floor(f"{interval}min")

        agg = (
            df.groupby("bucket")
            .agg(
                close=("close", "last"),
                volume=("volume", "sum"),
            )
            .reset_index()
        )

        # Format label — intraday keeps time, daily is date-only
        date_fmt = "%Y-%m-%d" if interval == 1440 else "%Y-%m-%dT%H:%M"

        return [
            {
                "date": row["bucket"].strftime(date_fmt),
                "close": _safe(round(row["close"], 4)),
                "volume": int(row["volume"]) if pd.notna(row["volume"]) else None,
            }
            for _, row in agg.iterrows()
        ]

    def _build_transactions(
        self,
        ticker: str,
        tx: pd.DataFrame,
        today: date,
    ) -> list[dict]:
        """Buys, sells, and dividends received — chronological."""
        records = []

        # Buys & sells
        for _, row in tx.sort_values("trade_date").iterrows():
            records.append(
                {
                    "date": (
                        row["trade_date"].isoformat()
                        if hasattr(row["trade_date"], "isoformat")
                        else str(row["trade_date"])
                    ),
                    "type": row["action"],  # "BUY" | "SELL"
                    "shares": round(float(row["shares"]), 6),
                    "price": _safe(round(float(row["price"]), 4)),
                    "commission": _safe(round(float(row["commission"]), 2)),
                    "total": _safe(round(float(row["total_cost"]), 2)),
                }
            )

        # Dividends received
        for div in self.get_dividends(ticker):
            if not div.pay_date or not div.cash_amount or div.pay_date > today:
                continue
            holdings = self.get_holdings(div.ex_date, [ticker], tx)
            shares = holdings.get(ticker, 0.0)
            if shares <= 0:
                continue
            try:
                amount = round(float(div.cash_amount.split()[0]) * shares, 2)
                records.append(
                    {
                        "date": div.pay_date.isoformat(),
                        "type": "DIVIDEND",
                        "shares": round(shares, 6),
                        "price": _safe(float(div.cash_amount.split()[0])),
                        "commission": 0.0,
                        "total": amount,
                    }
                )
            except Exception:
                pass

        return sorted(records, key=lambda x: x["date"])

    def _build_fundamentals(self, ticker: str) -> dict:
        data = self.get_ticker(ticker)
        if not data:
            return {}

        result = {}

        #  1. Overview stats (quick snapshot)
        if data.overview and data.overview.stats:
            s = data.overview.stats
            result["snapshot"] = {
                k: v
                for k, v in {
                    "market_cap": _clean_str(getattr(s, "market_cap", None)),
                    "pe_ratio": _clean_str(getattr(s, "pe_ratio", None)),
                    "forward_pe": _clean_str(getattr(s, "forward_pe", None)),
                    "eps": _clean_str(getattr(s, "eps", None)),
                    "dividend": _clean_str(getattr(s, "dividend", None)),
                    "ex_dividend_date": _clean_str(
                        getattr(s, "ex_dividend_date", None)
                    ),
                    "earnings_date": _clean_str(getattr(s, "earnings_date", None)),
                    "week_52_range": _clean_str(getattr(s, "week_52_range", None)),
                    "beta": _clean_str(getattr(s, "beta", None)),
                    "volume": _clean_str(getattr(s, "volume", None)),
                    "avg_volume": _clean_str(
                        getattr(s, "Average Volume", None)
                        or getattr(s, "average_volume", None)
                    ),
                    "revenue_ttm": _clean_str(
                        getattr(s, "Revenue (ttm)", None)
                        or getattr(s, "revenue_ttm", None)
                    ),
                    "net_income": _clean_str(
                        getattr(s, "Net Income", None) or getattr(s, "net_income", None)
                    ),
                    "shares_out": _clean_str(
                        getattr(s, "Shares Out", None) or getattr(s, "shares_out", None)
                    ),
                    "rsi": _clean_str(
                        getattr(s, "RSI", None) or getattr(s, "rsi", None)
                    ),
                }.items()
                if v is not None
            }

        # 2. Statistics sections (key ratios)
        if data.statistics and data.statistics.sections:
            sec = data.statistics.sections

            val = sec.valuation_ratios
            if val:
                result["valuation"] = {
                    k: v
                    for k, v in {
                        "pe_ratio": _clean_str(getattr(val, "pe_ratio", None)),
                        "forward_pe": _clean_str(getattr(val, "forward_pe", None)),
                        "ps_ratio": _clean_str(getattr(val, "ps_ratio", None)),
                        "pb_ratio": _clean_str(getattr(val, "pb_ratio", None)),
                        "peg_ratio": _clean_str(getattr(val, "peg_ratio", None)),
                        "p_fcf": _clean_str(
                            getattr(val, "P/FCF Ratio", None)
                            or getattr(val, "p_fcf_ratio", None)
                        ),
                        "p_ocf": _clean_str(
                            getattr(val, "P/OCF Ratio", None)
                            or getattr(val, "p_ocf_ratio", None)
                        ),
                    }.items()
                    if v is not None
                }

            div = sec.dividends_and_yields
            if div:
                result["dividends_and_yields"] = {
                    k: v
                    for k, v in {
                        "dividend_per_share": _clean_str(
                            getattr(div, "dividend_per_share", None)
                        ),
                        "dividend_yield": _clean_str(
                            getattr(div, "dividend_yield", None)
                        ),
                        "dividend_growth_yoy": _clean_str(
                            getattr(div, "dividend_growth_yoy", None)
                        ),
                        "payout_ratio": _clean_str(getattr(div, "payout_ratio", None)),
                        "fcf_yield": _clean_str(
                            getattr(div, "FCF Yield", None)
                            or getattr(div, "fcf_yield", None)
                        ),
                        "earnings_yield": _clean_str(
                            getattr(div, "earnings_yield", None)
                            or getattr(div, "Earnings Yield", None)
                        ),
                    }.items()
                    if v is not None
                }

            eff = sec.financial_efficiency
            if eff:
                result["efficiency"] = {
                    k: v
                    for k, v in {
                        "roe": _clean_str(getattr(eff, "roe", None)),
                        "roa": _clean_str(getattr(eff, "roa", None)),
                        "roic": _clean_str(getattr(eff, "roic", None)),
                        "wacc": _clean_str(getattr(eff, "wacc", None)),
                        "roce": _clean_str(
                            getattr(eff, "Return on Capital Employed", None)
                            or getattr(eff, "roce", None)
                        ),
                        "asset_turnover": _clean_str(
                            getattr(eff, "Asset Turnover", None)
                            or getattr(eff, "asset_turnover", None)
                        ),
                        "inventory_turnover": _clean_str(
                            getattr(eff, "Inventory Turnover", None)
                            or getattr(eff, "inventory_turnover", None)
                        ),
                        "employees": _clean_str(
                            getattr(eff, "Employee Count", None)
                            or getattr(eff, "employee_count", None)
                        ),
                    }.items()
                    if v is not None
                }

        #  3. Statistics raw dict (deep metrics)
        if (
            data.statistics
            and data.statistics.sections
            and data.statistics.sections.raw
        ):
            raw = data.statistics.sections.raw

            def _r(section: str, key: str):
                return raw.get(section, {}).get(key)

            result["balance_sheet"] = {
                k: v
                for k, v in {
                    "total_assets": _r("Balance Sheet", "Total Assets"),
                    "total_debt": _r("Balance Sheet", "Total Debt"),
                    "net_cash": _r("Balance Sheet", "Net Cash"),
                    "net_cash_ps": _r("Balance Sheet", "Net Cash Per Share"),
                    "book_value": _r("Balance Sheet", "Equity (Book Value)"),
                    "book_value_ps": _r("Balance Sheet", "Book Value Per Share"),
                    "working_capital": _r("Balance Sheet", "Working Capital"),
                }.items()
                if v is not None
            }

            result["income_statement"] = {
                k: v
                for k, v in {
                    "revenue": _r("Income Statement", "Revenue"),
                    "gross_profit": _r("Income Statement", "Gross Profit"),
                    "operating_income": _r("Income Statement", "Operating Income"),
                    "pretax_income": _r("Income Statement", "Pretax Income"),
                    "net_income": _r("Income Statement", "Net Income"),
                    "ebitda": _r("Income Statement", "EBITDA"),
                    "ebit": _r("Income Statement", "EBIT"),
                    "eps": _r("Income Statement", "EPS (Diluted)"),
                    "shares_out": _r(
                        "Income Statement", "Shares Outstanding (Diluted)"
                    ),
                }.items()
                if v is not None
            }

            result["cash_flow"] = {
                k: v
                for k, v in {
                    "operating_cf": _r("Cash Flow", "Operating Cash Flow"),
                    "capex": _r("Cash Flow", "Capital Expenditures"),
                    "free_cash_flow": _r("Cash Flow", "Free Cash Flow"),
                    "fcf_per_share": _r("Cash Flow", "FCF Per Share"),
                    "net_borrowing": _r("Cash Flow", "Net Borrowing"),
                    "da": _r("Cash Flow", "Depreciation & Amortization"),
                }.items()
                if v is not None
            }

            result["margins"] = {
                k: v
                for k, v in {
                    "gross_margin": _r("Margins", "Gross Margin"),
                    "operating_margin": _r("Margins", "Operating Margin"),
                    "profit_margin": _r("Margins", "Profit Margin"),
                    "ebitda_margin": _r("Margins", "EBITDA Margin"),
                    "fcf_margin": _r("Margins", "FCF Margin"),
                    "pretax_margin": _r("Margins", "Pretax Margin"),
                }.items()
                if v is not None
            }

            result["financial_position"] = {
                k: v
                for k, v in {
                    "current_ratio": _r("Financial Position", "Current Ratio"),
                    "quick_ratio": _r("Financial Position", "Quick Ratio"),
                    "debt_equity": _r("Financial Position", "Debt / Equity"),
                    "debt_ebitda": _r("Financial Position", "Debt / EBITDA"),
                    "debt_fcf": _r("Financial Position", "Debt / FCF"),
                    "interest_coverage": _r("Financial Position", "Interest Coverage"),
                }.items()
                if v is not None
            }

            result["ev_multiples"] = {
                k: v
                for k, v in {
                    "enterprise_value": _r("Total Valuation", "Enterprise Value"),
                    "ev_earnings": _r("Enterprise Valuation", "EV / Earnings"),
                    "ev_sales": _r("Enterprise Valuation", "EV / Sales"),
                    "ev_ebitda": _r("Enterprise Valuation", "EV / EBITDA"),
                    "ev_ebit": _r("Enterprise Valuation", "EV / EBIT"),
                    "ev_fcf": _r("Enterprise Valuation", "EV / FCF"),
                }.items()
                if v is not None
            }

            result["fair_value"] = {
                k: v
                for k, v in {
                    "graham_number": _r("Fair Value", "Graham Number"),
                    "graham_upside": _r("Fair Value", "Graham Upside"),
                    "lynch_fair_value": _r("Fair Value", "Lynch Fair Value"),
                    "lynch_upside": _r("Fair Value", "Lynch Upside"),
                }.items()
                if v is not None
            }

            result["scores"] = {
                k: v
                for k, v in {
                    "altman_z": _r("Scores", "Altman Z-Score"),
                    "piotroski_f": _r("Scores", "Piotroski F-Score"),
                }.items()
                if v is not None
            }

            result["price_stats"] = {
                k: v
                for k, v in {
                    "beta": _r("Stock Price Statistics", "Beta (5Y)"),
                    "52w_change": _r("Stock Price Statistics", "52-Week Price Change"),
                    "sma_50": _r("Stock Price Statistics", "50-Day Moving Average"),
                    "sma_200": _r("Stock Price Statistics", "200-Day Moving Average"),
                    "rsi": _r(
                        "Stock Price Statistics", "Relative Strength Index (RSI)"
                    ),
                    "avg_volume": _r(
                        "Stock Price Statistics", "Average Volume (20 Days)"
                    ),
                }.items()
                if v is not None
            }

            result["ownership"] = {
                k: v
                for k, v in {
                    "shares_outstanding": _r("Share Statistics", "Shares Outstanding"),
                    "institutional_pct": _r(
                        "Share Statistics", "Owned by Institutions (%)"
                    ),
                    "float": _r("Share Statistics", "Float"),
                }.items()
                if v is not None
            }

        #  4. Multi-year growth trends (from financials + ratios tabular)
        result["growth_trends"] = _parse_growth_trends(data.financials)
        result["ratio_trends"] = _parse_ratio_trends(data.ratios)

        return result

    # Helpers
    def _active_tickers(self, tx: pd.DataFrame, as_of: date) -> list[str]:
        mask = pd.to_datetime(tx["trade_date"]).dt.date <= as_of
        held = tx[mask].groupby("ticker")["signed_shares"].sum()
        return held[held > 0].index.tolist()

    def _build_sparkline(
        self,
        ticker: str,
        prices: pd.DataFrame,
        today: date,
    ) -> list[dict]:
        if ticker not in prices.columns:
            return []
        cutoff = pd.Timestamp(today - timedelta(days=30), tz="Asia/Dubai")
        series = prices[ticker][prices.index >= cutoff].dropna()
        return [
            {"date": ts.strftime("%Y-%m-%d"), "close": round(float(v), 4)}
            for ts, v in series.items()
        ]

    def _ticker_dividends_received(
        self,
        ticker: str,
        tx: pd.DataFrame,
        as_of: date,
    ) -> float:
        total = 0.0
        for div in self.get_dividends(ticker):
            if not div.pay_date or div.pay_date > as_of or not div.cash_amount:
                continue
            holdings = self.get_holdings(div.ex_date, [ticker], tx)
            shares = holdings.get(ticker, 0.0)
            if shares > 0:
                try:
                    total += float(div.cash_amount.split()[0]) * shares
                except Exception:
                    pass
        return total

    def _ticker_meta(self, ticker: str) -> dict:
        data = self.get_ticker(ticker)
        if not data:
            return {}
        # Pull from purchase_details for exchange/sector/logo
        # (already stored per-transaction, just take first)
        if data.purchase_details:
            pd_row = data.purchase_details[0]
            return {
                "name": getattr(pd_row, "name", None) or ticker,
                "sector": getattr(pd_row, "sector", None),
                "exchange": getattr(pd_row, "exchange", None),
                "logo_url": getattr(pd_row, "logo_url", None),
            }
        return {"name": ticker}

    @staticmethod
    def _timeframe_start(timeframe: str, today: date) -> date:
        return {
            "1d": today - timedelta(days=1),
            "1w": today - timedelta(weeks=1),
            "1m": today - timedelta(days=30),
            "3m": today - timedelta(days=90),
            "all": date(2000, 1, 1),
        }.get(timeframe, today - timedelta(days=30))


#  Helpers
def _clean_str(v) -> str | None:
    """Return string value or None — strips empty/n/a."""
    if v is None:
        return None
    s = str(v).strip()
    return None if s in ("", "-", "n/a", "N/A", "None") else s


def _parse_growth_trends(financials) -> dict:
    """
    Extract key multi-year metrics from financials tabular rows.
    Returns dict of metric → {FY2025, FY2024, FY2023, ...}
    """
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

    if not financials or not financials.rows:
        return {}

    years = [h for h in (financials.headers or []) if h.startswith("FY")]
    out = {}

    for row in financials.rows:
        label = row.get("Fiscal Year", "")
        key = WANTED.get(label)
        if not key:
            continue
        out[key] = {
            yr: _clean_str(row.get(yr))
            for yr in years
            if row.get(yr) not in (None, "-", "")
        }

    return out


def _parse_ratio_trends(ratios) -> dict:
    """
    Extract valuation + efficiency ratios across years from ratios tabular rows.
    """
    WANTED = {
        "PE Ratio": "pe_ratio",
        "PS Ratio": "ps_ratio",
        "PB Ratio": "pb_ratio",
        "EV/EBITDA Ratio": "ev_ebitda",
        "Return on Equity (ROE)": "roe",
        "Return on Assets (ROA)": "roa",
        "Return on Capital Employed (ROCE)": "roce",
        "Debt / Equity Ratio": "debt_equity",
        "Debt / EBITDA Ratio": "debt_ebitda",
        "Current Ratio": "current_ratio",
        "Dividend Yield": "dividend_yield",
        "Payout Ratio": "payout_ratio",
        "FCF Yield": "fcf_yield",
        "Earnings Yield": "earnings_yield",
    }

    if not ratios or not ratios.rows:
        return {}

    years = [h for h in (ratios.headers or []) if h.startswith("FY") or h == "Current"]
    out = {}

    for row in ratios.rows:
        label = row.get("Fiscal Year", "")
        key = WANTED.get(label)
        if not key:
            continue
        out[key] = {
            yr: _clean_str(row.get(yr))
            for yr in years
            if row.get(yr) not in (None, "-", "")
        }

    return out
