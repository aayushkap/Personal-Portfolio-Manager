# app/services/holdings.py

from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

import pandas as pd

from app.core.logger import get_logger
from app.services.base import BaseModule
from app.services.filters import DateRange, PortfolioFilters
from app.services.overlays import OverlayResolver, OVERLAY_CATALOGUE
from app.utils.fin import safe_float as _safe
from app.services.holdings_news import HoldingsNewsAgent


from app.utils.parsers import (
    parse_number,
    parse_percent,
)

logger = get_logger()
pd.set_option("display.max_rows", None)


def _earnings_nearby(earnings_date, today: date) -> bool:
    return earnings_date is not None and abs((earnings_date - today).days) <= 2


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

        results = sorted(results, key=lambda x: x["total_value"] or 0, reverse=True)
        results = HoldingsNewsAgent().merge_news(results)

        return results

    def get_holding_detail(
        self,
        ticker: str,
        timeframe: str = "1m",
        overlays: Optional[list[str]] = None,
        filters: Optional[PortfolioFilters] = None,
    ) -> dict:
        p = self.hql.portfolio()
        today = date.today()
        info = self.hql.ticker(ticker).info()
        overlay_map = self._build_overlays(ticker, timeframe, today, overlays or [])

        return {
            "ticker": ticker,
            "chart": self._build_chart(ticker, timeframe, today),
            "overlays": overlay_map,
            "transactions": self._build_transactions(ticker, today, p),
            "fundamentals": self._build_fundamentals(ticker),
            "last_updated": info.get("last_updated"),
            "news": HoldingsNewsAgent().merge_news([{"ticker": ticker}]),
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
        earnings_date = t.overview().get("earnings_date")

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
            "earnings_nearby": _earnings_nearby(earnings_date, today),
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

    def _build_overlays(
        self,
        ticker: str,
        timeframe: str,
        today: date,
        overlays: list[str],
    ) -> dict[str, list[dict]]:
        if not overlays:
            return {}

        config = HoldingsModule._timeframe_config(timeframe)
        start = (
            (today - timedelta(days=config["days_back"]))
            if config["days_back"]
            else date(2000, 1, 1)
        )
        end = today
        range_filters = PortfolioFilters(date_range=DateRange(start=start, end=end))

        resolver = OverlayResolver(self)
        result: dict[str, list[dict]] = {}

        for key in overlays:
            normalized = key.upper()
            if normalized in OVERLAY_CATALOGUE:
                series = resolver.resolve(normalized, range_filters)
                result[normalized] = HoldingsModule._series_to_records(
                    series, config["granularity"]
                )
                continue

            series = HoldingsModule._overlay_ticker_series(
                self, normalized, start, end, config["granularity"]
            )
            if not series.empty:
                result[normalized] = HoldingsModule._series_to_records(
                    series, config["granularity"]
                )

        return result

    @staticmethod
    def _timeframe_config(timeframe: str) -> dict:
        mapping = {
            "1d": {"granularity": "15min", "days_back": 1},
            "1w": {"granularity": "30min", "days_back": 7},
            "1m": {"granularity": "60min", "days_back": 30},
            "3m": {"granularity": "1D", "days_back": 90},
            "6m": {"granularity": "1D", "days_back": 180},
            "1y": {"granularity": "1D", "days_back": 365},
            "5y": {"granularity": "1D", "days_back": 365 * 5},
            "all": {"granularity": "1D", "days_back": None},
        }
        return mapping.get(timeframe, mapping["1m"])

    @staticmethod
    def _series_to_records(series: pd.Series, granularity: str) -> list[dict]:
        if series.empty:
            return []
        if series.index.tz is not None:
            series.index = series.index.tz_localize(None)
        if granularity == "1D":
            series.index = series.index.normalize()
        series = series.sort_index()
        return [
            {
                "date": ts.strftime(
                    "%Y-%m-%dT%H:%M" if granularity != "1D" else "%Y-%m-%d"
                ),
                "value": _safe(round(float(val), 4)) if pd.notna(val) else None,
            }
            for ts, val in series.items()
        ]

    def _overlay_ticker_series(
        self, ticker: str, start: date, end: date, granularity: str
    ) -> pd.Series:
        t = self.hql.ticker(ticker)
        df = t.prices(start=start, end=end, granularity=granularity)
        if df is None or (hasattr(df, "empty") and df.empty):
            return pd.Series(dtype=float, name=ticker)
        if isinstance(df, pd.DataFrame):
            if "close" not in df.columns:
                return pd.Series(dtype=float, name=ticker)
            s = df["close"].rename(ticker)
        else:
            s = df.rename(ticker)
        if s.index.tz is not None:
            s.index = s.index.tz_localize(None)
        return s

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
        t = self.hql.ticker(ticker)

        ov = t.overview()
        if ov:
            if ov.get("about"):
                result["about"] = ov["about"]
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
                    "dividend_per_share": ov.get("dividend_per_share"),
                    "ex_dividend_date": ov.get("ex_dividend_date"),
                    "earnings_date": ov.get("earnings_date"),
                    "analyst_rating": ov.get("analyst_rating"),
                    "price_target": ov.get("price_target"),
                    "price_target_upside": ov.get("price_target_upside"),
                    "shares_out": ov.get("shares_out"),
                    "revenue_ttm": ov.get("revenue_ttm"),
                    "net_income": ov.get("net_income"),
                }.items()
                if v is not None
            }

        stats = t.statistics()
        if stats:

            def _s(section, key):
                return stats.get(section, {}).get(key)

            result["valuation"] = {
                k: v
                for k, v in {
                    "pe_ratio": _s("Valuation Ratios", "PE Ratio"),
                    "forward_pe": _s("Valuation Ratios", "Forward PE"),
                    "ps_ratio": _s("Valuation Ratios", "PS Ratio"),
                    "pb_ratio": _s("Valuation Ratios", "PB Ratio"),
                    "peg_ratio": _s("Valuation Ratios", "PEG Ratio"),
                    "p_fcf": _s("Valuation Ratios", "P/FCF Ratio"),
                    "p_ocf": _s("Valuation Ratios", "P/OCF Ratio"),
                }.items()
                if v is not None
            }

            result["efficiency"] = {
                k: v
                for k, v in {
                    "roe": _s("Financial Efficiency", "Return on Equity (ROE)"),
                    "roa": _s("Financial Efficiency", "Return on Assets (ROA)"),
                    "roic": _s(
                        "Financial Efficiency", "Return on Invested Capital (ROIC)"
                    ),
                    "roce": _s(
                        "Financial Efficiency", "Return on Capital Employed (ROCE)"
                    ),
                    "wacc": _s(
                        "Financial Efficiency",
                        "Weighted Average Cost of Capital (WACC)",
                    ),
                    "asset_turnover": _s("Financial Efficiency", "Asset Turnover"),
                    "employees": _s("Financial Efficiency", "Employee Count"),
                }.items()
                if v is not None
            }

            result["margins"] = {
                k: v
                for k, v in {
                    "gross_margin": _s("Margins", "Gross Margin"),
                    "operating_margin": _s("Margins", "Operating Margin"),
                    "profit_margin": _s("Margins", "Profit Margin"),
                    "ebitda_margin": _s("Margins", "EBITDA Margin"),
                    "fcf_margin": _s("Margins", "FCF Margin"),
                }.items()
                if v is not None
            }

            result["balance_sheet"] = {
                k: v
                for k, v in {
                    "total_debt": _s("Balance Sheet", "Total Debt"),
                    "net_cash": _s("Balance Sheet", "Net Cash"),
                    "net_cash_ps": _s("Balance Sheet", "Net Cash Per Share"),
                    "book_value": _s("Balance Sheet", "Equity (Book Value)"),
                    "book_value_ps": _s("Balance Sheet", "Book Value Per Share"),
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

            result["dividends_yields"] = {
                k: v
                for k, v in {
                    "dividend_per_share": _s(
                        "Dividends & Yields", "Dividend Per Share"
                    ),
                    "dividend_yield": _s("Dividends & Yields", "Dividend Yield"),
                    "payout_ratio": _s("Dividends & Yields", "Payout Ratio"),
                    "earnings_yield": _s("Dividends & Yields", "Earnings Yield"),
                    "fcf_yield": _s("Dividends & Yields", "FCF Yield"),
                }.items()
                if v is not None
            }

        result["growth_trends"] = _parse_growth_trends(t.financials())
        result["ratio_trends"] = _parse_ratio_trends(t.ratios())

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
    PCT_KEYS = {
        "revenue_growth",
        "net_income_growth",
        "profit_margin",
        "ebitda_margin",
        "fcf_margin",
    }

    if financials_df is None or (
        hasattr(financials_df, "empty") and financials_df.empty
    ):
        return {}

    out = {}
    for label, key in WANTED.items():
        if label not in financials_df.index:
            continue
        row = financials_df.loc[label]
        parsed = {}
        for col, val in row.items():
            if not str(col).startswith("FY"):
                continue
            s = _clean_str(str(val) if val is not None else None)
            if not s or s == "-":
                continue
            n = parse_percent(s) if key in PCT_KEYS else parse_number(s)
            if n is not None:
                parsed[col] = n
        if parsed:
            out[key] = parsed

    return out


def _parse_ratio_trends(ratios_df: pd.DataFrame) -> dict:
    WANTED = {
        "PE Ratio": "pe_ratio",
        "PS Ratio": "ps_ratio",
        "PB Ratio": "pb_ratio",
        "EV/EBITDA Ratio": "ev_ebitda",
        "Return on Equity (ROE)": "roe",
        "Return on Assets (ROA)": "roa",
        "Return on Invested Capital (ROIC)": "roic",
        "Return on Capital Employed (ROCE)": "roce",
        "Debt / Equity Ratio": "debt_equity",
        "Current Ratio": "current_ratio",
        "Dividend Yield": "dividend_yield",
        "Payout Ratio": "payout_ratio",
        "Earnings Yield": "earnings_yield",
        "FCF Yield": "fcf_yield",
    }
    PCT_KEYS = {
        "roe",
        "roa",
        "roic",
        "roce",
        "dividend_yield",
        "payout_ratio",
        "earnings_yield",
        "fcf_yield",
    }

    if ratios_df is None or (hasattr(ratios_df, "empty") and ratios_df.empty):
        return {}

    out = {}
    for label, key in WANTED.items():
        if label not in ratios_df.index:
            continue
        row = ratios_df.loc[label]
        parsed = {}
        for col, val in row.items():
            if col in (None, "", "Fiscal Year"):
                continue
            s = _clean_str(str(val) if val is not None else None)
            if not s or s == "-":
                continue
            n = parse_percent(s) if key in PCT_KEYS else parse_number(s)
            if n is not None:
                parsed[col] = n
        if parsed:
            out[key] = parsed

    return out
