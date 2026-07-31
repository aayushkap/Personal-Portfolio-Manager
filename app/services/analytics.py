# app/services/analytics.py

from __future__ import annotations

import calendar
from datetime import date, timedelta
from typing import Literal

import pandas as pd
from scipy.optimize import brentq

from app.config import BENCHMARKS
from app.core.logger import get_logger
from app.services.base import BaseModule
from app.services.filters import PortfolioFilters

logger = get_logger()


# The cache uses TradingView-style exchange prefixes.  A benchmark is assigned
# from the exchange rather than from a hard-coded list of portfolio tickers, so
# newly added holdings participate automatically.
EXCHANGE_BENCHMARKS: dict[str, str] = {
    "DFM": "DFM:DFMGI",
    "ADX": "ADX:FADGI",
    "LSE": "FTSE-UKX",
    "FTSE": "FTSE-UKX",
    "LON": "FTSE-UKX",
    "NYSE": "TVC:SPX",
    "NASDAQ": "TVC:SPX",
    "AMEX": "TVC:SPX",
    "NYSEARCA": "TVC:SPX",
    "ARCA": "TVC:SPX",
    "BATS": "TVC:SPX",
    "CBOE": "TVC:SPX",
}


def _benchmark_for_exchange(exchange: object) -> str | None:
    value = str(exchange or "").strip().upper()
    return EXCHANGE_BENCHMARKS.get(value)


def _annualize(total_return: float, days: int) -> float:
    if days <= 0:
        return total_return
    if total_return <= -1:
        return -1.0
    return (1.0 + total_return) ** (365.0 / days) - 1.0


def _xnpv(rate: float, cash_flows: list[tuple[date, float]]) -> float:
    origin = cash_flows[0][0]
    return sum(
        amount / (1.0 + rate) ** ((when - origin).days / 365.0)
        for when, amount in cash_flows
    )


def _xirr(cash_flows: list[tuple[date, float]]) -> float | None:
    """Return an annualized money-weighted return, or None if not solvable."""
    flows = [(when, float(amount)) for when, amount in cash_flows if amount]
    if (
        not flows
        or not any(amount < 0 for _, amount in flows)
        or not any(amount > 0 for _, amount in flows)
    ):
        return None

    lower = -0.999999
    upper = 1.0
    lower_value = _xnpv(lower, flows)
    upper_value = _xnpv(upper, flows)
    while lower_value * upper_value > 0 and upper < 1_000_000:
        upper = (upper + 1.0) * 2.0 - 1.0
        upper_value = _xnpv(upper, flows)

    if lower_value * upper_value > 0:
        return None
    try:
        return float(brentq(lambda rate: _xnpv(rate, flows), lower, upper))
    except (ValueError, ZeroDivisionError):
        return None


def _date_index(values: pd.Series | pd.DataFrame) -> pd.DatetimeIndex:
    index = pd.to_datetime(values.index)
    if index.tz is not None:
        index = index.tz_localize(None)
    return index.normalize()


def _snap_to_index(when: date, index: pd.DatetimeIndex) -> pd.Timestamp | None:
    candidates = index[index >= pd.Timestamp(when)]
    return candidates.min() if len(candidates) else None


def _quarter_label(d: date) -> str:
    return f"Q{(d.month - 1) // 3 + 1} {d.year}"


def _quarter_bounds(d: date) -> tuple[date, date]:
    start_month = ((d.month - 1) // 3) * 3 + 1
    end_month = start_month + 2
    return (
        date(d.year, start_month, 1),
        date(d.year, end_month, calendar.monthrange(d.year, end_month)[1]),
    )


def _point_in_time_yoc(
    transactions: pd.DataFrame,
    events: list[dict],
    *,
    pay_date_from: date | None = None,
    pay_date_to: date | None = None,
) -> float:
    """Return period dividend income over point-in-time per-ticker capital.

    Each ticker contributes its cost basis once, at its first qualifying
    dividend ex-date in the period. Dividend entitlement is established on the
    ex-date, so a subsequent sale cannot change that ticker's denominator.
    """
    tx = transactions.copy()
    tx["_ticker"] = tx["ticker"].fillna("").astype(str).str.lower()
    tx["_date"] = pd.to_datetime(tx["date"], errors="coerce").dt.date
    tx["_sign"] = tx["transaction"].fillna("").str.lower().map({"buy": 1, "sell": -1})
    tx["_net_cost"] = tx["total_cost_aed"].fillna(0) * tx["_sign"].fillna(0)

    dividends = 0.0
    first_event_date_by_ticker: dict[str, date] = {}
    for event in events:
        if event["status"] != "received" or not event["ex_date"]:
            continue

        pay_date = (
            pd.to_datetime(event["pay_date"]).date() if event["pay_date"] else None
        )
        if pay_date_from and (not pay_date or pay_date < pay_date_from):
            continue
        if pay_date_to and (not pay_date or pay_date > pay_date_to):
            continue

        event_date = pd.to_datetime(event["ex_date"]).date()
        ticker = str(event["ticker"]).lower()
        dividends += event["amount"]
        if (
            ticker not in first_event_date_by_ticker
            or event_date < first_event_date_by_ticker[ticker]
        ):
            first_event_date_by_ticker[ticker] = event_date

    cost_basis = 0.0
    for ticker, event_date in first_event_date_by_ticker.items():
        ticker_cost_basis = tx.loc[
            (tx["_ticker"] == ticker) & (tx["_date"] <= event_date), "_net_cost"
        ].sum()
        if ticker_cost_basis > 0:
            cost_basis += ticker_cost_basis

    return round(dividends / cost_basis * 100, 2) if cost_basis else 0.0


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

        total_received = sum(e["amount"] for e in events if e["status"] == "received")

        # Yield on cost is period income divided by each qualifying ticker's
        # capital once, measured on its first dividend ex-date in that period.
        # This preserves income from positions since sold without adding a
        # ticker's capital again for every dividend it pays. The trailing view
        # only limits dividend pay dates.
        yoc_alltime = _point_in_time_yoc(tx_df, events)
        yoc_trailing_12m = _point_in_time_yoc(
            tx_df,
            events,
            pay_date_from=one_year_ago,
            pay_date_to=today,
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
            "events": events[-8:],
        }

    @staticmethod
    def get_indexes() -> list[dict]:
        """Return the configured benchmark choices for API filter controls."""
        return [
            {"key": key, "label": details["label"], "exchange": details["exchange"]}
            for key, details in BENCHMARKS.items()
            if details.get("type") == "index"
        ]

    def _benchmark_prices(self, ticker: str, start: date, end: date) -> pd.Series:
        benchmark = BENCHMARKS.get(ticker)
        query_ticker = (
            f"{benchmark['exchange']}:{benchmark['symbol']}" if benchmark else ticker
        )
        try:
            prices = self.hql.ticker(query_ticker).prices(start=start, end=end)
        except Exception as exc:
            logger.warning("Unable to load benchmark %s: %s", ticker, exc)
            return pd.Series(dtype=float, name=ticker)

        if isinstance(prices, pd.DataFrame):
            if "close" not in prices.columns:
                return pd.Series(dtype=float, name=ticker)
            prices = prices["close"]
        if not isinstance(prices, pd.Series) or prices.empty:
            return pd.Series(dtype=float, name=ticker)

        prices = pd.to_numeric(prices, errors="coerce").dropna().copy()
        prices.index = _date_index(prices)
        return prices.groupby(level=0).last().sort_index().rename(ticker)

    def _benchmark_return(
        self,
        ticker: str,
        start: date,
        end: date,
    ) -> float | None:
        prices = self._benchmark_prices(ticker, start, end)
        if len(prices) < 2 or prices.iloc[0] <= 0:
            return None
        total = float(prices.iloc[-1] / prices.iloc[0] - 1.0)
        return _annualize(total, max(1, (prices.index[-1] - prices.index[0]).days))

    @staticmethod
    def _calculate_twr(
        value_df: pd.DataFrame,
        transactions: pd.DataFrame,
        dividends: pd.DataFrame,
    ) -> tuple[float | None, float | None, date | None, date | None]:
        """Calculate total and annualized TWR from daily market values.

        Buys and sells are external cash flows. Received dividends are added to
        the numerator on their payment date, so they count as investment return
        even though the portfolio value series only contains securities.
        """
        if value_df.empty or "market_value_aed" not in value_df.columns:
            return None, None, None, None

        values = pd.to_numeric(value_df["market_value_aed"], errors="coerce").dropna()
        if values.empty:
            return None, None, None, None
        values.index = _date_index(values)
        values = values.groupby(level=0).last().sort_index()

        first_day = values.index[0].date()
        last_day = values.index[-1].date()
        first_value = float(values.iloc[0])
        cash_flows: dict[pd.Timestamp, float] = {}
        dividend_flows: dict[pd.Timestamp, float] = {}

        if not transactions.empty:
            tx = transactions.copy()
            tx["_date"] = pd.to_datetime(tx["date"], errors="coerce").dt.date
            for _, row in tx.dropna(subset=["_date"]).iterrows():
                when = row["_date"]
                if when < first_day or when > last_day:
                    continue
                # A transaction already appears in the first valuation.  Only
                # later flows should be removed from the period return.
                if when == first_day and first_value > 0:
                    continue
                snapped = _snap_to_index(when, values.index)
                if snapped is None:
                    continue
                sign = {"buy": 1.0, "sell": -1.0}.get(
                    str(row.get("transaction", "")).lower(), 0.0
                )
                cash_flows[snapped] = cash_flows.get(snapped, 0.0) + sign * float(
                    row.get("total_cost_aed") or 0.0
                )

        if not dividends.empty:
            received = dividends[dividends["status"] == "received"]
            for _, row in received.iterrows():
                pay_date = row.get("pay_date")
                if not pay_date:
                    continue
                when = pd.Timestamp(pay_date).date()
                if when <= first_day or when > last_day:
                    continue
                snapped = _snap_to_index(when, values.index)
                if snapped is not None:
                    dividend_flows[snapped] = dividend_flows.get(snapped, 0.0) + float(
                        row.get("total_aed") or 0.0
                    )

        factors: list[float] = []
        previous = first_value
        for day, current in values.iloc[1:].items():
            if previous > 0:
                factor = (
                    float(current)
                    - cash_flows.get(day, 0.0)
                    + dividend_flows.get(day, 0.0)
                ) / previous
                factors.append(factor)
            elif float(current) > 0 and cash_flows.get(day, 0.0) > 0:
                factors.append(1.0)
            previous = float(current)

        total_return = float(pd.Series(factors).prod() - 1.0) if factors else 0.0
        annualized = _annualize(total_return, max(1, (last_day - first_day).days))
        return total_return, annualized, first_day, last_day

    @staticmethod
    def _calculate_xirr(
        value_df: pd.DataFrame,
        transactions: pd.DataFrame,
        dividends: pd.DataFrame,
    ) -> float | None:
        if value_df.empty or "market_value_aed" not in value_df.columns:
            return None
        values = pd.to_numeric(value_df["market_value_aed"], errors="coerce").dropna()
        if values.empty:
            return None
        values.index = _date_index(values)
        values = values.groupby(level=0).last().sort_index()
        first_day = values.index[0].date()
        last_day = values.index[-1].date()
        first_value = float(values.iloc[0])
        cash_flows: list[tuple[date, float]] = []
        if first_value > 0:
            cash_flows.append((first_day, -first_value))

        if not transactions.empty:
            tx = transactions.copy()
            tx["_date"] = pd.to_datetime(tx["date"], errors="coerce").dt.date
            for _, row in tx.dropna(subset=["_date"]).iterrows():
                when = row["_date"]
                if when < first_day or when > last_day:
                    continue
                if when == first_day and first_value > 0:
                    continue
                amount = float(row.get("total_cost_aed") or 0.0)
                if str(row.get("transaction", "")).lower() == "buy":
                    amount = -amount
                elif str(row.get("transaction", "")).lower() != "sell":
                    continue
                cash_flows.append((when, amount))

        if not dividends.empty:
            received = dividends[dividends["status"] == "received"]
            for _, row in received.iterrows():
                pay_date = row.get("pay_date")
                if not pay_date:
                    continue
                when = pd.Timestamp(pay_date).date()
                if first_day < when <= last_day:
                    cash_flows.append((when, float(row.get("total_aed") or 0.0)))

        cash_flows.append((last_day, float(values.iloc[-1])))
        return _xirr(sorted(cash_flows, key=lambda item: item[0]))

    def get_performance(
        self,
        filters: PortfolioFilters,
        *,
        benchmark: str | None = None,
        index_scope: Literal["all", "mapped"] = "all",
    ) -> dict:
        """Return alpha, XIRR, TWR, and timing skill for a filtered portfolio."""
        benchmark_key = None
        if benchmark:
            benchmark_key = next(
                (key for key in BENCHMARKS if key.upper() == benchmark.upper()), None
            )
            if (
                benchmark_key is None
                or BENCHMARKS[benchmark_key].get("type") != "index"
            ):
                raise ValueError(f"Unknown index benchmark: {benchmark}")
        if index_scope == "mapped" and benchmark_key is None:
            raise ValueError("index_scope='mapped' requires an index benchmark")

        p = self.hql.portfolio()
        tx = p.transactions()
        empty_summary = {
            "xirr_pct": None,
            "twr_pct": None,
            "twr_total_pct": None,
            "timing_skill_pct": None,
            "alpha_pct": None,
            "benchmark_return_pct": None,
            "benchmark_coverage_pct": 0.0,
        }
        if tx.empty:
            return {
                "benchmark": benchmark_key,
                "index_scope": index_scope,
                "summary": empty_summary,
                "positions": [],
            }

        tx = tx.copy()
        tx["_date"] = pd.to_datetime(tx["date"], errors="coerce").dt.date
        tx = tx[tx["_date"].isna() | (tx["_date"] <= filters.date_range.end)]
        if filters.tickers:
            tx = tx[tx["ticker"].isin(filters.tickers)]
        if filters.sectors:
            tx = tx[tx["sector"].isin(filters.sectors)]
        if filters.exchanges:
            tx = tx[tx["exchange"].isin(filters.exchanges)]
        if tx.empty:
            return {
                "benchmark": benchmark_key,
                "index_scope": index_scope,
                "summary": empty_summary,
                "positions": [],
            }

        meta = tx.sort_values("_date").drop_duplicates("ticker", keep="first").copy()
        meta["mapped_benchmark"] = meta["exchange"].map(_benchmark_for_exchange)
        if index_scope == "mapped":
            meta = meta[meta["mapped_benchmark"] == benchmark_key]
        if meta.empty:
            return {
                "benchmark": benchmark_key,
                "index_scope": index_scope,
                "summary": empty_summary,
                "positions": [],
            }

        tickers = meta["ticker"].tolist()
        tx = tx[tx["ticker"].isin(tickers)]
        divs = p.dividends()
        if not divs.empty:
            divs = divs[divs["ticker"].isin(tickers)].copy()

        portfolio_values = p.value(
            start_date=filters.date_range.start,
            end_date=filters.date_range.end,
            tickers=tickers,
        )
        twr_total, twr_annualized, first_day, last_day = self._calculate_twr(
            portfolio_values, tx, divs
        )
        xirr = self._calculate_xirr(portfolio_values, tx, divs)

        holdings = p.holdings()
        if not holdings.empty:
            holdings = holdings[holdings["ticker"].isin(tickers)].copy()
            market_total = float(holdings["market_value_aed"].sum())
        else:
            market_total = 0.0

        benchmark_cache: dict[str, float | None] = {}
        positions: list[dict] = []
        weighted_benchmark = 0.0
        weighted_coverage = 0.0

        for _, row in meta.iterrows():
            ticker = row["ticker"]
            ticker_benchmark = benchmark_key or row["mapped_benchmark"]
            holding = (
                holdings[holdings["ticker"] == ticker]
                if not holdings.empty
                else pd.DataFrame()
            )
            market_value = (
                float(holding["market_value_aed"].iloc[0]) if not holding.empty else 0.0
            )
            weight = market_value / market_total if market_total > 0 else 0.0
            ticker_tx = tx[tx["ticker"] == ticker]
            ticker_divs = divs[divs["ticker"] == ticker] if not divs.empty else divs
            ticker_values = p.value(
                start_date=filters.date_range.start,
                end_date=filters.date_range.end,
                tickers=[ticker],
            )
            _, position_twr, _, _ = self._calculate_twr(
                ticker_values, ticker_tx, ticker_divs
            )

            if ticker_benchmark:
                if ticker_benchmark not in benchmark_cache:
                    benchmark_cache[ticker_benchmark] = self._benchmark_return(
                        ticker_benchmark,
                        filters.date_range.start,
                        filters.date_range.end,
                    )
                benchmark_return = benchmark_cache[ticker_benchmark]
            else:
                benchmark_return = None

            position_alpha = (
                (position_twr - benchmark_return) * 100
                if position_twr is not None and benchmark_return is not None
                else None
            )
            if benchmark_return is not None:
                weighted_benchmark += weight * benchmark_return
                weighted_coverage += weight

            positions.append(
                {
                    "ticker": ticker,
                    "exchange": row.get("exchange"),
                    "sector": row.get("sector"),
                    "index": ticker_benchmark,
                    "index_label": (
                        BENCHMARKS.get(ticker_benchmark, {}).get("label")
                        if ticker_benchmark
                        else None
                    ),
                    "market_value": round(market_value, 2),
                    "weight_pct": round(weight * 100, 2),
                    "twr_pct": (
                        round(position_twr * 100, 2)
                        if position_twr is not None
                        else None
                    ),
                    "benchmark_return_pct": (
                        round(benchmark_return * 100, 2)
                        if benchmark_return is not None
                        else None
                    ),
                    "alpha_pct": (
                        round(position_alpha, 2) if position_alpha is not None else None
                    ),
                }
            )

        benchmark_return = weighted_benchmark if weighted_coverage else None
        alpha = (
            (twr_annualized - benchmark_return) * 100
            if twr_annualized is not None and benchmark_return is not None
            else None
        )
        timing_skill = (
            (xirr - twr_annualized) * 100
            if xirr is not None and twr_annualized is not None
            else None
        )

        summary = {
            "xirr_pct": round(xirr * 100, 2) if xirr is not None else None,
            "twr_pct": (
                round(twr_annualized * 100, 2) if twr_annualized is not None else None
            ),
            "twr_total_pct": (
                round(twr_total * 100, 2) if twr_total is not None else None
            ),
            "timing_skill_pct": (
                round(timing_skill, 2) if timing_skill is not None else None
            ),
            "alpha_pct": round(alpha, 2) if alpha is not None else None,
            "benchmark_return_pct": (
                round(benchmark_return * 100, 2)
                if benchmark_return is not None
                else None
            ),
            "benchmark_coverage_pct": round(weighted_coverage * 100, 2),
            "period_start": first_day.isoformat() if first_day else None,
            "period_end": last_day.isoformat() if last_day else None,
        }
        return {
            "benchmark": (
                {
                    "key": benchmark_key,
                    "label": BENCHMARKS[benchmark_key]["label"],
                }
                if benchmark_key
                else None
            ),
            "index_scope": index_scope,
            "summary": summary,
            "positions": positions,
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
