# app/hql/queries/portfolio.py

from __future__ import annotations

from datetime import date, timedelta
from typing import Literal

import pandas as pd

from app.hql.parsers import parse_date, parse_money_string, parse_number
from app.hql.repositories import CacheRepository, FXService, PriceRepository


class PortfolioQuery:
    """
    Portfolio domain abstraction.

    Provides clean, functional access to portfolio state, history, and metrics.
    All monetary values returned are consistently in AED.
    """

    def __init__(
        self,
        cache_repo: CacheRepository,
        price_repo: PriceRepository,
        fx: FXService,
    ) -> None:
        self.cache_repo = cache_repo
        self.price_repo = price_repo
        self.fx = fx

    # Internal helpers
    def _build_daily_shares(
        self, tx: pd.DataFrame, timeline: pd.DatetimeIndex
    ) -> pd.DataFrame:
        """
        Returns a DataFrame of cumulative shares held per ticker per day,
        aligned to the given timeline. Point-in-time: shares only appear
        from the day they were purchased.
        """
        sign = tx["transaction"].str.lower().map({"buy": 1, "sell": -1}).fillna(0)
        tx = tx.copy()
        tx["net_shares"] = tx["shares"].fillna(0) * sign
        tx["date_clean"] = pd.to_datetime(tx["date"]).fillna(timeline[0])

        daily_changes = (
            tx.groupby(["date_clean", "ticker"])["net_shares"]
            .sum()
            .unstack(fill_value=0)
        )
        return daily_changes.reindex(timeline, fill_value=0).cumsum()

    def _fetch_prices(
        self,
        tickers: list[str],
        timeline_start: date,
        end_date: date,
        timeline: pd.DatetimeIndex,
        granularity: str = "1D",
    ) -> pd.DataFrame:
        """
        Fetches OHLCV close prices for all tickers, normalizes to timezone-naive
        midnight, reindexes to the full timeline, and forward-fills.
        """
        frames = []
        for ticker in tickers:
            price_df = self.price_repo.get_ohlcv(
                ticker=ticker,
                start=timeline_start,
                end=end_date,
                granularity=granularity,
            )
            if not price_df.empty:
                frames.append(price_df["close"].rename(ticker))

        if not frames:
            return pd.DataFrame()

        prices = pd.concat(frames, axis=1)

        if prices.index.tz is not None:
            prices.index = prices.index.tz_localize(None)
        prices.index = prices.index.normalize()

        return prices.reindex(timeline).ffill()

    # Public API
    def transactions(self) -> pd.DataFrame:
        """
            Retrieves the complete history of all portfolio transactions.

            Returns
        -
            pd.DataFrame
                A timeline of all buy/sell events.
                Columns: ticker, date, transaction, platform, sector, exchange,
                         shares, price, price_aed, total_cost, total_cost_aed, currency
        """
        rows: list[dict] = []

        for ticker in self.cache_repo.list_tickers():
            raw = self.cache_repo.get_raw_ticker(ticker)
            details = raw.get("purchase_details") or []

            for d in details:
                price, currency = parse_money_string(d.get("cost_per_share"))
                total_cost, total_currency = parse_money_string(d.get("total_cost"))
                tx_currency = (
                    currency or total_currency or self.cache_repo.resolve_currency(raw)
                )

                shares = parse_number(d.get("shares"))
                price_aed = self.fx.to_aed(price, tx_currency)
                total_cost_aed = self.fx.to_aed(total_cost, tx_currency)

                rows.append(
                    {
                        "ticker": ticker,
                        "date": parse_date(d.get("purchase_date") or d.get("date")),
                        "transaction": (d.get("transaction") or "").strip(),
                        "platform": d.get("platform"),
                        "sector": d.get("sector"),
                        "exchange": (raw.get("overview") or {}).get("exchange"),
                        "shares": shares,
                        "price": price,
                        "price_aed": price_aed,
                        "total_cost": total_cost,
                        "total_cost_aed": total_cost_aed,
                        "currency": tx_currency or "AED",
                    }
                )

        df = pd.DataFrame(rows)
        if df.empty:
            return pd.DataFrame(
                columns=[
                    "ticker",
                    "date",
                    "transaction",
                    "platform",
                    "sector",
                    "exchange",
                    "shares",
                    "price",
                    "price_aed",
                    "total_cost",
                    "total_cost_aed",
                    "currency",
                ]
            )

        return df.sort_values(["date", "ticker"]).reset_index(drop=True)

    def holdings(self, on: date | None = None) -> pd.DataFrame:
        """
            Calculates the current portfolio holdings and cost basis.

            Parameters
        -
            on : date, optional
                The date to calculate holdings for. Defaults to today.

            Returns
        -
            pd.DataFrame
                The active holdings.
                Columns: ticker, shares, cost_basis_aed, last_price_aed, market_value_aed
        """
        tx = self.transactions()
        if tx.empty:
            return pd.DataFrame(
                columns=[
                    "ticker",
                    "shares",
                    "cost_basis_aed",
                    "last_price_aed",
                    "market_value_aed",
                ]
            )

        cutoff = pd.Timestamp(on or date.today())
        tx_dates = pd.to_datetime(tx["date"])
        work = tx[tx_dates.isna() | (tx_dates <= cutoff)].copy()

        if work.empty:
            return pd.DataFrame(
                columns=[
                    "ticker",
                    "shares",
                    "cost_basis_aed",
                    "last_price_aed",
                    "market_value_aed",
                ]
            )

        sign = work["transaction"].str.lower().map({"buy": 1, "sell": -1}).fillna(0)
        work["net_shares"] = work["shares"].fillna(0) * sign
        work["net_cost"] = work["total_cost_aed"].fillna(0) * sign

        grouped = work.groupby("ticker", as_index=False).agg(
            shares=("net_shares", "sum"), cost_basis_aed=("net_cost", "sum")
        )
        grouped = (
            grouped[grouped["shares"] != 0].sort_values("ticker").reset_index(drop=True)
        )

        if grouped.empty:
            return grouped

        price_map = {}
        for ticker in grouped["ticker"]:
            latest = self.price_repo.get_latest_price(ticker)
            price_map[ticker] = latest.get("close", 0)

        grouped["last_price_aed"] = grouped["ticker"].map(price_map)
        grouped["market_value_aed"] = grouped["shares"] * grouped[
            "last_price_aed"
        ].fillna(0)

        return grouped

    def dividends(self, on: date | None = None) -> pd.DataFrame:
        """
            Returns all dividend events the portfolio is eligible for, separated
            by whether they have been received or are still pending.

            Eligibility is determined by matching each dividend's ex-dividend date
            against the shares held at that point in time. A dividend is only
            included if shares were held on the ex-date.

            A dividend is marked 'received' once its pay_date has passed.
            A dividend is marked 'pending' if the pay_date is in the future or unknown.

            Parameters
        -
            on : date, optional
                Reference date for received/pending classification. Defaults to today.

            Returns
        -
            pd.DataFrame
                Columns:
                    ticker               — position ticker
                    ex_date              — ex-dividend date
                    pay_date             — payment date (None if not available)
                    shares_held          — shares held on the ex-date
                    amount_per_share_aed — dividend per share converted to AED
                    total_aed            — total income (shares_held × amount_per_share_aed)
                    status               — 'received' or 'pending'
        """
        today = on or date.today()
        tx = self.transactions()

        empty = pd.DataFrame(
            columns=[
                "ticker",
                "ex_date",
                "pay_date",
                "shares_held",
                "amount_per_share_aed",
                "total_aed",
                "status",
            ]
        )
        if tx.empty:
            return empty

        # Build a running share ledger per ticker sorted by date
        tx = tx.copy()
        sign = tx["transaction"].str.lower().map({"buy": 1, "sell": -1}).fillna(0)
        tx["net_shares"] = tx["shares"].fillna(0) * sign
        tx["date_clean"] = pd.to_datetime(tx["date"]).fillna(pd.Timestamp("1900-01-01"))
        tx = tx.sort_values("date_clean")

        rows = []
        for ticker in tx["ticker"].unique():
            ticker_tx = tx[tx["ticker"] == ticker].copy()
            ticker_tx["running_shares"] = ticker_tx["net_shares"].cumsum()

            raw = self.cache_repo.get_raw_ticker(ticker)
            div_rows = (raw.get("dividends") or {}).get("rows") or []
            ticker_currency = self.cache_repo.resolve_currency(raw)

            for div in div_rows:
                ex_date_raw = div.get("Ex-Dividend Date") or div.get("ex_date")
                if not ex_date_raw or ex_date_raw == "-":
                    continue

                ex_date = parse_date(ex_date_raw)
                if not ex_date:
                    continue

                pay_date = parse_date(div.get("Pay Date") or div.get("pay_date"))

                # Shares held on the ex-dividend date (point-in-time)
                held = ticker_tx[ticker_tx["date_clean"] <= pd.Timestamp(ex_date)]
                shares_held = (
                    float(held["running_shares"].iloc[-1]) if not held.empty else 0.0
                )
                if shares_held <= 0:
                    continue

                amount, div_currency = parse_money_string(
                    div.get("Cash Amount") or div.get("amount") or ""
                )
                amount_aed = self.fx.to_aed(amount, div_currency or ticker_currency)
                if not amount_aed:
                    continue

                rows.append(
                    {
                        "ticker": ticker,
                        "ex_date": ex_date,
                        "pay_date": pay_date,
                        "shares_held": shares_held,
                        "amount_per_share_aed": round(amount_aed, 6),
                        "total_aed": round(shares_held * amount_aed, 2),
                        "status": (
                            "received"
                            if (pay_date and pay_date <= today)
                            else "pending"
                        ),
                    }
                )

        if not rows:
            return empty

        return (
            pd.DataFrame(rows)
            .sort_values(["ex_date", "ticker"], ascending=[False, True])
            .reset_index(drop=True)
        )

    def value(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
        granularity: str = "1D",
    ) -> pd.DataFrame:
        """
            Calculates the historical daily portfolio value broken into three components.

            Columns
        -
            total_invested_aed
                Cumulative net cash deployed into the portfolio on each day.
                Buys increase it, sells reduce it (since that cash came back).
                This is your running cost basis over time.

            market_value_aed
                The value of currently held open positions on each day.
                Point-in-time: a position only contributes from its purchase date.
                IBM buy+sell on the same day nets to zero here — it leaves no trace.

            total_value_aed
                The full economic picture:
                    market_value_aed
                  + cumulative realized P&L from closed positions (e.g. IBM scalp)
                  + cumulative dividends received up to that day
                This is what the portfolio has actually generated for you over time.

            Prices are forward-filled across weekends and holidays. The series is
            capped at the last date for which any price data exists.

            Parameters
        -
            start_date : date, optional
                Start of the return window. Defaults to one year before end_date.
            end_date : date, optional
                End of the window. Defaults to today.
            granularity : str
                Resampling granularity. Default "1D".

            Returns
        -
            pd.DataFrame
                Index: pd.DatetimeIndex (timezone-naive, midnight).
                Columns: total_invested_aed, market_value_aed, total_value_aed
        """
        empty = pd.DataFrame(
            columns=["total_invested_aed", "market_value_aed", "total_value_aed"]
        )

        tx = self.transactions()
        if tx.empty:
            return empty

        end_date = parse_date(end_date) or date.today()
        start_date = parse_date(start_date) or (end_date - timedelta(days=365))

        first_tx_date = pd.to_datetime(tx["date"]).min()
        timeline_start = (
            start_date
            if pd.isna(first_tx_date)
            else min(first_tx_date.date(), start_date)
        )
        timeline = pd.date_range(start=timeline_start, end=end_date, freq="D")

        # Point-in-time share counts
        daily_shares = self._build_daily_shares(tx, timeline)

        # Cumulative net cash invested (buys positive, sells reduce it)
        tx = tx.copy()
        sign = tx["transaction"].str.lower().map({"buy": 1, "sell": -1}).fillna(0)
        tx["date_clean"] = pd.to_datetime(tx["date"]).fillna(timeline[0])
        tx["cash_deployed"] = tx["total_cost_aed"].fillna(0) * sign
        daily_invested = (
            tx.groupby("date_clean")["cash_deployed"]
            .sum()
            .reindex(timeline, fill_value=0)
            .cumsum()
        )

        # Cumulative realized P&L from closed positions
        # Realized gain on a sell = sell proceeds - proportional cost basis.
        # Simple approach: for each sell, realized = sell_total - avg_cost × shares_sold.
        # We calculate this per ticker using a running average cost method.
        realized_events: dict[date, float] = {}
        avg_cost: dict[str, float] = {}  # ticker → current avg cost per share in AED
        running: dict[str, float] = {}  # ticker → current shares held

        for _, row in tx.sort_values("date_clean").iterrows():
            ticker = row["ticker"]
            shares = row["shares"] if pd.notna(row["shares"]) else 0.0
            cost = row["total_cost_aed"] if pd.notna(row["total_cost_aed"]) else 0.0
            tx_type = (row["transaction"] or "").strip().lower()
            tx_date = row["date_clean"]

            if tx_type == "buy":
                prev_shares = running.get(ticker, 0.0)
                prev_avg = avg_cost.get(ticker, 0.0)
                new_shares = prev_shares + shares
                avg_cost[ticker] = (
                    ((prev_shares * prev_avg) + cost) / new_shares
                    if new_shares
                    else 0.0
                )
                running[ticker] = new_shares

            elif tx_type == "sell":
                cost_basis_of_sold = avg_cost.get(ticker, 0.0) * shares
                gain = cost - cost_basis_of_sold
                event_date = tx_date.date() if hasattr(tx_date, "date") else tx_date
                realized_events[event_date] = (
                    realized_events.get(event_date, 0.0) + gain
                )
                running[ticker] = max(0.0, running.get(ticker, 0.0) - shares)

        realized_series = (
            pd.Series(realized_events).reindex(timeline.date, fill_value=0.0).values
        )
        cumulative_realized = pd.Series(realized_series, index=timeline).cumsum()

        # Cumulative received dividends per day
        divs = self.dividends()
        daily_div = pd.Series(0.0, index=timeline)
        if not divs.empty:
            received = divs[divs["status"] == "received"]
            if not received.empty:
                div_by_day = received.groupby("pay_date")["total_aed"].sum()
                for pay_date, amount in div_by_day.items():
                    ts = pd.Timestamp(pay_date)
                    if ts in daily_div.index:
                        daily_div[ts] = amount
        cumulative_divs = daily_div.cumsum()

        # Prices
        tickers = tx["ticker"].unique().tolist()
        prices = self._fetch_prices(
            tickers, timeline_start, end_date, timeline, granularity
        )

        if prices.empty:
            return empty

        # Cap at last real data date
        last_data_row = prices.dropna(how="all").index.max()
        if last_data_row is None:
            return empty

        prices = prices.loc[:last_data_row]
        daily_shares = daily_shares.loc[:last_data_row]
        daily_invested = daily_invested.loc[:last_data_row]
        cumulative_realized = cumulative_realized.loc[:last_data_row]
        cumulative_divs = cumulative_divs.loc[:last_data_row]

        common_tickers = [t for t in daily_shares.columns if t in prices.columns]
        if not common_tickers:
            return empty

        market_value = (
            (daily_shares[common_tickers] * prices[common_tickers])
            .sum(axis=1, min_count=1)
            .dropna()
        )

        # Align all series to the market_value index (trading days only)
        idx = market_value.index
        total_value = (
            market_value
            + cumulative_realized.reindex(idx, method="ffill").fillna(0)
            + cumulative_divs.reindex(idx, method="ffill").fillna(0)
        )

        df = pd.DataFrame(
            {
                "total_invested_aed": daily_invested.reindex(
                    idx, method="ffill"
                ).fillna(0),
                "market_value_aed": market_value,
                "total_value_aed": total_value,
            }
        )

        return df[df.index.date >= start_date]

    def allocation(
        self,
        by: Literal["position", "sector", "exchange"] = "position",
    ) -> dict:
        """
            Calculates the portfolio allocation weights.

            Parameters
        -
            by : 'position', 'sector', or 'exchange'
                The dimension to group allocations by.

            Returns
        -
            dict
                Contains total market value and a list of allocation weights.
                Example: {"by": "sector", "total_market_value": 1000, "allocations": [...]}
        """
        holdings = self.holdings()
        if holdings.empty:
            return {"by": by, "total_market_value": 0.0, "allocations": []}

        tx = self.transactions()
        meta = tx.drop_duplicates("ticker")[["ticker", "sector", "exchange"]].set_index(
            "ticker"
        )

        holdings = holdings.copy()
        holdings["sector"] = holdings["ticker"].map(meta["sector"].to_dict())
        holdings["exchange"] = holdings["ticker"].map(meta["exchange"].to_dict())

        key = {"position": "ticker", "sector": "sector", "exchange": "exchange"}[by]
        grouped = (
            holdings.groupby(key, dropna=False)["market_value_aed"]
            .sum()
            .sort_values(ascending=False)
        )

        total_mv = float(grouped.sum())
        if total_mv == 0:
            return {"by": by, "total_market_value": 0.0, "allocations": []}

        allocations = [
            {
                "name": str(name),
                "market_value": float(value),
                "weight": float(value / total_mv),
            }
            for name, value in grouped.items()
        ]

        return {
            "by": by,
            "total_market_value": total_mv,
            "allocations": allocations,
        }
