# app/hql/queries/portfolio_returns.py

from __future__ import annotations

from datetime import date

import pandas as pd


def _annualize(total_return: float, days: int) -> float:
    if days <= 0:
        return total_return
    if total_return <= -1:
        return -1.0
    return (1.0 + total_return) ** (365.0 / days) - 1.0


def _date_index(values: pd.Series) -> pd.DatetimeIndex:
    index = pd.to_datetime(values.index)
    if index.tz is not None:
        index = index.tz_localize(None)
    return index.normalize()


def _snap_to_index(when: date, index: pd.DatetimeIndex) -> pd.Timestamp | None:
    candidates = index[index >= pd.Timestamp(when)]
    return candidates.min() if len(candidates) else None


class PortfolioReturnsMixin:
    """
    Return/risk analytics layered onto PortfolioQuery: realized P&L,
    time-weighted return, locked benchmark weights, and the daily returns
    matrix used for volatility/beta/covariance. Split out of portfolio.py to
    keep that file focused on state (transactions/holdings/dividends/value)
    while this one owns performance math. Relies on the host class providing
    transactions(), dividends(), holdings(), value(), and price_repo.
    """

    def _realized_events(self, tx: pd.DataFrame) -> pd.DataFrame:
        """
        Walks the full transaction history in chronological order and returns
        one row per sell, using a running average-cost basis per ticker: buys
        update the average, sells realize against whatever the average was at
        that point in time. This is what makes a position bought in several
        lots (e.g. three separate EMAAR buys) realize correctly instead of
        being flattened into a single all-time average price.

        Returns
        -------
        pd.DataFrame
            Columns: date, ticker, realized_aed -- one row per sell event.
        """
        empty = pd.DataFrame(columns=["date", "ticker", "realized_aed"])
        if tx.empty:
            return empty

        tx = tx.copy()
        tx["date_clean"] = pd.to_datetime(tx["date"]).fillna(pd.Timestamp("1900-01-01"))
        tx["_tx_lower"] = tx["transaction"].str.lower()
        tx_ordered = tx.sort_values(
            ["date_clean", "_tx_lower"], kind="stable", ascending=[True, True]
        )

        avg_cost: dict[str, float] = {}
        running: dict[str, float] = {}
        rows: list[dict] = []

        for _, row in tx_ordered.iterrows():
            ticker = row["ticker"]
            shares = row["shares"] if pd.notna(row["shares"]) else 0.0
            cost = row["total_cost_aed"] if pd.notna(row["total_cost_aed"]) else 0.0
            tx_type = row["_tx_lower"]
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
                rows.append(
                    {"date": event_date, "ticker": ticker, "realized_aed": gain}
                )
                running[ticker] = max(0.0, running.get(ticker, 0.0) - shares)

        if not rows:
            return empty
        return pd.DataFrame(rows)

    def realized_pnl(
        self,
        start_date: date,
        end_date: date,
        tickers: list[str] | None = None,
    ) -> pd.DataFrame:
        """
            Realized P&L per ticker for sells that occurred within
            [start_date, end_date].

            Uses the same point-in-time running-average-cost method as
            value(), so a sell's gain reflects the average cost of shares
            actually held at the time of sale, not an all-time average that
            ignores trade order.

            Returns
        -
            pd.DataFrame
                Columns: ticker, realized_aed
        """
        empty = pd.DataFrame(columns=["ticker", "realized_aed"])
        events = self._realized_events(self.transactions())
        if events.empty:
            return empty

        mask = (events["date"] >= start_date) & (events["date"] <= end_date)
        if tickers:
            mask &= events["ticker"].isin(tickers)
        filtered = events[mask]
        if filtered.empty:
            return empty

        return (
            filtered.groupby("ticker", as_index=False)["realized_aed"]
            .sum()
            .sort_values("ticker")
            .reset_index(drop=True)
        )

    def twr(
        self,
        start_date: date,
        end_date: date,
        tickers: list[str] | None = None,
        include_dividends: bool = True,
    ) -> dict:
        """
            Sub-period, geometrically-linked time-weighted return.

            Breaks the period at every external cash flow (buy/sell) so
            contributions and withdrawals are never counted as market
            performance. Identical math for the whole portfolio
            (tickers=None) or a single position (tickers=[x]) -- which is
            what makes a position bought in several lots resolve correctly
            instead of averaging purchase prices together.

            Returns
        -
            dict
                total_return, annualized_return : float | None (percent, e.g. 12.34)
                dates : list[str] (ISO)
                cumulative_return_pct : list[float] -- daily running geometric
                    link, starting at 0.0 on the first day
                period_start, period_end : str (ISO) | None
        """
        empty = {
            "total_return": None,
            "annualized_return": None,
            "dates": [],
            "cumulative_return_pct": [],
            "period_start": None,
            "period_end": None,
        }

        value_df = self.value(start_date=start_date, end_date=end_date, tickers=tickers)
        if value_df.empty or "market_value_aed" not in value_df.columns:
            return empty

        values = pd.to_numeric(value_df["market_value_aed"], errors="coerce").dropna()
        if values.empty:
            return empty
        values.index = _date_index(values)
        values = values.groupby(level=0).last().sort_index()

        first_day = values.index[0].date()
        last_day = values.index[-1].date()
        first_value = float(values.iloc[0])

        tx = self.transactions()
        if tickers:
            tx = tx[tx["ticker"].isin(tickers)]

        cash_flows: dict[pd.Timestamp, float] = {}
        if not tx.empty:
            tx = tx.copy()
            tx["_date"] = pd.to_datetime(tx["date"], errors="coerce").dt.date
            for _, row in tx.dropna(subset=["_date"]).iterrows():
                when = row["_date"]
                if when < first_day or when > last_day:
                    continue
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

        dividend_flows: dict[pd.Timestamp, float] = {}
        if include_dividends:
            divs = self.dividends()
            if tickers:
                divs = divs[divs["ticker"].isin(tickers)]
            if not divs.empty:
                received = divs[divs["status"] == "received"]
                for _, row in received.iterrows():
                    pay_date = row.get("pay_date")
                    if not pay_date:
                        continue
                    when = pd.Timestamp(pay_date).date()
                    if when <= first_day or when > last_day:
                        continue
                    snapped = _snap_to_index(when, values.index)
                    if snapped is not None:
                        dividend_flows[snapped] = dividend_flows.get(
                            snapped, 0.0
                        ) + float(row.get("total_aed") or 0.0)

        dates: list[str] = [first_day.isoformat()]
        cumulative: list[float] = [0.0]
        factors: list[float] = []
        running_product = 1.0
        previous = first_value

        for day, current in values.iloc[1:].items():
            if previous > 0:
                factor = (
                    float(current)
                    - cash_flows.get(day, 0.0)
                    + dividend_flows.get(day, 0.0)
                ) / previous
            elif float(current) > 0 and cash_flows.get(day, 0.0) > 0:
                factor = 1.0
            else:
                previous = float(current)
                continue

            factors.append(factor)
            running_product *= factor
            dates.append(day.date().isoformat())
            cumulative.append(round((running_product - 1.0) * 100, 4))
            previous = float(current)

        total_return = float(running_product - 1.0) if factors else 0.0
        annualized = _annualize(total_return, max(1, (last_day - first_day).days))

        return {
            "total_return": round(total_return * 100, 4),
            "annualized_return": round(annualized * 100, 4),
            "dates": dates,
            "cumulative_return_pct": cumulative,
            "period_start": first_day.isoformat(),
            "period_end": last_day.isoformat(),
        }

    def benchmark_weights(
        self,
        on: date,
        exchange_to_index: dict[str, str],
    ) -> dict[str, float]:
        """
            Locks each mapped benchmark index's weight to the portfolio's
            actual capital allocation by exchange at a single point in time
            (GIPS-style). Weights are not re-derived daily, so the blended
            benchmark itself can't drift into a moving target.

            Parameters
        -
            on : date
                Snapshot date (typically the start of the comparison window).
            exchange_to_index : dict
                Maps a holding's exchange to its benchmark index key.

            Returns
        -
            dict[str, float]
                {index_key: weight}, weights sum to 1.0 across all exchanges
                with a mapped index and non-zero market value on `on`.
        """
        holdings = self.holdings(on=on)
        if holdings.empty:
            return {}

        tx = self.transactions()
        meta = tx.drop_duplicates("ticker").set_index("ticker")["exchange"]
        holdings = holdings.copy()
        holdings["exchange"] = holdings["ticker"].map(meta.to_dict())
        holdings["index"] = holdings["exchange"].map(
            lambda ex: exchange_to_index.get(str(ex or "").strip().upper())
        )

        total_mv = float(holdings["market_value_aed"].sum())
        if total_mv <= 0:
            return {}

        grouped = (
            holdings.dropna(subset=["index"]).groupby("index")["market_value_aed"].sum()
        )
        return {index: float(mv / total_mv) for index, mv in grouped.items()}

    def returns_matrix(
        self,
        tickers: list[str],
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:
        """
            Daily simple returns (pct-change of AED close price) per ticker,
            each column keeping its own actual trading days rather than being
            forward-filled onto a shared calendar -- forward-filling would
            fabricate zero-return days on exchange holidays and bias
            volatility/covariance. Used for volatility, beta, drawdown, and
            the Section 4 covariance matrix; independent of position sizing
            or cash-flow timing.

            Returns
        -
            pd.DataFrame
                Index: trading days (tz-naive, midnight). Columns: tickers.
                Values: daily return (fraction).
        """
        if not tickers:
            return pd.DataFrame()

        frames = []
        for ticker in tickers:
            price_df = self.price_repo.get_ohlcv(ticker, start_date, end_date)
            if price_df.empty or "close" not in price_df.columns:
                continue
            series = price_df["close"]
            if series.index.tz is not None:
                series.index = series.index.tz_localize(None)
            series.index = series.index.normalize()
            frames.append(series.groupby(level=0).last().rename(ticker))

        if not frames:
            return pd.DataFrame()

        prices = pd.concat(frames, axis=1).sort_index()
        return prices.pct_change(fill_method=None).dropna(how="all")
