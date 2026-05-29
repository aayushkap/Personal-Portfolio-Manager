from __future__ import annotations

from datetime import date
from typing import Literal

import pandas as pd

from app.hql.parsers import parse_date, parse_money_string, parse_number
from app.hql.repositories import CacheRepository, FXService, PriceRepository


class PortfolioQuery:
    def __init__(
        self,
        cache_repo: CacheRepository,
        price_repo: PriceRepository,
        fx: FXService,
    ) -> None:
        self.cache_repo = cache_repo
        self.price_repo = price_repo
        self.fx = fx

    def transactions(self) -> pd.DataFrame:
        rows: list[dict] = []

        for ticker in self.cache_repo.list_tickers():
            raw = self.cache_repo.get_raw_ticker(ticker)
            details = raw.get("purchase_details") or []
            if not details:
                continue

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
                        "date": parse_date(d.get("date")),
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
        tx = self.transactions()
        if tx.empty:
            return pd.DataFrame(columns=["ticker", "shares"])

        cutoff = pd.Timestamp(on or date.today())
        work = tx[pd.to_datetime(tx["date"]) <= cutoff].copy()
        if work.empty:
            return pd.DataFrame(columns=["ticker", "shares"])

        sign = work["transaction"].str.lower().map({"buy": 1, "sell": -1}).fillna(0)
        work["net_shares"] = work["shares"].fillna(0) * sign

        out = (
            work.groupby("ticker", as_index=False)["net_shares"]
            .sum()
            .rename(columns={"net_shares": "shares"})
        )

        return out[out["shares"] != 0].sort_values("ticker").reset_index(drop=True)

    def value(
        self,
        days: int | None = 365,
        start: date | str | None = None,
        end: date | str | None = None,
        granularity: str = "1D",
    ) -> pd.Series:
        holdings = self.holdings()
        if holdings.empty:
            return pd.Series(dtype=float, name="portfolio_value_aed")

        tickers = holdings["ticker"].tolist()
        shares_map = holdings.set_index("ticker")["shares"].to_dict()

        prices = self.price_repo.get_multi_close_series(
            tickers,
            start=(
                parse_date(start)
                if start
                else (date.today() if days is None else date.today())
            ),
            end=parse_date(end) if end else date.today(),
            granularity=granularity,
        )

        if prices.empty:
            return pd.Series(dtype=float, name="portfolio_value_aed")

        for ticker in prices.columns:
            prices[ticker] = prices[ticker] * shares_map.get(ticker, 0)

        total = prices.sum(axis=1)
        total.name = "portfolio_value_aed"
        return total

    def allocation(
        self,
        by: Literal["position", "sector", "exchange"] = "position",
    ) -> dict:
        tx = self.transactions()
        if tx.empty:
            return {"by": by, "total_market_value": 0.0, "allocations": []}

        holdings = self.holdings()
        if holdings.empty:
            return {"by": by, "total_market_value": 0.0, "allocations": []}

        tickers = holdings["ticker"].tolist()
        latest = self.price_repo.get_multi_close_series(
            tickers=tickers,
            start=date.today(),
            end=date.today(),
            granularity="1D",
        )

        if latest.empty:
            return {"by": by, "total_market_value": 0.0, "allocations": []}

        last_prices = latest.ffill().iloc[-1].to_dict()
        holdings = holdings.copy()
        holdings["market_value"] = holdings["ticker"].map(
            lambda t: holdings.loc[holdings["ticker"] == t, "shares"].iloc[0]
            * last_prices.get(t, 0.0)
        )

        meta = tx.drop_duplicates("ticker")[["ticker", "sector", "exchange"]].set_index(
            "ticker"
        )

        holdings["sector"] = holdings["ticker"].map(meta["sector"].to_dict())
        holdings["exchange"] = holdings["ticker"].map(meta["exchange"].to_dict())

        key = {"position": "ticker", "sector": "sector", "exchange": "exchange"}[by]
        grouped = (
            holdings.groupby(key, dropna=False)["market_value"]
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
