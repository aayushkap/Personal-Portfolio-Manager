#!/usr/bin/env python3
"""Ranked asset-allocation demo using the local OHLC database.

Example:
    python3 ranked_allocation_demo.py --lookback-days 20 --top-n 5
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from app.data.db import DB
from app.data.gsheet import GSheet_Manager


@dataclass
class Config:
    lookback_days: int = 1000
    rebalance_days: int = 5
    top_n: int = 5
    min_history_days: int = 60
    max_stale_days: int = 5
    weighting: str = "inverse-volatility"
    require_positive_momentum: bool = True
    output_dir: Path | None = None


def portfolio_and_watchlist_tickers() -> set[str]:
    """Return current portfolio holdings plus every watchlist ticker."""
    sheet = GSheet_Manager()
    net_shares: dict[str, float] = {}
    for row in sheet.fetch_transactions():
        ticker = str(row.get("ticker") or "").upper()
        try:
            shares = float(row.get("shares") or 0)
        except (TypeError, ValueError):
            shares = 0
        direction = -1 if str(row.get("transaction", "")).lower() == "sell" else 1
        if ticker:
            net_shares[ticker] = net_shares.get(ticker, 0) + direction * shares

    holdings = {ticker for ticker, shares in net_shares.items() if shares > 0}
    watchlist = {
        str(row["ticker"]).upper()
        for row in sheet.fetch_watchlist()
        if row.get("ticker")
    }
    if not holdings and not watchlist:
        raise RuntimeError(
            "No portfolio or watchlist tickers were returned from Google Sheets."
        )
    return holdings | watchlist


def daily_close(db: DB, ticker: str) -> pd.Series:
    rows = db.get(ticker, limit=50_000)
    if not rows:
        return pd.Series(dtype=float, name=ticker)
    frame = pd.DataFrame(rows)
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], errors="coerce")
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    frame = frame.dropna(subset=["timestamp", "close"]).sort_values("timestamp")
    if frame.empty:
        return pd.Series(dtype=float, name=ticker)
    # The final bar of each local session is the daily close used by the model.
    frame["date"] = (
        frame["timestamp"]
        .dt.tz_convert("Asia/Dubai")
        .dt.tz_localize(None)
        .dt.normalize()
    )
    return frame.groupby("date")["close"].last().rename(ticker)


def load_prices(tickers: set[str], config: Config) -> tuple[pd.DataFrame, list[str]]:
    db = DB()
    database_symbols = set(db.get_all_symbols())
    requested = sorted(tickers & database_symbols)
    absent = sorted(tickers - database_symbols)
    series = {ticker: daily_close(db, ticker) for ticker in requested}
    series = {ticker: price for ticker, price in series.items() if not price.empty}
    if not series:
        raise RuntimeError("None of the portfolio/watchlist tickers have price data.")

    newest_date = max(price.index.max() for price in series.values())
    fresh = {
        ticker: price
        for ticker, price in series.items()
        if len(price) >= config.min_history_days
        and (newest_date - price.index.max()).days <= config.max_stale_days
    }
    excluded = absent + sorted(set(requested) - set(fresh))
    if len(fresh) < 2:
        raise RuntimeError(
            "Fewer than two tickers meet the history/freshness requirements."
        )

    # Including every eligible asset means the newest eligible asset determines
    # the longest safe common backtest window.
    start = max(price.index.min() for price in fresh.values())
    end = min(price.index.max() for price in fresh.values())
    timeline = pd.DatetimeIndex(
        sorted(set().union(*(price.loc[start:end].index for price in fresh.values())))
    )
    prices = (
        pd.concat(fresh.values(), axis=1, sort=False).reindex(timeline).sort_index()
    )
    prices = prices.ffill(limit=config.max_stale_days).dropna(axis=1)
    if len(prices) <= config.lookback_days + 1:
        raise RuntimeError("The common history is shorter than the ranking lookback.")
    return prices, excluded


def target_weights(
    prices: pd.DataFrame, at: int, config: Config
) -> tuple[pd.DataFrame, pd.Series]:
    window = prices.iloc[at - config.lookback_days : at + 1]
    momentum = window.iloc[-1].div(window.iloc[0]).sub(1)
    volatility = window.pct_change(fill_method=None).std() * np.sqrt(252)
    ranking = pd.DataFrame({"momentum": momentum, "volatility": volatility}).dropna()
    if config.require_positive_momentum:
        ranking = ranking[ranking["momentum"] > 0]
    ranking = ranking.sort_values("momentum", ascending=False).head(config.top_n)

    weights = pd.Series(0.0, index=prices.columns)
    if not ranking.empty:
        if config.weighting == "equal":
            selected = pd.Series(1.0, index=ranking.index)
        else:
            selected = 1 / ranking["volatility"].replace(0, np.nan)
            selected = selected.fillna(1.0)
        weights.loc[selected.index] = selected / selected.sum()
    ranking["weight"] = weights.loc[ranking.index]
    ranking.index.name = "ticker"
    return ranking, weights


def run_backtest(
    prices: pd.DataFrame, config: Config
) -> tuple[pd.Series, pd.DataFrame, pd.DataFrame]:
    returns = prices.pct_change(fill_method=None).fillna(0.0)
    portfolio_returns = pd.Series(0.0, index=prices.index, name="portfolio_return")
    history: list[pd.DataFrame] = []

    for at in range(config.lookback_days, len(prices) - 1, config.rebalance_days):
        ranking, weights = target_weights(prices, at, config)
        next_at = min(at + config.rebalance_days, len(prices) - 1)
        portfolio_returns.iloc[at + 1 : next_at + 1] = (
            returns.iloc[at + 1 : next_at + 1].mul(weights).sum(axis=1)
        )
        rebalance = ranking.reset_index()
        rebalance.insert(0, "rebalance_date", prices.index[at].date().isoformat())
        history.append(rebalance)

    equity = (1 + portfolio_returns).cumprod().rename("equity")
    latest, _ = target_weights(prices, len(prices) - 1, config)
    return (
        equity,
        pd.concat(history, ignore_index=True) if history else pd.DataFrame(),
        latest.reset_index(),
    )


def print_table(frame: pd.DataFrame, columns: list[str]) -> None:
    if frame.empty:
        print("  None")
        return
    shown = frame[columns].copy()
    for column in ("momentum", "volatility", "weight"):
        if column in shown:
            shown[column] = shown[column].map(lambda value: f"{value:.2%}")
    widths = {
        column: max(len(column), *(len(str(value)) for value in shown[column]))
        for column in columns
    }
    print("  " + "  ".join(column.ljust(widths[column]) for column in columns))
    print("  " + "  ".join("-" * widths[column] for column in columns))
    for _, row in shown.iterrows():
        print(
            "  "
            + "  ".join(str(row[column]).ljust(widths[column]) for column in columns)
        )


def report(
    prices: pd.DataFrame,
    excluded: list[str],
    equity: pd.Series,
    latest: pd.DataFrame,
    config: Config,
) -> None:
    daily = equity.pct_change(fill_method=None).dropna()
    total_return = equity.iloc[-1] - 1
    annual_return = equity.iloc[-1] ** (252 / max(len(daily), 1)) - 1
    annual_volatility = daily.std() * np.sqrt(252)
    drawdown = equity.div(equity.cummax()).sub(1).min()

    print("\nRANKED ASSET ALLOCATION DEMO")
    print("=" * 58)
    print(
        f"Universe: {len(prices.columns)} eligible assets | {prices.index[0].date()} to {prices.index[-1].date()} ({len(prices)} sessions)"
    )
    print(
        f"Model: top {config.top_n} by {config.lookback_days}-session momentum | rebalance every {config.rebalance_days} sessions | {config.weighting} weights"
    )
    print(
        f"Eligibility: at least {config.min_history_days} daily observations; no more than {config.max_stale_days} days stale"
    )
    print("\nLATEST RANKING AND TARGET ALLOCATION")
    print_table(latest, ["ticker", "momentum", "volatility", "weight"])
    print("\nBACKTEST SUMMARY")
    print(f"  Total return:        {total_return:>9.2%}")
    print(f"  Annualised return:   {annual_return:>9.2%}")
    print(f"  Annualised volatility:{annual_volatility:>8.2%}")
    print(f"  Maximum drawdown:    {drawdown:>9.2%}")
    if excluded:
        print(
            f"\nExcluded ({len(excluded)}; no data, insufficient history, or stale): {', '.join(excluded)}"
        )


def parse_args() -> tuple[Config, set[str] | None]:
    parser = argparse.ArgumentParser(
        description="Rank portfolio/watchlist assets by momentum and backtest allocations."
    )
    parser.add_argument("--lookback-days", type=int, default=10)
    parser.add_argument("--rebalance-days", type=int, default=5)
    parser.add_argument("--top-n", type=int, default=7)
    parser.add_argument("--min-history-days", type=int, default=200)
    parser.add_argument("--max-stale-days", type=int, default=5)
    parser.add_argument(
        "--weighting",
        choices=["equal", "inverse-volatility"],
        default="inverse-volatility",
    )
    parser.add_argument(
        "--allow-negative",
        action="store_true",
        default=False,
        
        help="Allow assets with negative momentum to be allocated.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Optional directory for equity, rebalance, and latest-rank CSV files.",
    )
    parser.add_argument(
        "--tickers",
        help="Comma-separated local universe override; skips Google Sheets.",
    )
    args = parser.parse_args()
    if (
        min(args.lookback_days, args.rebalance_days, args.top_n, args.min_history_days)
        < 1
    ):
        parser.error("lookback, rebalance, top-n, and minimum history must be positive")
    config = Config(
        lookback_days=args.lookback_days,
        rebalance_days=args.rebalance_days,
        top_n=args.top_n,
        min_history_days=args.min_history_days,
        max_stale_days=args.max_stale_days,
        weighting=args.weighting,
        require_positive_momentum=not args.allow_negative,
        output_dir=args.output_dir,
    )
    manual_tickers = (
        {ticker.strip().upper() for ticker in args.tickers.split(",") if ticker.strip()}
        if args.tickers
        else None
    )
    return config, manual_tickers


def main() -> None:
    config, manual_tickers = parse_args()
    tickers = manual_tickers or portfolio_and_watchlist_tickers()
    prices, excluded = load_prices(tickers, config)
    equity, rebalances, latest = run_backtest(prices, config)
    report(prices, excluded, equity, latest, config)
    if config.output_dir:
        config.output_dir.mkdir(parents=True, exist_ok=True)
        equity.to_frame().to_csv(config.output_dir / "equity_curve.csv")
        rebalances.to_csv(config.output_dir / "rebalances.csv", index=False)
        latest.to_csv(config.output_dir / "latest_allocation.csv", index=False)
        print(f"\nSaved CSV output to {config.output_dir}")


if __name__ == "__main__":
    main()
