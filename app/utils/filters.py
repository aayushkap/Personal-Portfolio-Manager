# app/utils/filters.py

from __future__ import annotations
from datetime import date
import pandas as pd
from pathlib import Path

from app.data.cache import Cache
from app.data.db import DB

cache = Cache()
db = DB()


def get_all_transactions() -> pd.DataFrame:
    """
    Reads purchase_details from every cached JSON.
    Returns a clean DataFrame with parsed types.
    """
    rows = []
    for path in Path(cache.cache_dir).glob("*.json"):
        ticker_key = path.stem.replace("_", ":", 1).upper()  # dfm_dewa -> DFM:DEWA
        data = cache.load(ticker_key)
        if not data or "purchase_details" not in data:
            continue
        for t in data["purchase_details"]:
            rows.append(
                {
                    "ticker": f"{t['exchange']}:{t['symbol']}",
                    "action": t["transaction"].upper(),  # BUY / SELL
                    "trade_date": pd.to_datetime(
                        t["purchase date"], format="%m/%d/%Y"
                    ).date(),
                    "shares": float(t["shares"]),
                    "price_aed": float(
                        str(t["cost per share"])
                        .replace("AED", "")
                        .replace(",", "")
                        .strip()
                    ),
                    "commission_aed": float(
                        str(t["commision paid"])
                        .replace("AED", "")
                        .replace(",", "")
                        .strip()
                    ),
                    "total_cost_aed": float(
                        str(t["total cost"]).replace("AED", "").replace(",", "").strip()
                    ),
                    "platform": t.get("platform"),
                    "sector": t.get("sector"),
                    "exchange": t.get("exchange"),
                }
            )

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows).sort_values("trade_date").reset_index(drop=True)
    # Sells become negative shares for net position calculations
    df["signed_shares"] = df.apply(
        lambda r: r["shares"] if r["action"] == "BUY" else -r["shares"], axis=1
    )
    return df


def get_holdings_on_date(
    as_of: date,
    tickers: list[str] | None = None,
    transactions: pd.DataFrame | None = None,
) -> dict[str, float]:
    """
    Returns {ticker: shares_held} as of a given date.
    Pass in transactions df to avoid re-reading cache in loops.
    """
    tx = transactions if transactions is not None else get_all_transactions()
    if tx.empty:
        return {}

    mask = tx["trade_date"] <= as_of
    if tickers:
        mask &= tx["ticker"].isin(tickers)

    held = tx[mask].groupby("ticker")["signed_shares"].sum()
    # Only return positions with shares > 0 (exclude fully sold positions)
    return held[held > 0].to_dict()


def get_price_series(
    tickers: list[str],
    start: date,
    end: date,
) -> pd.DataFrame:
    """
    Returns a pivoted DataFrame: index=date, columns=ticker, values=close.
    Only includes trading days present in OHLC.
    """
    frames = []
    for ticker in tickers:
        rows = db.get(ticker, limit=10_000)  # get full history, filter below
        if not rows:
            continue
        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["timestamp"]).dt.date
        df = df[(df["date"] >= start) & (df["date"] <= end)]
        df = df.groupby("date")["close"].last().reset_index()  # EOD close
        df["ticker"] = ticker
        frames.append(df)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames)
    pivoted = combined.pivot(index="date", columns="ticker", values="close")
    pivoted.index = pd.to_datetime(pivoted.index)
    return pivoted.sort_index()


def get_dividend_events(
    tickers: list[str] | None = None,
) -> pd.DataFrame:
    """
    Returns all historical dividend rows from cached JSONs.
    Does NOT filter by holdings — that's the caller's responsibility.
    """
    rows = []
    all_tickers = tickers or [
        path.stem.replace("_", ":", 1).upper()
        for path in Path(cache.cache_dir).glob("*.json")
    ]
    for ticker in all_tickers:
        data = cache.load(ticker)
        if not data or "dividends" not in data:
            continue
        div_data = data["dividends"]
        for row in div_data.get("rows", []):
            try:
                rows.append(
                    {
                        "ticker": ticker,
                        "ex_date": pd.to_datetime(row["Ex-Dividend Date"]).date(),
                        "pay_date": pd.to_datetime(row["Pay Date"]).date(),
                        "amount_aed": float(
                            str(row["Cash Amount"]).replace("AED", "").strip()
                        ),
                    }
                )
            except Exception:
                continue

    return pd.DataFrame(rows).sort_values("ex_date") if rows else pd.DataFrame()


def get_dividends_received(
    start: date,
    end: date,
    tickers: list[str] | None = None,
    transactions: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Returns dividends YOU personally received: cross-references
    ex-dates with your holdings on that date.
    Returns: {ticker, ex_date, pay_date, amount_aed, shares_held, total_received_aed}
    """
    tx = transactions if transactions is not None else get_all_transactions()
    dividends = get_dividend_events(tickers)
    if dividends.empty or tx.empty:
        return pd.DataFrame()

    results = []
    mask = (dividends["ex_date"] >= start) & (dividends["ex_date"] <= end)
    for _, div in dividends[mask].iterrows():
        holdings = get_holdings_on_date(
            div["ex_date"],
            tickers=[div["ticker"]] if not tickers else tickers,
            transactions=tx,
        )
        shares = holdings.get(div["ticker"], 0)
        if shares > 0:
            results.append(
                {
                    **div.to_dict(),
                    "shares_held": shares,
                    "total_received_aed": shares * div["amount_aed"],
                }
            )

    return pd.DataFrame(results) if results else pd.DataFrame()
