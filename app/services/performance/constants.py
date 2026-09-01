# app/services/performance/constants.py

from __future__ import annotations

DEFAULT_BENCHMARK_INDEX = "TVC:SPX"

# The cache uses TradingView-style exchange prefixes. Any exchange without an
# explicit regional index (NSE, TSX, XETR, CPH, TSE, ...) falls back to the
# S&P 500 rather than being excluded from the blend/beta calculations.
EXCHANGE_BENCHMARKS: dict[str, str] = {
    "DFM": "DFM:DFMGI",
    "ADX": "ADX:FADGI",
    "LSE": "FTSE-UKX",
    "FTSE": "FTSE-UKX",
    "LON": "FTSE-UKX",
    "NYSE": DEFAULT_BENCHMARK_INDEX,
    "NASDAQ": DEFAULT_BENCHMARK_INDEX,
    "AMEX": DEFAULT_BENCHMARK_INDEX,
    "NYSEARCA": DEFAULT_BENCHMARK_INDEX,
    "ARCA": DEFAULT_BENCHMARK_INDEX,
    "BATS": DEFAULT_BENCHMARK_INDEX,
    "CBOE": DEFAULT_BENCHMARK_INDEX,
}

MIN_HISTORY_DAYS = 20


def mapped_benchmark(exchange: object) -> str:
    """Every exchange resolves to a benchmark -- explicitly mapped exchanges
    use their own regional index, anything else defaults to the S&P 500."""
    value = str(exchange or "").strip().upper()
    return EXCHANGE_BENCHMARKS.get(value, DEFAULT_BENCHMARK_INDEX)


def yield_on_cost_by_ticker(p) -> dict[str, float]:
    """All-time dividends received over all-time buy cost, per ticker."""
    tx = p.transactions()
    if tx.empty:
        return {}
    buys = tx[tx["transaction"].str.lower() == "buy"]
    cost_by_ticker = buys.groupby("ticker")["total_cost_aed"].sum()

    divs = p.dividends()
    received_by_ticker = (
        divs[divs["status"] == "received"].groupby("ticker")["total_aed"].sum()
        if not divs.empty
        else {}
    )

    result = {}
    for ticker, cost in cost_by_ticker.items():
        if cost:
            result[ticker] = round(received_by_ticker.get(ticker, 0.0) / cost * 100, 2)
    return result
