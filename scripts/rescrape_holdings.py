"""Refresh every current holding serially without publishing failed dividends.

Run this while the scheduled worker is stopped.  It deliberately works one
ticker at a time to avoid source-site throttling, and only saves a ticker when
its dividend table was successfully loaded (an empty table is a valid result).
"""

from __future__ import annotations

import argparse
import asyncio
import fcntl
import sys
from pathlib import Path
from typing import Any

# Permit ``python scripts/rescrape_holdings.py`` from the repository root.
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.data.cache import Cache
from app.hql import HQL
from app.scraper.sa import StockAnalysisScraper
from app.utils.time_utils import dubai_now_iso


def _valid_dividends(value: Any) -> bool:
    """Accept a parsed table, including a genuine empty dividend history."""
    if not isinstance(value, dict) or not isinstance(value.get("rows"), list):
        return False
    return not any(isinstance(row, dict) and "error" in row for row in value["rows"])


async def _scrape_dividends_only(ticker: dict[str, Any]) -> dict[str, Any]:
    """Refresh just the dividend history without invoking browser-only pages."""
    scraper = StockAnalysisScraper()
    exchange = ticker["exchange"]
    symbol = ticker["symbol"]
    is_etf = scraper._has_etf_tag(ticker.get("tags"))
    url = f"{scraper._get_base_url(exchange, symbol, is_etf)}/dividend/"
    extracted = await asyncio.to_thread(scraper._fetch_dividend_html, url)
    if extracted is None:
        return scraper._empty_dividends(exchange, symbol, url)

    headers, rows = extracted
    for row in rows:
        for field in ("Ex-Dividend Date", "Record Date", "Pay Date"):
            if field in row:
                row[field] = scraper._to_iso_date(row[field])
    return {
        "symbol": symbol,
        "exchange": exchange.upper(),
        "url": url,
        "scraped_at": dubai_now_iso(),
        "headers": headers,
        "rows": rows,
    }


async def main(attempts: int, retry_delay: float, dividends_only: bool) -> int:
    hql = HQL()
    cache = Cache(read_only=False)
    holdings = hql.portfolio().holdings()
    tickers = holdings["ticker"].tolist()

    print(f"Refreshing {len(tickers)} current holdings serially.", flush=True)
    failures: list[str] = []

    for position, ticker in enumerate(tickers, start=1):
        existing = cache.load(ticker) or {}
        overview = existing.get("overview") or {}
        exchange = overview.get("exchange")
        symbol = overview.get("symbol")
        if not exchange or not symbol:
            print(
                f"[{position}/{len(tickers)}] {ticker}: skipped (missing symbol metadata)",
                flush=True,
            )
            failures.append(ticker)
            continue

        ticker_config = {
            "exchange": exchange,
            "symbol": symbol,
            "tags": existing.get("tags"),
        }
        saved = False
        for attempt in range(1, attempts + 1):
            print(
                f"[{position}/{len(tickers)}] {ticker}: scrape attempt {attempt}/{attempts}",
                flush=True,
            )
            try:
                if dividends_only:
                    dividends = await _scrape_dividends_only(ticker_config)
                    result = {
                        "ticker": ticker,
                        "scraped_at": dubai_now_iso(),
                        "dividends": dividends,
                    }
                else:
                    result = await StockAnalysisScraper().scrape(ticker_config)
                    dividends = result.get("dividends")
            except Exception as exc:
                print(
                    f"[{position}/{len(tickers)}] {ticker}: fetch failed ({exc})",
                    flush=True,
                )
                dividends = None
                result = {}
            if _valid_dividends(dividends):
                result["purchase_details"] = existing.get("purchase_details") or []
                if cache.save(ticker, result):
                    print(
                        f"[{position}/{len(tickers)}] {ticker}: saved "
                        f"{len(dividends['rows'])} dividend rows",
                        flush=True,
                    )
                    saved = True
                    break
                print(
                    f"[{position}/{len(tickers)}] {ticker}: cache save failed",
                    flush=True,
                )
            else:
                print(
                    f"[{position}/{len(tickers)}] {ticker}: dividend table unavailable; "
                    "not publishing this scrape",
                    flush=True,
                )

            if attempt < attempts:
                await asyncio.sleep(retry_delay)

        if not saved:
            failures.append(ticker)

    if failures:
        print(f"Finished with failed tickers: {', '.join(failures)}", flush=True)
        return 1

    print("Finished successfully.", flush=True)
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--attempts", type=int, default=3)
    parser.add_argument("--retry-delay", type=float, default=60.0)
    parser.add_argument(
        "--dividends-only",
        action="store_true",
        help="Refresh only dividend history, retaining other cached sections.",
    )
    args = parser.parse_args()

    lock_path = ROOT_DIR / "cache" / "holdings-rescrape.lock"
    with lock_path.open("w") as lock_file:
        try:
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("Another holdings rebuild is already running.", flush=True)
            raise SystemExit(2)
        raise SystemExit(
            asyncio.run(main(args.attempts, args.retry_delay, args.dividends_only))
        )
