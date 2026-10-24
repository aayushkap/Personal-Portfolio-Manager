"""
Merge a fresh StockAnalysis scrape into the cached ticker document.

The source only shows a rolling window (~5 fiscal years / ~5 years of
dividends), so overwriting loses the oldest period on every roll. Each section
is merged according to what it represents:

- overview, statistics: point-in-time snapshots -> replaced.
- financials, ratios: fiscal-year columns accumulate; moving columns (TTM,
  Current) are replaced.
- dividends: ex-date events accumulate.

Inside the window a fresh scrape covers, the source is authoritative: restated
values win and periods/events it no longer lists are removed. Only data older
than that window is retained. Retained values are rescaled when the source
rebases history consistently (a split), and discarded (for archiving) when it
rebases inconsistently. The scraper's JSON shape is kept exactly.
"""

from __future__ import annotations

from typing import Any

from app.data.merge.dividends import merge_dividends
from app.data.merge.tables import merge_table, return_metric_rows
from app.data.merge.values import MergeReport, period_year, section_failed

__all__ = [
    "MergeReport",
    "merge_dividends",
    "merge_table",
    "merge_ticker",
    "period_year",
    "return_metric_rows",
    "section_failed",
]

SCRAPED_SECTIONS = ("overview", "financials", "dividends", "statistics", "ratios")


def merge_ticker(
    previous: Any, incoming: dict[str, Any]
) -> tuple[dict[str, Any], MergeReport]:
    """Merge ``incoming`` into ``previous`` without mutating either."""
    report = MergeReport()
    merged = dict(incoming)
    if not isinstance(previous, dict):
        return merged, report

    for name in SCRAPED_SECTIONS:
        new, old = merged.get(name), previous.get(name)
        old_ok = not section_failed(old)
        if section_failed(new):
            if old_ok:
                merged[name] = old
                report.notes.append(f"{name}: refresh failed; kept last good section")
            continue
        if not old_ok:
            continue
        if name in ("financials", "ratios"):
            merged[name] = merge_table(old, new, report, name)
        elif name == "dividends":
            merged[name] = merge_dividends(old, new, report)
        # overview and statistics are snapshots: the fresh section replaces.
    return merged, report
