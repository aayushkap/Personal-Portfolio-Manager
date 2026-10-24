"""Merge annual tables (financials, ratios) so fiscal-year columns accumulate."""

from __future__ import annotations

from typing import Any, Iterable

from app.data.merge.values import (
    MISSING_CELL,
    MergeReport,
    consistent_factor,
    is_placeholder,
    is_revision,
    period_year,
    plain_number,
    rescale_cell,
    valid_rows,
)

RETURN_METRIC_KEYWORDS = ("ROE", "ROA", "ROIC", "ROCE", "RETURN")

# A table counts as rebased wholesale when most comparable overlapping cells
# moved and no per-row factor explains them (e.g. a reporting-currency switch
# restated at historical rates). A normal restatement touches a minority.
_BREAK_MIN_CELLS = 8
_BREAK_CHANGED_SHARE = 0.75
_CHANGE_TOLERANCE = 0.02


def return_metric_rows(rows: list[dict], headers: list[str]) -> list[dict]:
    label = headers[0] if headers else "col_0"
    return [
        r
        for r in rows
        if any(kw in str(r.get(label, "")).upper() for kw in RETURN_METRIC_KEYWORDS)
    ]


def _keyed_rows(rows: Iterable[dict], label: str) -> list[tuple[Any, dict]]:
    """Key rows by (line item, occurrence) so repeated labels stay distinct."""
    seen: dict[str, int] = {}
    out = []
    for row in rows:
        name = row.get(label)
        if isinstance(name, str) and name.strip():
            seen[name] = seen.get(name, 0) + 1
            out.append(((name, seen[name]), row))
        else:
            out.append((None, row))
    return out


def merge_table(old: dict, new: dict, report: MergeReport, name: str) -> dict:
    old_headers = list(old.get("headers") or [])
    new_headers = list(new.get("headers") or [])
    old_rows, new_rows = valid_rows(old.get("rows")), valid_rows(new.get("rows"))

    if not old_rows or not old_headers:
        return new
    if not new_rows:
        report.notes.append(f"{name}: source returned an empty table; kept cache")
        return old
    label = new_headers[0] if new_headers else None
    if label != old_headers[0]:
        report.notes.append(f"{name}: label column changed to {label!r}; replaced")
        return new

    # Years older than the fresh window are retained; years inside it that the
    # source no longer lists were withdrawn by the source and are removed.
    new_years = [y for h in new_headers if (y := period_year(h)) is not None]
    floor = min(new_years) if new_years else None
    gone = [
        h for h in old_headers if period_year(h) is not None and h not in new_headers
    ]
    archived = [h for h in gone if floor is None or period_year(h) < floor]
    if dropped := [h for h in gone if h not in archived]:
        report.notes.append(f"{name}: source no longer lists {dropped}; removed")
    overlap = [
        h for h in new_headers if period_year(h) is not None and h in old_headers
    ]

    old_keyed = _keyed_rows(old_rows, label)
    old_by_key = {k: r for k, r in old_keyed if k is not None}
    new_keyed = _keyed_rows(new_rows, label)

    factors: dict[Any, float] = {}
    comparable = changed = revised = 0
    for key, new_row in new_keyed:
        if (old_row := old_by_key.get(key)) is None:
            continue
        revised += sum(is_revision(new_row.get(h), old_row.get(h)) for h in overlap)
        pairs = [
            (n, o)
            for h in overlap
            if (n := plain_number(new_row.get(h)))
            and (o := plain_number(old_row.get(h)))
        ]
        factor = consistent_factor(pairs) if archived else None
        if factor is not None:
            factors[key] = factor
        for n, o in pairs:
            if o[0] != 0:
                comparable += 1
                changed += factor is None and abs(n[0] / o[0] - 1) > _CHANGE_TOLERANCE

    if revised:
        report.notes.append(f"{name}: {revised} cached value(s) restated by source")
    if factors:
        report.notes.append(f"{name}: rescaled retained years on {len(factors)} row(s)")
    if archived and comparable >= _BREAK_MIN_CELLS:
        if changed / comparable >= _BREAK_CHANGED_SHARE:
            report.notes.append(
                f"{name}: source rebased history inconsistently; dropped {archived}"
            )
            report.superseded[name] = old
            archived, factors = [], {}

    archived.sort(key=period_year, reverse=True)
    headers = new_headers + archived

    def build(key: Any, new_row: dict | None, old_row: dict | None) -> dict:
        row = {}
        for h in headers:
            if h in archived:
                value = old_row.get(h, MISSING_CELL) if old_row else MISSING_CELL
                if key in factors:
                    value = rescale_cell(value, factors[key])
            elif new_row is None:
                value = old_row.get(h, MISSING_CELL) if h == label else MISSING_CELL
            else:
                value = new_row.get(h, MISSING_CELL)
                # A placeholder never erases a real fiscal-year value; moving
                # columns are always the latest source reading.
                if (
                    old_row is not None
                    and period_year(h) is not None
                    and is_placeholder(value)
                    and not is_placeholder(old_row.get(h))
                ):
                    value = old_row[h]
            row[h] = value
        if new_row is not None:
            row.update({k: v for k, v in new_row.items() if k not in row})
        return row

    rows = [build(key, new_row, old_by_key.get(key)) for key, new_row in new_keyed]
    keys = [key for key, _ in new_keyed]

    # Line items the source dropped survive only for their retained years,
    # placed after the cached neighbour they used to follow.
    cursor = 0
    for key, old_row in old_keyed:
        if key is not None and key in keys:
            cursor = keys.index(key) + 1
        elif key is not None and any(
            not is_placeholder(old_row.get(h)) for h in archived
        ):
            rows.insert(cursor, build(key, None, old_row))
            keys.insert(cursor, key)
            cursor += 1

    merged = {**new, "headers": headers, "rows": rows}
    if "return_metrics_rows" in new or "return_metrics_rows" in old:
        merged["return_metrics_rows"] = return_metric_rows(rows, headers)
    return merged
