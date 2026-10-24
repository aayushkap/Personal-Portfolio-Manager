"""Merge dividend history so ex-date events accumulate."""

from __future__ import annotations

import re

from app.data.merge.values import (
    MergeReport,
    consistent_factor,
    is_placeholder,
    is_revision,
    money_parts,
    rescale_money,
    valid_rows,
)

EX_DATE = "Ex-Dividend Date"
CASH_AMOUNT = "Cash Amount"
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _group_by_ex_date(rows: list[dict]) -> dict[str, list[dict]]:
    """Group events by ex-date; a special and a regular dividend can share one."""
    groups: dict[str, list[dict]] = {}
    for row in rows:
        ex_date = row.get(EX_DATE)
        if isinstance(ex_date, str) and _ISO_DATE_RE.match(ex_date):
            groups.setdefault(ex_date, []).append(row)
    return groups


def _single(groups: dict[str, list[dict]], ex_date: str) -> dict | None:
    group = groups.get(ex_date) or []
    return group[0] if len(group) == 1 else None


def merge_dividends(old: dict, new: dict, report: MergeReport) -> dict:
    old_rows, new_rows = valid_rows(old.get("rows")), valid_rows(new.get("rows"))
    if not old_rows:
        return new
    if not new_rows:
        # An empty table (or "no TTM dividend") does not un-pay past dividends.
        report.notes.append("dividends: source returned no rows; kept cached history")
        return old

    # Events older than the fresh window are retained; events inside it that
    # the source no longer lists were withdrawn by the source and are removed.
    new_groups, old_groups = _group_by_ex_date(new_rows), _group_by_ex_date(old_rows)
    floor = min(new_groups) if new_groups else None
    gone = [d for d in old_groups if d not in new_groups]
    archived = sorted((d for d in gone if floor is None or d < floor), reverse=True)
    if dropped := [d for d in gone if d not in archived]:
        report.notes.append(f"dividends: source no longer lists {dropped}; removed")

    # Split-adjusted history shows up as every overlapping amount moving by
    # the same factor (in the same currency); retained events are rebased.
    pairs, revised = [], 0
    for ex_date in new_groups:
        current, prior = _single(new_groups, ex_date), _single(old_groups, ex_date)
        if current is None or prior is None:
            continue
        revised += any(is_revision(v, prior.get(k)) for k, v in current.items())
        n = money_parts(current.get(CASH_AMOUNT))
        o = money_parts(prior.get(CASH_AMOUNT))
        if n and o and n[2] == o[2]:
            pairs.append(((n[0], n[1]), (o[0], o[1])))
    factor = consistent_factor(pairs) if archived else None
    if revised:
        report.notes.append(f"dividends: {revised} cached event(s) updated by source")
    if factor is not None:
        report.notes.append(f"dividends: rebased retained events by {factor:.6g}")

    new_headers = list(new.get("headers") or [])
    extra = [h for h in old.get("headers") or [] if h not in new_headers]
    headers = new_headers + extra

    def fill(row: dict, prior: dict | None) -> dict:
        out = {h: row.get(h) for h in headers}
        if prior is not None:
            # A placeholder never erases a known value (e.g. a pay date).
            for h in headers:
                if is_placeholder(out[h]) and not is_placeholder(prior.get(h)):
                    out[h] = prior[h]
        out.update({k: v for k, v in row.items() if k not in out})
        return out

    rows = []
    for row in new_rows:
        ex_date = row.get(EX_DATE)
        paired = _single(new_groups, ex_date) is row
        rows.append(fill(row, _single(old_groups, ex_date) if paired else None))
    for ex_date in archived:
        for row in old_groups[ex_date]:
            row = fill(row, None)
            if factor is not None:
                row[CASH_AMOUNT] = rescale_money(row.get(CASH_AMOUNT), factor)
            rows.append(row)

    return {**new, "headers": headers, "rows": rows}
