# app/hql/queries/__init__.py

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import pandas as pd

from app.utils.parsers import (
    parse_any_stat,
    parse_date,
)

from app.hql.constants import FIELD_MAP
from app.hql.errors import HQLFieldError


def _coerce_date_range(
    days: int | None = None,
    start: date | str | None = None,
    end: date | str | None = None,
) -> tuple[date, date]:
    end_date = parse_date(end) if end else date.today()
    if start:
        start_date = parse_date(start)
    else:
        days = 365 if days is None else days
        start_date = end_date - timedelta(days=days)

    if start_date is None or end_date is None:
        raise ValueError("Could not resolve date range.")
    return start_date, end_date


def _first_purchase_detail(raw: dict) -> dict | None:
    details = raw.get("purchase_details") or []
    return details[0] if details else None


def _overview_stats(raw: dict) -> dict[str, Any]:
    return ((raw.get("overview") or {}).get("stats") or {}).copy()


def _statistics_sections(raw: dict) -> dict[str, Any]:
    return ((raw.get("statistics") or {}).get("sections") or {}).copy()


def _normalize_stat_block(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _normalize_stat_block(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_normalize_stat_block(v) for v in value]
    return parse_any_stat(value)


def _tabular_rows_to_df(rows: list[dict], headers: list[str]) -> pd.DataFrame:
    """
    Convert cache tabular rows into a normalized DataFrame.

    Input shape:
        [
            {"Fiscal Year": "Revenue", "TTM": "51,858", "FY 2025": "49,557"},
            ...
        ]

    Output shape:
        index   -> metric name (e.g. "Revenue")
        columns -> period labels (e.g. "TTM", "FY 2025", ...)
        values  -> parsed float/str/None
    """
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows).copy()
    metric_col = "Fiscal Year"
    if metric_col not in df.columns:
        return pd.DataFrame()

    df = df.set_index(metric_col)

    ordered_cols = [c for c in headers if c in df.columns and c != metric_col]
    if ordered_cols:
        df = df[ordered_cols]

    for col in df.columns:
        df[col] = df[col].map(parse_any_stat)

    return df


def _coerce_period_df(
    df: pd.DataFrame, period: str = "all"
) -> pd.DataFrame | pd.Series:
    """
    period='all'     -> full DataFrame
    period='ttm'     -> Series from TTM column
    period='current' -> Series from Current column
    period='FY 2025' -> Series from exact matching column
    """
    if df.empty:
        return df

    if period == "all":
        return df

    wanted = period.strip().lower()
    col_map = {str(c).strip().lower(): c for c in df.columns}

    if wanted == "ttm" and "ttm" in col_map:
        return df[col_map["ttm"]]
    if wanted == "current" and "current" in col_map:
        return df[col_map["current"]]
    if wanted in col_map:
        return df[col_map[wanted]]

    return pd.Series(dtype=float)


def _extract_field_from_raw(raw: dict, field: str) -> Any:
    """
    Resolve a canonical HQL field alias from FIELD_MAP against raw statistics.
    """
    if field not in FIELD_MAP:
        raise HQLFieldError(field)

    root, section, label = FIELD_MAP[field]

    if root != "statistics":
        return None

    sections = _statistics_sections(raw)
    value = (sections.get(section) or {}).get(label)
    return parse_any_stat(value)
