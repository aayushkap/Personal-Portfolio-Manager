from __future__ import annotations

import re
from datetime import date
from typing import Any

_SUFFIXES = {"K": 1e3, "M": 1e6, "B": 1e9, "T": 1e12}


def parse_date(value: Any) -> date | None:
    if not value or str(value).strip() in {"", "-", "n/a", "None"}:
        return None
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value).strip())
    except ValueError:
        return None


def parse_number(value: Any) -> float | None:
    if value is None:
        return None
    s = str(value).strip().replace(",", "")
    if s in {"", "-", "n/a", "None"}:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def parse_percent(value: Any) -> float | None:
    s = str(value).strip()
    if not s or s in {"-", "n/a", "None"}:
        return None
    if s.endswith("%"):
        n = parse_number(s[:-1])
        return n / 100 if n is not None else None
    return None


def parse_suffix_number(value: Any) -> float | None:
    s = str(value).strip().replace(",", "")
    if not s or s in {"-", "n/a", "None"}:
        return None
    m = re.fullmatch(r"([-+]?\d*\.?\d+)([KMBT])", s)
    if not m:
        return parse_number(s)
    num = float(m.group(1))
    mult = _SUFFIXES[m.group(2)]
    return num * mult


def parse_money_string(value: Any) -> tuple[float | None, str | None]:
    s = str(value).strip()
    if not s or s in {"-", "n/a", "None"}:
        return None, None
    m = re.search(r"([A-Z]{3})\s*([\d,.\-]+)|([\d,.\-]+)\s*([A-Z]{3})", s)
    if not m:
        return parse_number(s), None
    if m.group(1) and m.group(2):
        return parse_number(m.group(2)), m.group(1)
    return parse_number(m.group(3)), m.group(4)


def parse_range(value: Any) -> tuple[float | None, float | None]:
    s = str(value).strip()
    if not s or s in {"-", "n/a", "None"}:
        return None, None
    parts = [p.strip() for p in s.split("-")]
    if len(parts) != 2:
        return None, None
    return parse_number(parts[0]), parse_number(parts[1])


def parse_price_target(value: Any) -> dict[str, float | None]:
    s = str(value).strip()
    if not s or s in {"-", "n/a", "None"}:
        return {"value": None, "upside": None}
    m = re.match(r"\s*([\d,.]+)\s*\(([-+]?[\d,.]+)%\)\s*", s)
    if not m:
        return {"value": parse_number(s), "upside": None}
    return {
        "value": parse_number(m.group(1)),
        "upside": (
            parse_number(m.group(2)) / 100
            if parse_number(m.group(2)) is not None
            else None
        ),
    }


def parse_mixed_stat(value: Any) -> dict[str, float | None]:
    s = str(value).strip()
    if not s or s in {"-", "n/a", "None"}:
        return {"value": None, "change": None}
    m = re.match(r"\s*([\d,.]+[KMBT]?)\s+([-+]?[\d,.]+%)\s*$", s)
    if not m:
        return {"value": parse_suffix_number(s), "change": None}
    return {
        "value": parse_suffix_number(m.group(1)),
        "change": parse_percent(m.group(2)),
    }


def parse_any_stat(value: Any) -> Any:
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    if s in {"", "-", "n/a", "None"}:
        return None
    if "%" in s and re.fullmatch(r"[-+]?[\d,.]+%", s):
        return parse_percent(s)
    if re.fullmatch(r"[-+]?[\d,.]+[KMBT]", s.replace(",", "")):
        return parse_suffix_number(s)
    if re.fullmatch(r"[-+]?[\d,.]+", s.replace(",", "")):
        return parse_number(s)
    return s
