"""Shared cell rules for merging scraped sections: placeholders, periods, rescaling."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from typing import Any

from app.utils.parsers import parse_number

MISSING_CELL = "-"  # the source's own "no value" marker in tables
PLACEHOLDERS = frozenset({"", "-", "—", "n/a", "N/A", "NA", "None", "Upgrade"})

_PERIOD_RE = re.compile(r"^(?:FY\s*)?(\d{4})$")
_PLAIN_NUMBER_RE = re.compile(r"^-?(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?$")
_MONEY_NUMBER_RE = re.compile(r"\d[\d,]*(?:\.\d+)?")
_SLOT = "\0"


@dataclass
class MergeReport:
    notes: list[str] = field(default_factory=list)
    # Previous sections whose retained history was discarded; callers archive them.
    superseded: dict[str, Any] = field(default_factory=dict)


def section_failed(section: Any) -> bool:
    """Return whether a scraper section carries no usable result.

    An empty ``rows`` list is a valid result. A missing section, a captured
    error, or a table holding only error rows must never replace good data.
    """
    if not isinstance(section, dict) or "error" in section:
        return True
    rows = section.get("rows")
    return bool(rows) and all(isinstance(r, dict) and "error" in r for r in rows)


def valid_rows(rows: Any) -> list[dict]:
    if not isinstance(rows, list):
        return []
    return [r for r in rows if isinstance(r, dict) and "error" not in r]


def is_placeholder(value: Any) -> bool:
    return value is None or (isinstance(value, str) and value.strip() in PLACEHOLDERS)


def is_revision(new: Any, old: Any) -> bool:
    """Both sides carry a real value and the source changed it."""
    return not is_placeholder(new) and not is_placeholder(old) and new != old


def period_year(header: Any) -> int | None:
    """Fiscal-period columns ("FY 2024") accumulate; anything else is moving."""
    match = _PERIOD_RE.match(str(header).strip())
    return int(match.group(1)) if match else None


def _decimals(text: str) -> int:
    return len(text.split(".", 1)[1]) if "." in text else 0


def plain_number(value: Any) -> tuple[float, int] | None:
    """Parse "1,234.5" -> (1234.5, 1). Percentages and suffixed values are skipped."""
    if not isinstance(value, str) or not _PLAIN_NUMBER_RE.match(value.strip()):
        return None
    number = parse_number(value)
    return None if number is None else (number, _decimals(value.strip()))


def money_parts(value: Any) -> tuple[float, int, str] | None:
    """Split "0.630 AED" into (0.63, 3, "<slot> AED") so currencies compare."""
    if not isinstance(value, str) or not (m := _MONEY_NUMBER_RE.search(value)):
        return None
    number = parse_number(m.group(0))
    if number is None:
        return None
    template = value[: m.start()] + _SLOT + value[m.end() :]
    return number, _decimals(m.group(0)), template


def _format_scaled(number: float, decimals: int, factor: float, commas: bool) -> str:
    # Shrinking values need extra precision so e.g. a 20:1 split keeps detail.
    if factor < 1:
        decimals += math.ceil(math.log10(1 / factor))
    text = f"{number * factor:{',' if commas else ''}.{decimals}f}"
    return text.lstrip("-") if parse_number(text) == 0 else text


def rescale_cell(value: Any, factor: float) -> Any:
    if (parsed := plain_number(value)) is None:
        return value
    return _format_scaled(parsed[0], parsed[1], factor, "," in value)


def rescale_money(value: Any, factor: float) -> Any:
    if (parts := money_parts(value)) is None:
        return value
    scaled = _format_scaled(parts[0], parts[1], factor, "," in value)
    return parts[2].replace(_SLOT, scaled)


def consistent_factor(
    pairs: list[tuple[tuple[float, int], tuple[float, int]]],
) -> float | None:
    """Return the common new/old factor when every overlapping value moved by it.

    ``pairs`` holds ((new, decimals), (old, decimals)). Display rounding is
    allowed for, so a split visible on "0.28" vs "5.61" is still recognised.
    At least two agreeing pairs are required: one changed value is
    indistinguishable from an ordinary restatement.
    """
    usable = [(n, o) for n, o in pairs if n[0] != 0 and o[0] != 0]
    if len(usable) < 2:
        return None
    ratios = [n[0] / o[0] for n, o in usable]
    if any(r <= 0 for r in ratios):
        return None
    factor = sum(abs(n[0]) for n, _ in usable) / sum(abs(o[0]) for _, o in usable)
    tolerances = [
        0.005 + 0.5 * 10 ** -n[1] / abs(n[0]) + 0.5 * 10 ** -o[1] / abs(o[0])
        for n, o in usable
    ]
    agrees = all(abs(r / factor - 1) <= t for r, t in zip(ratios, tolerances))
    return factor if agrees and abs(factor - 1) > max(tolerances) else None
