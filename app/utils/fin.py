import math
from typing import Optional

from app.utils.parsers import parse_money_string


def parse_money(value: Optional[str]) -> tuple[float, str]:
    """
    Parse ISO-code or symbol-denominated cash amounts into (amount, currency).
    Currency is '' when not present (plain number string).
    """
    amount, currency = parse_money_string(value)
    return amount or 0.0, currency or ""


def safe_float(v) -> Optional[float]:
    if v is None:
        return None
    try:
        n = float(v)
        return None if (math.isnan(n) or math.isinf(n)) else n
    except (TypeError, ValueError):
        return None
