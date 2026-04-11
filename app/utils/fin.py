import math
import re
from typing import Optional


def parse_money(value: Optional[str]) -> tuple[float, str]:
    """
    Parse 'AED 12.25', 'GBX 2,201.00', '22.800 GBX' → (amount, currency).
    Currency is '' when not present (plain number string).
    """
    if not value:
        return 0.0, ""
    value = str(value).strip()
    # Prefix form: 'AED 12.25'
    m = re.match(r"^([A-Za-z]{2,4})\s+([\d,]+\.?\d*)$", value)
    if m:
        return float(m.group(2).replace(",", "")), m.group(1).upper()
    # Suffix form: '22.800 GBX'
    m = re.match(r"^([\d,]+\.?\d*)\s+([A-Za-z]{2,4})$", value)
    if m:
        return float(m.group(1).replace(",", "")), m.group(2).upper()
    # Plain number
    cleaned = re.sub(r"[^\d.]", "", value)
    return (float(cleaned) if cleaned else 0.0), ""


def safe_float(v) -> Optional[float]:
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v
