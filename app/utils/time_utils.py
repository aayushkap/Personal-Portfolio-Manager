from __future__ import annotations
from datetime import datetime, date
from zoneinfo import ZoneInfo
from typing import Optional


DUBAI_TZ = ZoneInfo("Asia/Dubai")
UTC_TZ = ZoneInfo("UTC")


def dubai_now() -> datetime:
    return datetime.now(DUBAI_TZ)


def dubai_today() -> date:
    return dubai_now().date()


def dubai_now_iso() -> str:
    return dubai_now().isoformat()


def to_dubai(dt: datetime, assume_tz=UTC_TZ) -> datetime:
    """
    Convert datetime to Asia/Dubai.
    If naive, assume `assume_tz` first.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=assume_tz)
    return dt.astimezone(DUBAI_TZ)


DATE_FORMATS = (
    "%Y-%m-%d",  # 2025-10-26
    "%d/%m/%Y",  # 26/10/2025
    "%m/%d/%Y",  # 10/26/2025
    "%d-%m-%Y",  # 26-10-2025
    "%m-%d-%Y",  # 10-26-2025
    "%d %b %Y",  # 26 Oct 2025
    "%d %B %Y",  # 26 October 2025
    "%b %d, %Y",  # Oct 26, 2025
    "%B %d, %Y",  # October 26, 2025
)


def parse_flexible_date(value: object) -> Optional[date]:
    if value is None:
        return None

    s = str(value).strip()
    if not s or s in {"-", "—", "N/A", "None", "null"}:
        return None

    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass

    raise ValueError(f"Unsupported date format: {value}")


def normalise_date(value: object) -> Optional[str]:
    parsed = parse_flexible_date(value)
    return parsed.isoformat() if parsed else None
