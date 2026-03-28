from datetime import datetime, date
from zoneinfo import ZoneInfo

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
