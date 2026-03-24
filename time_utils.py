from datetime import datetime, date
from zoneinfo import ZoneInfo

DUBAI_TZ = ZoneInfo("Asia/Dubai")


def dubai_now() -> datetime:
    return datetime.now(DUBAI_TZ)


def dubai_today() -> date:
    return dubai_now().date()


def dubai_now_iso() -> str:
    return dubai_now().isoformat()
