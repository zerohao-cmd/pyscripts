from datetime import datetime, timezone, timedelta, date
from zoneinfo import ZoneInfo


if __name__ == "__main__":
    # tz = timezone(timedelta(hours=8))
    tz = ZoneInfo("Asia/Shanghai")
    dt1 = datetime(2025, 6, 15, 12, 0, tzinfo=tz)
    dt2 = datetime(2025, 6, 15, 12, 0)
    dt3 = date(2025, 6, 15)
    print(dt1.isoformat())
    print(dt2.isoformat())
    print(dt3.isoformat())
