from datetime import timedelta
from typing import List, Optional

from .config import DISPLAY_MIN_HOURS, UNAVAILABLE_BEFORE_HOURS
from .services import cached_bookings
from .utils import now, parse_dt


async def get_taken_times(
    exclude_client_id: Optional[str] = None, min_hours: Optional[int] = None
) -> List[str]:
    bookings = await cached_bookings()
    out: List[str] = []
    for b in bookings:
        dt_str = None
        client_id = None
        for k, v in b.items():
            if "дата" in k.lower() and "врем" in k.lower():
                dt_str = v
                break
        for k in b.keys():
            if "id" in k.lower() and "клиент" in k.lower():
                client_id = b.get(k)
                break
        if not dt_str:
            continue
        try:
            dt = parse_dt(dt_str)
        except Exception:
            continue
        if dt <= now():
            continue
        if min_hours is not None and dt <= now() + timedelta(hours=min_hours):
            continue
        if (
            exclude_client_id is not None
            and client_id is not None
            and str(client_id).strip() == str(exclude_client_id).strip()
        ):
            continue
        out.append(dt_str)
    out.sort(key=lambda t: parse_dt(t))
    return out


async def filter_available_times(all_times: List[str], taken: List[str]) -> List[str]:
    available = []
    for t in all_times:
        try:
            dt = parse_dt(t)
        except Exception:
            continue
        if dt <= now():
            continue
        if dt <= now() + timedelta(hours=DISPLAY_MIN_HOURS):
            continue
        if t in taken:
            continue
        available.append((dt, t))
    available.sort(key=lambda x: x[0])
    return [t for _, t in available]

