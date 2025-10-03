from typing import List, Dict

from .sheets import Sheets
from .utils import ttl_cache


sheets = Sheets()


@ttl_cache(30)
async def cached_services() -> Dict[str, dict]:
    return await sheets.services()


@ttl_cache(20)
async def cached_times() -> List[str]:
    return await sheets.times()


@ttl_cache(8)
async def cached_taken() -> List[str]:
    return await sheets.taken_slots()


@ttl_cache(8)
async def cached_bookings() -> List[dict]:
    return await sheets.bookings()

