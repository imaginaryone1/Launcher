import asyncio
import logging
from datetime import timedelta
from typing import Any, Dict, List, Optional, Tuple

import gspread
from oauth2client.service_account import ServiceAccountCredentials

from .config import GS_BACKOFF, GS_RETRY, SPREADSHEET_KEY, CREDS_FILE, UNAVAILABLE_BEFORE_HOURS
from .utils import fmt_dt, parse_dt, now


log = logging.getLogger("LyuNailsBot")


class Sheets:
    def __init__(self, creds_file: str = CREDS_FILE, key: str = SPREADSHEET_KEY):
        self.creds_file = creds_file
        self.key = key
        self.scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        self._gc = None
        self._sheet = None

    def _init_sync(self):
        if self._gc:
            return
        creds = ServiceAccountCredentials.from_json_keyfile_name(self.creds_file, self.scope)
        self._gc = gspread.authorize(creds)
        self._sheet = self._gc.open_by_key(self.key)
        # ensure worksheets exist
        for name in ("Клиенты", "Услуги", "ВремяЗаписей", "Записи", "ПойматьОчередь", "Настройки"):
            try:
                self._sheet.worksheet(name)
            except Exception:
                try:
                    self._sheet.add_worksheet(title=name, rows="1000", cols="20")
                    log.info("Created worksheet %s", name)
                except Exception:
                    pass

    async def _run(self, func, *a, **k):
        loop = asyncio.get_running_loop()
        last = None
        for attempt in range(1, GS_RETRY + 1):
            try:
                return await loop.run_in_executor(None, lambda: func(*a, **k))
            except Exception as e:  # pragma: no cover - network retries
                log.warning("gspread attempt %s failed: %s", attempt, e)
                await asyncio.sleep(GS_BACKOFF * (2 ** (attempt - 1)))
                last = e
        if last:
            raise last

    async def _ws(self, name: str):
        await self._run(self._init_sync)
        return self._sheet.worksheet(name)

    async def get_all(self, name: str):
        ws = await self._ws(name)
        return await self._run(ws.get_all_values)

    async def append(self, name: str, row: List[Any]):
        ws = await self._ws(name)
        return await self._run(ws.append_row, row)

    async def update_cell(self, name: str, r: int, c: int, v: Any):
        ws = await self._ws(name)
        return await self._run(ws.update_cell, r, c, v)

    async def delete_row(self, name: str, idx: int):
        ws = await self._ws(name)
        return await self._run(ws.delete_rows, idx)

    # -------- Domain helpers --------
    async def services(self) -> Dict[str, dict]:
        allv = (await self.get_all("Услуги"))[1:]
        out: Dict[str, dict] = {}
        for row in allv:
            if len(row) < 2:
                continue
            sid = row[0].strip()
            name = row[1].strip()
            price = int(row[2]) if len(row) > 2 and row[2].strip().isdigit() else 0
            dur = int(row[3]) if len(row) > 3 and row[3].strip().isdigit() else 60
            out[name] = {"id": sid, "price": price, "duration": dur}
        return out

    async def times(self) -> List[str]:
        rows = (await self.get_all("ВремяЗаписей"))[1:]
        result: List[str] = []
        for r in rows:
            if not r:
                continue
            d = r[0].strip() if len(r) > 0 else ""
            tm = r[1].strip() if len(r) > 1 else ""
            if d and tm:
                result.append(f"{d} {tm}")
        return result

    async def bookings(self) -> List[dict]:
        allv = await self.get_all("Записи")
        if not allv:
            return []
        hdr = allv[0]
        rows = allv[1:]
        return [dict(zip(hdr, r)) for r in rows]

    async def taken_slots(self) -> List[str]:
        rows = (await self.get_all("Записи"))[1:]
        out: List[str] = []
        for r in rows:
            if len(r) >= 6 and r[5].strip():
                out.append(r[5].strip())
        return out

    async def add_booking(
        self,
        id_client: str,
        name_client: str,
        date_str: str,
        time_str: str,
        service_name: str,
        price: int,
        duration: int,
    ) -> str:
        id_client_str = str(id_client).strip()
        allv = await self.get_all("Записи")
        rows = allv[1:] if allv and len(allv) > 1 else []
        next_id = 1
        try:
            ids = [int(r[0]) for r in rows if r and len(r) > 0 and str(r[0]).strip().isdigit()]
            next_id = max(ids) + 1 if ids else 1
        except Exception:
            next_id = len(rows) + 1
        dt_combined = f"{date_str} {time_str}"
        service_id = (await self.services()).get(service_name, {}).get("id", "")
        await self.append(
            "Записи",
            [
                next_id,
                id_client_str,
                name_client,
                date_str,
                time_str,
                dt_combined,
                service_id,
                price,
                duration,
                "Не подтверждена",
                "Нет",
                "",
            ],
        )
        return str(next_id)

    async def remove_booking_by_client(self, client_id: str) -> Tuple[Optional[str], Optional[List[str]]]:
        allv = await self.get_all("Записи")
        rows = allv[1:]
        for i, r in enumerate(rows, start=2):
            if len(r) >= 2 and str(r[1]).strip() == str(client_id):
                freed = r[5] if len(r) > 5 else None
                await self.delete_row("Записи", i)
                return freed, r
        return None, None

    async def add_catch(
        self, user_id: int, desired_time: str, service: str, id_client: str, chat_id: int
    ) -> bool:
        try:
            dt = parse_dt(desired_time)
        except Exception:
            return False
        if now() >= dt - timedelta(hours=UNAVAILABLE_BEFORE_HOURS):
            return False
        await self.append(
            "ПойматьОчередь",
            [user_id, desired_time, id_client or "", service or "", "Нет", fmt_dt(now()), chat_id],
        )
        return True

    async def get_catch_for_time(self, desired_time: str):
        rows = (await self.get_all("ПойматьОчередь"))[1:]
        out = []
        for i, r in enumerate(rows, start=2):
            if len(r) >= 5 and r[1].strip() == desired_time and r[4].strip().lower() in ("нет", ""):
                out.append((i, r))
        return out

    async def mark_catch_notified(self, idx: int):
        await self.update_cell("ПойматьОчередь", idx, 5, "Да")

    async def delete_catch(self, idx: int):
        await self.delete_row("ПойматьОчередь", idx)

    async def set_setting(self, key: str, value: str):
        allv = await self.get_all("Настройки")
        for i, r in enumerate(allv[1:], start=2):
            if r and r[0] == key:
                await self.update_cell("Настройки", i, 2, str(value))
                return
        await self.append("Настройки", [key, str(value)])

    async def get_setting(self, key: str) -> Optional[str]:
        allv = await self.get_all("Настройки")
        for r in allv[1:]:
            if r and r[0] == key:
                return r[1] if len(r) > 1 else None
        return None

    async def add_client(self, name: str, last: str, phone: str, tg: str, chat_id: int) -> str:
        allv = await self.get_all("Клиенты")
        rows = allv[1:] if allv and len(allv) > 1 else []
        next_id = 1
        try:
            ids = [int(r[0]) for r in rows if r and len(r) > 0 and str(r[0]).strip().isdigit()]
        
            next_id = max(ids) + 1 if ids else 1
        except Exception:
            next_id = len(rows) + 1
        await self.append("Клиенты", [next_id, name, last, phone, tg, chat_id])
        return str(next_id)