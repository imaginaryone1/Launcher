import logging
from datetime import timedelta

from telegram.ext import Application

from .config import CLEANUP_INTERVAL, UNAVAILABLE_BEFORE_HOURS
from .handlers import notify_catchers_for_time
from .services import sheets
from .state_store import holds, registered
from .utils import now, parse_dt


log = logging.getLogger("LyuNailsBot")


async def mark_past_bookings(app: Application):
    try:
        allv = await sheets.get_all("Записи")
        hdr = allv[0] if allv else []
        rows = allv[1:] if len(allv) > 1 else []
        st_idx = None
        for j, h in enumerate(hdr):
            if "статус" in h.lower():
                st_idx = j + 1
                break
        for i, r in enumerate(rows, start=2):
            try:
                dt_str = r[5] if len(r) > 5 else ""
                if not dt_str:
                    continue
                dt = parse_dt(dt_str)
                if dt < now() and st_idx:
                    cur = r[st_idx - 1].strip().lower() if len(r) >= st_idx else ""
                    if cur not in ("отменена", "прошедшая"):
                        await sheets.update_cell("Записи", i, st_idx, "Прошедшая")
            except Exception:
                continue
    except Exception:
        log.exception("mark_past_bookings failed")


async def background_tasks(app: Application):
    log.info("Background started")
    while True:
        try:
            await sheets.get_all("ПойматьОчередь")
            await sheets.get_all("Записи")

            expired = [uid for uid, h in list(holds.items()) if now() >= h.get("expires", now())]
            for uid in expired:
                try:
                    await app.bot.send_message(chat_id=uid, text="⏰ Время подтверждения истекло")
                except Exception:
                    pass
                holds.pop(uid, None)

            allv = await sheets.get_all("Записи")
            hdr = allv[0] if allv else []
            rows = allv[1:] if len(allv) > 1 else []
            st_idx = None
            notif_idx = None
            for j, h in enumerate(hdr):
                if "статус" in h.lower():
                    st_idx = j + 1
                if "бот уведом" in h.lower():
                    notif_idx = j + 1
            for i, r in enumerate(rows, start=2):
                try:
                    dt_str = r[5] if len(r) > 5 else ""
                    if not dt_str:
                        continue
                    dt = parse_dt(dt_str)
                    already = r[notif_idx - 1].strip().lower() == "да" if notif_idx and len(r) >= notif_idx else False
                    if (
                        st_idx
                        and dt
                        and not already
                        and now() >= dt - timedelta(hours=UNAVAILABLE_BEFORE_HOURS)
                        and now() < dt - timedelta(hours=UNAVAILABLE_BEFORE_HOURS) + timedelta(seconds=CLEANUP_INTERVAL)
                    ):
                        id_client = r[1] if len(r) > 1 else ""
                        clients = (await sheets.get_all("Клиенты"))[1:]
                        tg_chat = None
                        for cr in clients:
                            if cr and len(cr) > 0 and str(cr[0]) == str(id_client):
                                if len(cr) > 5 and str(cr[5]).strip().isdigit():
                                    tg_chat = int(cr[5])
                                    break
                        if not tg_chat:
                            for k, v in registered.items():
                                if str(v.get("id_client")) == str(id_client):
                                    tg_chat = k
                                    break
                        if tg_chat:
                            try:
                                from telegram import InlineKeyboardButton, InlineKeyboardMarkup

                                kb = InlineKeyboardMarkup([[InlineKeyboardButton("✅ Подтвердить", callback_data=f"confirm_booking::{r[0]}")]])
                                await app.bot.send_message(chat_id=tg_chat, text=f"🔔 Напоминание: запись на {dt_str}. Подтвердите.", reply_markup=kb)
                                if notif_idx:
                                    await sheets.update_cell("Записи", i, notif_idx, "Да")
                                admin_chat = await sheets.get_setting("ADMIN_CHAT_ID")
                                if admin_chat:
                                    try:
                                        await app.bot.send_message(chat_id=int(admin_chat), text=f"Напоминание отправлено клиенту {id_client} для записи {dt_str}")
                                    except Exception:
                                        pass
                            except Exception:
                                pass
                    if st_idx and dt and now() >= dt - timedelta(hours=UNAVAILABLE_BEFORE_HOURS) and (len(r) >= st_idx and r[st_idx - 1].strip().lower() == "не подтверждена"):
                        await sheets.update_cell("Записи", i, st_idx, "Отменена")
                        await notify_catchers_for_time(dt_str, app)
                except Exception:
                    continue
            await mark_past_bookings(app)
        except Exception as e:
            log.exception("Background error: %s", e)
        from asyncio import sleep

        await sleep(CLEANUP_INTERVAL)

