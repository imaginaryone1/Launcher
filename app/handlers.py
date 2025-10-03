import logging
from datetime import timedelta
from typing import Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from .config import (
    ADMIN_PASSWORD,
    CATCH_BUTTON,
    CATCH_MIN_HOURS,
    DISPLAY_MIN_HOURS,
    UNAVAILABLE_BEFORE_HOURS,
)
from .logic import filter_available_times, get_taken_times
from .services import cached_bookings, cached_services, cached_times, sheets
from .state_store import holds, registered
from .states import (
    CHOOSING_CATCH,
    CHOOSING_SERVICE,
    CHOOSING_TIME,
    MY_RECORDS,
    OFFER_ADD_CATCH,
    REG_LAST,
    REG_NAME,
    REG_PHONE,
)
from .ui import is_back, kbd_from, main_menu_kbd
from .utils import catch_errors, normalize_phone, now, parse_dt


log = logging.getLogger("LyuNailsBot")


def set_state(context: ContextTypes.DEFAULT_TYPE, new_state: Optional[str], push: bool = True):
    stack = context.user_data.setdefault("state_stack", [])
    cur = context.user_data.get("state")
    if push and cur is not None:
        stack.append(cur)
    context.user_data["state"] = new_state


def reset_state(context: ContextTypes.DEFAULT_TYPE):
    context.user_data["state"] = None
    context.user_data["state_stack"] = []


def pop_state(context: ContextTypes.DEFAULT_TYPE):
    stack = context.user_data.get("state_stack", [])
    if stack:
        prev = stack.pop()
        context.user_data["state"] = prev
        return prev
    context.user_data["state"] = None
    return None


async def send_state_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, state):
    if state is None:
        await update.message.reply_text("Возврат в меню:", reply_markup=main_menu_kbd())
        return
    if state == REG_NAME:
        await update.message.reply_text("👋 Привет! Как тебя зовут?")
        return
    if state == REG_LAST:
        await update.message.reply_text("Фамилия?")
        return
    if state == REG_PHONE:
        await update.message.reply_text("Телефон?")
        return
    if state == CHOOSING_SERVICE:
        sv = await cached_services()
        if not sv:
            await update.message.reply_text("Список услуг пуст")
            return
        await update.message.reply_text("Выберите услугу:", reply_markup=kbd_from(list(sv.keys())))
        return
    if state == CHOOSING_TIME:
        allt = await cached_times()
        taken = await get_taken_times()
        avail = await filter_available_times(allt, taken)
        kb = [CATCH_BUTTON] + avail if avail else [CATCH_BUTTON]
        await update.message.reply_text("Выберите время:", reply_markup=kbd_from(kb))
        return
    if state == CHOOSING_CATCH:
        slots = await get_taken_times()
        if not slots:
            await update.message.reply_text("Нет занятых слотов для поимки", reply_markup=kbd_from([], back=False))
            return
        await update.message.reply_text("Выберите время для поимки:", reply_markup=kbd_from(slots))
        return
    if state == OFFER_ADD_CATCH:
        await update.message.reply_text(
            "Добавить в очередь 'Поймать запись'?",
            reply_markup=kbd_from(["➕ Да, добавить", "🔁 Выбрать другое"], two=False),
        )
        return
    if state == MY_RECORDS:
        uid = update.effective_user.id
        client = registered.get(uid)
        if not client:
            await update.message.reply_text("Сначала /start", reply_markup=main_menu_kbd())
            return
        my_booking = None
        for b in await cached_bookings():
            id_key = next((k for k in b.keys() if "id" in k.lower() and "клиент" in k.lower()), None)
            if id_key and str(b.get(id_key, "")) == str(client["id_client"]):
                my_booking = b
                break
        if not my_booking:
            await update.message.reply_text("У вас нет записей", reply_markup=main_menu_kbd())
            return
        dt = None
        for k in my_booking.keys():
            if "дата" in k.lower() and "врем" in k.lower():
                dt = my_booking.get(k)
                break
        status_key = next((k for k in my_booking.keys() if "статус" in k.lower()), None)
        status = my_booking.get(status_key, "") if status_key else ""
        await update.message.reply_text(
            f"Ваша запись:\n{dt}\nСтатус: {status}",
            reply_markup=kbd_from(["❌ Отменить запись"], two=False, back=True),
        )
        return


@catch_errors
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    uid = user.id
    chat = update.effective_chat.id
    tg = f"@{user.username}" if user.username else ""
    rows = (await sheets.get_all("Клиенты"))[1:]
    found = None
    for i, r in enumerate(rows, start=2):
        if len(r) >= 5 and str(r[4]).strip() == tg:
            found = (i, r)
            break
        if len(r) >= 6 and str(r[5]).strip().isdigit() and int(str(r[5]).strip()) == chat:
            found = (i, r)
            break
    if not found:
        set_state(context, REG_NAME)
        await update.message.reply_text("👋 Привет! Как тебя зовут?")
        return
    _, r = found
    registered[uid] = {
        "id_client": str(r[0]),
        "name": r[1] if len(r) > 1 else "",
        "lastname": r[2] if len(r) > 2 else "",
        "phone": r[3] if len(r) > 3 else "",
        "telegram": r[4] if len(r) > 4 else tg,
        "chat_id": int(r[5]) if len(r) > 5 and str(r[5]).strip().isdigit() else chat,
    }
    reset_state(context)
    await update.message.reply_text("Выбирай действие:", reply_markup=main_menu_kbd())


@catch_errors
async def setadmin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args or []
    if not args:
        await update.message.reply_text("Использование: /setadmin <пароль>")
        return
    if args[0] != ADMIN_PASSWORD and args[0] != await sheets.get_setting("ADMIN_SET_PASSWORD"):
        await update.message.reply_text("Неверный пароль")
        return
    await sheets.set_setting("ADMIN_CHAT_ID", str(update.effective_chat.id))
    await update.message.reply_text("Этот чат отмечен админским")


@catch_errors
async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    uid = update.effective_user.id
    state = context.user_data.get("state")

    if is_back(text):
        if state in (REG_NAME, REG_LAST, REG_PHONE):
            context.user_data.clear()
            reset_state(context)
            await update.message.reply_text("Отменено. Главное меню:", reply_markup=main_menu_kbd())
            return
        prev = pop_state(context)
        await send_state_menu(update, context, prev)
        return

    if state == REG_NAME:
        context.user_data["reg_name"] = text
        set_state(context, REG_LAST)
        await update.message.reply_text("Фамилия?")
        return
    if state == REG_LAST:
        context.user_data["reg_last"] = text
        set_state(context, REG_PHONE)
        await update.message.reply_text("Телефон?")
        return
    if state == REG_PHONE:
        norm = normalize_phone(text)
        if not norm:
            await update.message.reply_text("Нужен формат телефона РФ: +7... или 8...")
            return
        context.user_data["reg_phone_normalized"] = norm
        context.user_data["reg_phone_raw"] = text
        kb = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("✅ Подтвердить номер", callback_data="phone_confirm::yes"),
                    InlineKeyboardButton("❌ Ввести снова", callback_data="phone_confirm::no"),
                ]
            ]
        )
        await update.message.reply_text(f"Вы ввели номер: {norm}. Подтвердите, пожалуйста.", reply_markup=kb)
        return

    if state is None:
        if text == "📝 Записаться":
            client = registered.get(uid)
            if not client:
                await update.message.reply_text("Сначала /start")
                return
            for b in await cached_bookings():
                id_key = next((k for k in b.keys() if "id" in k.lower() and "клиент" in k.lower()), None)
                if id_key and str(b.get(id_key, "")) == str(client["id_client"]):
                    dt_str = None
                    for k in b.keys():
                        if "дата" in k.lower() and "врем" in k.lower():
                            dt_str = b.get(k)
                            break
                    if dt_str:
                        try:
                            dt = parse_dt(dt_str)
                            if dt > now():
                                await update.message.reply_text(
                                    f"У вас есть запись на {dt_str}. Отмените сначала.",
                                    reply_markup=main_menu_kbd(),
                                )
                                return
                        except Exception:
                            pass
            sv = await cached_services()
            if not sv:
                await update.message.reply_text("Список услуг пуст")
                return
            set_state(context, CHOOSING_SERVICE)
            await update.message.reply_text("Выберите услугу:", reply_markup=kbd_from(list(sv.keys())))
            return

        if text == "📋 Мои записи":
            client = registered.get(uid)
            if not client:
                await update.message.reply_text("Сначала /start", reply_markup=main_menu_kbd())
                return
            my_booking = None
            for b in await cached_bookings():
                id_key = next((k for k in b.keys() if "id" in k.lower() and "клиент" in k.lower()), None)
                if id_key and str(b.get(id_key, "")) == str(client["id_client"]):
                    my_booking = b
                    break
            if not my_booking:
                await update.message.reply_text("У вас нет записей", reply_markup=main_menu_kbd())
                return
            dt = None
            for k in my_booking.keys():
                if "дата" in k.lower() and "врем" in k.lower():
                    dt = my_booking.get(k)
                    break
            status_key = next((k for k in my_booking.keys() if "статус" in k.lower()), None)
            status = my_booking.get(status_key, "") if status_key else ""
            set_state(context, MY_RECORDS)
            await update.message.reply_text(
                f"Ваша запись:\n{dt}\nСтатус: {status}",
                reply_markup=kbd_from(["❌ Отменить запись"], two=False, back=True),
            )
            return

        if text in ("🤖 О боте",):
            await update.message.reply_text("Бот для записи в салон.", reply_markup=main_menu_kbd())
            return
        if text in ("❓ Помощь",):
            await update.message.reply_text(
                "Если возникли вопросы или проблемы, то скорее пиши создателю @imaginaryone.",
                reply_markup=main_menu_kbd(),
            )
            return

    if state == CHOOSING_SERVICE:
        sv = await cached_services()
        if text not in sv:
            await update.message.reply_text("Выберите услугу:", reply_markup=kbd_from(list(sv.keys())))
            return
        context.user_data["chosen_service"] = text
        set_state(context, CHOOSING_TIME)
        allt = await cached_times()
        taken = await get_taken_times()
        avail = await filter_available_times(allt, taken)
        kb = [CATCH_BUTTON] + avail if avail else [CATCH_BUTTON]
        await update.message.reply_text("Выберите время:", reply_markup=kbd_from(kb))
        return

    if state == CHOOSING_TIME:
        if is_back(text):
            set_state(context, CHOOSING_SERVICE)
            sv = await cached_services()
            await update.message.reply_text("Выберите услугу:", reply_markup=kbd_from(list(sv.keys())))
            return
        if text == CATCH_BUTTON:
            client = registered.get(uid)
            exclude = client["id_client"] if client else None
            slots = await get_taken_times(exclude_client_id=exclude, min_hours=CATCH_MIN_HOURS)
            if not slots:
                await update.message.reply_text("Нет занятых слотов для поимки", reply_markup=kbd_from([], back=False))
                reset_state(context)
                return
            set_state(context, CHOOSING_CATCH)
            await update.message.reply_text("Выберите время для поимки:", reply_markup=kbd_from(slots))
            return
        taken_now = await get_taken_times()
        if text in taken_now:
            set_state(context, OFFER_ADD_CATCH)
            context.user_data["offer_time"] = text
            await update.message.reply_text(
                "Добавить в очередь 'Поймать запись'?",
                reply_markup=kbd_from(["➕ Да, добавить", "🔁 Выбрать другое"], two=False),
            )
            return
        allt = await cached_times()
        taken = await get_taken_times()
        valid = await filter_available_times(allt, taken)
        if text in valid:
            client = registered.get(uid)
            if not client:
                await update.message.reply_text("Сначала /start")
                return
            svc = context.user_data.get("chosen_service")
            info = (await cached_services()).get(svc, {})
            d, p = text.split(" ")
            await sheets.add_booking(
                client["id_client"],
                f"{client.get('name','')} {client.get('lastname','')}",
                d,
                p,
                svc,
                info.get("price", 0),
                info.get("duration", 60),
            )
            reset_state(context)
            await update.message.reply_text(
                f"✅ Забронировано {text}", reply_markup=main_menu_kbd()
            )
            admin = await sheets.get_setting("ADMIN_CHAT_ID")
            if admin:
                try:
                    await context.bot.send_message(
                        chat_id=int(admin),
                        text=(
                            f"🆕 Новая запись\n"
                            f"Услуга: {svc}\n"
                            f"Время: {text}\n"
                            f"Клиент: {client.get('name','')} {client.get('lastname','')}\n"
                            f"Телефон: {client.get('phone','')}\n"
                            f"Telegram: {client.get('telegram','')}"
                        ),
                    )
                except Exception:
                    pass
            return
        await update.message.reply_text(
            "Выберите корректное время",
            reply_markup=kbd_from([t for t in allt if t not in await get_taken_times()]),
        )
        return

    if state == MY_RECORDS:
        if text == "❌ Отменить запись":
            client = registered.get(uid)
            if not client:
                await update.message.reply_text("Сначала /start", reply_markup=kbd_from(["📝 Записаться"], two=True, back=False))
                return

            freed_dt, deleted_row = await sheets.remove_booking_by_client(client["id_client"])
            reset_state(context)
            await update.message.reply_text("Ваша запись отменена ✅", reply_markup=main_menu_kbd())

            if deleted_row:
                try:
                    admin_chat = await sheets.get_setting("ADMIN_CHAT_ID")
                    if admin_chat:
                        svc = deleted_row[6] if len(deleted_row) > 6 else ""
                        dt = deleted_row[5] if len(deleted_row) > 5 else ""
                        client_name = deleted_row[2] if len(deleted_row) > 2 else ""
                        client_phone = deleted_row[3] if len(deleted_row) > 3 else ""
                        client_telegram = deleted_row[4] if len(deleted_row) > 4 else ""
                        await context.bot.send_message(
                            chat_id=int(admin_chat),
                            text=(
                                f"❌ Запись отменена\n"
                                f"Услуга: {svc}\n"
                                f"Время: {dt}\n"
                                f"Клиент: {client_name}\n"
                                f"Телефон: {client_phone}\n"
                                f"Telegram: {client_telegram}"
                            ),
                        )
                except Exception as e:
                    log.warning("Не удалось отправить админу уведомление об отмене: %s", e)

            if freed_dt:
                try:
                    await notify_catchers_for_time(freed_dt, context)
                except Exception as e:
                    log.warning("notify_catchers_for_time ошибка: %s", e)

            return

    if state == OFFER_ADD_CATCH:
        if text == "➕ Да, добавить":
            desired = context.user_data.get("offer_time")
            client = registered.get(uid)
            cid = client["id_client"] if client else ""
            ok = await sheets.add_catch(uid, desired, context.user_data.get("chosen_service", ""), cid, update.effective_chat.id)
            reset_state(context)
            await update.message.reply_text("Добавлено" if ok else "Не удалось добавить", reply_markup=main_menu_kbd())
            return
        if text == "🔁 Выбрать другое":
            set_state(context, CHOOSING_TIME)
            allt = await cached_times()
            await update.message.reply_text(
                "Выберите время:",
                reply_markup=kbd_from([t for t in allt if t not in await get_taken_times()]),
            )
            return

    if state == CHOOSING_CATCH:
        if is_back(text):
            set_state(context, CHOOSING_TIME)
            await update.message.reply_text("Выберите время:", reply_markup=kbd_from(await get_taken_times()))
            return
        taken_slots = await get_taken_times()
        if text in taken_slots:
            set_state(context, OFFER_ADD_CATCH)
            context.user_data["offer_time"] = text
            await update.message.reply_text("Добавить в очередь?", reply_markup=kbd_from(["➕ Да, добавить", "↩️ Назад"], two=False))
            return

    await update.message.reply_text("Не понял, выберите из меню", reply_markup=main_menu_kbd())


@catch_errors
async def phone_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not q.data:
        return
    if q.data == "phone_confirm::yes":
        norm = context.user_data.get("reg_phone_normalized")
        name = context.user_data.get("reg_name")
        last = context.user_data.get("reg_last")
        tg = f"@{q.from_user.username}" if q.from_user.username else ""
        chat_id = q.message.chat.id
        if norm and name:
            cid = await sheets.add_client(name, last or "", norm, tg, chat_id)
            registered[q.from_user.id] = {
                "id_client": str(cid),
                "name": name,
                "lastname": last or "",
                "phone": norm,
                "telegram": tg,
                "chat_id": chat_id,
            }
            reset_state(context)
            try:
                await q.edit_message_text("✅ Номер подтверждён. Регистрация завершена.")
            except Exception:
                pass
            try:
                await context.bot.send_message(chat_id=q.from_user.id, text="Выбирай действие:", reply_markup=main_menu_kbd())
            except Exception:
                pass
            try:
                admin_chat = await sheets.get_setting("ADMIN_CHAT_ID")
                if admin_chat:
                    await context.bot.send_message(chat_id=int(admin_chat), text=f"🆕 Новый клиент: {name} {last or ''} | {norm} | {tg}")
            except Exception:
                pass
        else:
            await q.edit_message_text("Ошибка регистрации. Попробуйте /start снова.")
    else:
        set_state(context, REG_PHONE)
        await q.edit_message_text("Введите номер снова")


@catch_errors
async def catch_claim_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    parts = data.split("::")
    if len(parts) < 2:
        await q.edit_message_text("Некорректно")
        return
    act = parts[0]
    desired = parts[1]
    uid = q.from_user.id
    if act == "claim":
        hold = holds.get(uid)
        if not hold or hold.get("time") != desired:
            await q.edit_message_text("Истекло")
            return
        if desired in await get_taken_times():
            await q.edit_message_text("Успели")
            holds.pop(uid, None)
            return
        try:
            dt = parse_dt(desired)
        except Exception:
            await q.edit_message_text("Неверный формат")
            holds.pop(uid, None)
            return
        if dt <= now() + timedelta(hours=UNAVAILABLE_BEFORE_HOURS):
            await q.edit_message_text("Слишком поздно")
            holds.pop(uid, None)
            return
        client = registered.get(uid)
        if not client:
            await q.edit_message_text("Зарегистрируйтесь")
            return
        await sheets.remove_booking_by_client(client["id_client"])
        svc = hold.get("service", "")
        info = (await cached_services()).get(svc, {})
        d, t = desired.split(" ")
        await sheets.add_booking(
            client["id_client"],
            f"{client.get('name','')} {client.get('lastname','')}",
            d,
            t,
            svc,
            info.get("price", 0),
            info.get("duration", 60),
        )
        holds.pop(uid, None)
        rid = hold.get("row")
        if rid:
            try:
                await sheets.delete_catch(rid)
            except Exception:
                pass
        await q.edit_message_text("Успешно")
        await notify_catchers_for_time(desired, context)
        return
    if act == "decline":
        rid = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else None
        if rid:
            try:
                await sheets.delete_catch(rid)
            except Exception:
                pass
        holds.pop(uid, None)
        await q.edit_message_text("Отказано")
        await notify_catchers_for_time(desired, context)
        return


@catch_errors
async def confirm_booking_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if data.startswith("confirm_booking::"):
        bid = data.split("::", 1)[1]
        allv = await sheets.get_all("Записи")
        hdr = allv[0] if allv else []
        rows = allv[1:] if len(allv) > 1 else []
        for i, r in enumerate(rows, start=2):
            if len(r) >= 1 and str(r[0]).strip() == str(bid):
                status_idx = None
                for j, h in enumerate(hdr):
                    if "статус" in h.lower():
                        status_idx = j + 1
                        break
                if status_idx:
                    await sheets.update_cell("Записи", i, status_idx, "Подтверждена")
                await q.edit_message_text("Подтверждена")
                try:
                    admin = await sheets.get_setting("ADMIN_CHAT_ID")
                    if admin:
                        id_client = r[1] if len(r) > 1 else ""
                        client_rows = await sheets.get_all("Клиенты")
                        client_info = next((row for row in client_rows[1:] if row and str(row[0]) == str(id_client)), None)
                        if client_info:
                            client_name = f"{client_info[1]} {client_info[2]}" if len(client_info) > 2 else client_info[1]
                            client_phone = client_info[3] if len(client_info) > 3 else ""
                            client_tg = client_info[4] if len(client_info) > 4 else ""
                            msg = (
                                f"✅ Подтверждена запись\n"
                                f"Клиент: {client_name}\n"
                                f"Телефон: {client_phone}\n"
                                f"Telegram: {client_tg}\n"
                                f"Время: {r[5] if len(r) > 5 else ''}"
                            )
                        else:
                            msg = f"✅ Подтверждена запись ID {bid}"
                        await context.bot.send_message(chat_id=int(admin), text=msg)
                except Exception:
                    pass
                return
        await q.edit_message_text("Не найдена")


async def notify_catchers_for_time(freed_time: str, context: ContextTypes.DEFAULT_TYPE):
    matches = await sheets.get_catch_for_time(freed_time)
    if not matches:
        return
    idx, row = matches[0]
    try:
        uid = int(row[0])
    except Exception:
        try:
            await sheets.delete_catch(idx)
        except Exception:
            pass
        return await notify_catchers_for_time(freed_time, context)
    svc = row[3] if len(row) > 3 else ""
    chat = int(row[6]) if len(row) > 6 and str(row[6]).isdigit() else uid
    await sheets.mark_catch_notified(idx)
    try:
        dt = parse_dt(freed_time)
    except Exception:
        try:
            await sheets.delete_catch(idx)
        except Exception:
            pass
        return await notify_catchers_for_time(freed_time, context)
    claim_window = min(
        timedelta(minutes=30),
        dt - now() - timedelta(hours=UNAVAILABLE_BEFORE_HOURS),
    )
    if claim_window.total_seconds() <= 0:
        try:
            await sheets.delete_catch(idx)
        except Exception:
            pass
        return await notify_catchers_for_time(freed_time, context)
    holds[uid] = {"time": freed_time, "expires": now() + claim_window, "service": svc, "row": idx}
    kb = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Переместить запись", callback_data=f"claim::{freed_time}::{idx}"),
                InlineKeyboardButton("❌ Отказаться", callback_data=f"decline::{freed_time}::{idx}"),
            ]
        ]
    )
    try:
        await context.bot.send_message(chat_id=chat, text=f"Освободился слот {freed_time}. Переместить запись?", reply_markup=kb)
    except Exception:
        try:
            await sheets.delete_catch(idx)
        except Exception:
            pass
        holds.pop(uid, None)
        return await notify_catchers_for_time(freed_time, context)

