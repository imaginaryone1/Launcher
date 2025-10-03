import os
import re
import time
import asyncio
import logging
import traceback
from datetime import datetime, timedelta
from functools import wraps
from typing import Any, Dict, List, Optional, Tuple

import pytz
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters


# -------------------- Config --------------------
TZ = pytz.timezone(os.getenv("TZ", "Europe/Moscow"))
SPREADSHEET_KEY = os.getenv("SPREADSHEET_KEY", "1aGS6K4vgbV42eHk06PN9kXyo6iZbC7tnfAHrBcre5Bs")
CREDS_FILE = os.getenv("GSHEET_CREDS", "credentials.json")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "12345")
CLEANUP_INTERVAL = int(os.getenv("CLEANUP_INTERVAL", "60"))
GS_RETRY = int(os.getenv("GS_RETRY", "3"))
GS_BACKOFF = float(os.getenv("GS_BACKOFF", "0.5"))

# основные пороги (можно менять)
UNAVAILABLE_BEFORE_HOURS = int(os.getenv("UNAVAILABLE_BEFORE_HOURS", "24"))
DISPLAY_MIN_HOURS = int(os.getenv("DISPLAY_MIN_HOURS", "28"))
CATCH_WINDOW_MIN = int(os.getenv("CATCH_WINDOW_MIN", "30"))
CATCH_MIN_HOURS = int(os.getenv("CATCH_MIN_HOURS", "36"))

BACK_TEXT = "↩️ Назад"
CATCH_BUTTON = "⏱️ Поймать запись"


# -------------------- Logging --------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger("LyuNailsBot")


# -------------------- Utilities --------------------
def now() -> datetime:
    return datetime.now(TZ)


def parse_dt(s: str) -> datetime:
    return TZ.localize(datetime.strptime(s, "%d.%m.%Y %H:%M"))


def fmt_dt(dt: datetime) -> str:
    return dt.astimezone(TZ).strftime("%d.%m.%Y %H:%M")


def normalize_phone(raw: str) -> Optional[str]:
    if not raw:
        return None
    sanitized = re.sub(r"[^\d+]", "", raw.strip())
    digits = re.sub(r"\D", "", sanitized)
    if not digits:
        return None
    if sanitized.startswith("+") and len(digits) == 11 and digits.startswith("7"):
        return "+" + digits
    if len(digits) == 11 and digits.startswith("8"):
        return "+7" + digits[1:]
    if len(digits) == 11 and digits.startswith("7"):
        return "+" + digits
    if len(digits) == 10 and digits.startswith("9"):
        return "+7" + digits
    return None


def ttl_cache(ttl_seconds: int):
    def decorator(func):
        cache = {"value": None, "exp": 0.0}

        @wraps(func)
        async def wrapper(*args, **kwargs):
            if time.time() < cache["exp"] and cache["value"] is not None:
                return cache["value"]
            value = await func(*args, **kwargs)
            cache["value"] = value
            cache["exp"] = time.time() + ttl_seconds
            return value

        wrapper._cache = cache  # type: ignore[attr-defined]
        return wrapper

    return decorator


def safe_get(dct: Dict[str, Any], key_contains: str) -> Optional[Tuple[str, Any]]:
    key_lower = key_contains.lower()
    for k, v in dct.items():
        if key_lower in k.lower():
            return k, v
    return None


def build_menu(items: List[str], two_columns: bool = True, add_back: bool = True) -> ReplyKeyboardMarkup:
    rows: List[List[str]] = []
    if two_columns:
        for i in range(0, len(items), 2):
            row = [items[i]]
            if i + 1 < len(items):
                row.append(items[i + 1])
            rows.append(row)
    else:
        for it in items:
            rows.append([it])
    if add_back:
        rows.append([BACK_TEXT])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def is_back(text: str) -> bool:
    return bool(text and (text.strip() == BACK_TEXT or text.strip().lower() == "назад"))


def set_state(context: ContextTypes.DEFAULT_TYPE, new_state: Optional[str], push: bool = True) -> None:
    stack = context.user_data.setdefault("state_stack", [])
    current = context.user_data.get("state")
    if push and current is not None:
        stack.append(current)
    context.user_data["state"] = new_state


def reset_state(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data["state"] = None
    context.user_data["state_stack"] = []


def pop_state(context: ContextTypes.DEFAULT_TYPE) -> Optional[str]:
    stack = context.user_data.get("state_stack", [])
    if stack:
        prev = stack.pop()
        context.user_data["state"] = prev
        return prev
    context.user_data["state"] = None
    return None


# -------------------- Sheets wrapper --------------------
class Sheets:
    def __init__(self, creds_file: str, key: str):
        self.creds_file = creds_file
        self.key = key
        self.scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        self._gc = None
        self._sheet = None

    def _init_sync(self) -> None:
        if self._gc:
            return
        creds = ServiceAccountCredentials.from_json_keyfile_name(self.creds_file, self.scope)
        self._gc = gspread.authorize(creds)
        self._sheet = self._gc.open_by_key(self.key)
        for name in ("Клиенты", "Услуги", "ВремяЗаписей", "Записи", "ПойматьОчередь", "Настройки"):
            try:
                self._sheet.worksheet(name)
            except Exception:
                try:
                    self._sheet.add_worksheet(title=name, rows="1000", cols="20")
                    log.info("Created worksheet %s", name)
                except Exception:
                    pass

    async def _run(self, func, *args, **kwargs):
        loop = asyncio.get_running_loop()
        last_exc: Optional[BaseException] = None
        for attempt in range(1, GS_RETRY + 1):
            try:
                return await loop.run_in_executor(None, lambda: func(*args, **kwargs))
            except Exception as exc:  # retry with backoff
                log.warning("gspread attempt %s failed: %s", attempt, exc)
                await asyncio.sleep(GS_BACKOFF * (2 ** (attempt - 1)))
                last_exc = exc
        if last_exc:
            raise last_exc
        raise RuntimeError("Unknown gspread failure")

    async def _ws(self, name: str):
        await self._run(self._init_sync)
        return self._sheet.worksheet(name)

    async def get_all(self, name: str) -> List[List[str]]:
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

    # Domain helpers
    async def services(self) -> Dict[str, dict]:
        values = (await self.get_all("Услуги"))[1:]
        result: Dict[str, dict] = {}
        for row in values:
            if len(row) < 2:
                continue
            sid = row[0].strip()
            name = row[1].strip()
            price = int(row[2]) if len(row) > 2 and row[2].strip().isdigit() else 0
            duration = int(row[3]) if len(row) > 3 and row[3].strip().isdigit() else 60
            result[name] = {"id": sid, "price": price, "duration": duration}
        return result

    async def times(self) -> List[str]:
        rows = (await self.get_all("ВремяЗаписей"))[1:]
        slots: List[str] = []
        for r in rows:
            if not r:
                continue
            date = r[0].strip() if len(r) > 0 else ""
            tm = r[1].strip() if len(r) > 1 else ""
            if date and tm:
                slots.append(f"{date} {tm}")
        return slots

    async def bookings(self) -> List[dict]:
        all_values = await self.get_all("Записи")
        if not all_values:
            return []
        header = all_values[0]
        return [dict(zip(header, r)) for r in all_values[1:]]

    async def taken_slots(self) -> List[str]:
        rows = (await self.get_all("Записи"))[1:]
        result: List[str] = []
        for r in rows:
            if len(r) >= 6 and r[5].strip():
                result.append(r[5].strip())
        return result

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
        all_values = await self.get_all("Записи")
        rows = all_values[1:] if all_values and len(all_values) > 1 else []
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
                str(id_client).strip(),
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
        all_values = await self.get_all("Записи")
        rows = all_values[1:]
        for i, r in enumerate(rows, start=2):
            if len(r) >= 2 and str(r[1]).strip() == str(client_id):
                freed = r[5] if len(r) > 5 else None
                await self.delete_row("Записи", i)
                return freed, r
        return None, None

    async def add_catch(self, user_id: int, desired_time: str, service: str, id_client: str, chat_id: int) -> bool:
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

    async def mark_catch_notified(self, idx: int) -> None:
        await self.update_cell("ПойматьОчередь", idx, 5, "Да")

    async def delete_catch(self, idx: int) -> None:
        await self.delete_row("ПойматьОчередь", idx)

    async def set_setting(self, key: str, value: str) -> None:
        all_values = await self.get_all("Настройки")
        for i, r in enumerate(all_values[1:], start=2):
            if r and r[0] == key:
                await self.update_cell("Настройки", i, 2, str(value))
                return
        await self.append("Настройки", [key, str(value)])

    async def get_setting(self, key: str) -> Optional[str]:
        all_values = await self.get_all("Настройки")
        for r in all_values[1:]:
            if r and r[0] == key:
                return r[1] if len(r) > 1 else None
        return None

    async def add_client(self, name: str, last: str, phone: str, tg: str, chat_id: int) -> str:
        all_values = await self.get_all("Клиенты")
        rows = all_values[1:] if all_values and len(all_values) > 1 else []
        next_id = 1
        try:
            ids = [int(r[0]) for r in rows if r and len(r) > 0 and str(r[0]).strip().isdigit()]
            next_id = max(ids) + 1 if ids else 1
        except Exception:
            next_id = len(rows) + 1
        await self.append("Клиенты", [next_id, name, last, phone, tg, chat_id])
        return str(next_id)


# -------------------- Global state --------------------
sheets = Sheets(CREDS_FILE, SPREADSHEET_KEY)
holds: Dict[int, dict] = {}
registered: Dict[int, dict] = {}


@ttl_cache(30)
async def cached_services():
    return await sheets.services()


@ttl_cache(20)
async def cached_times():
    return await sheets.times()


@ttl_cache(8)
async def cached_taken():
    return await sheets.taken_slots()


@ttl_cache(8)
async def cached_bookings():
    return await sheets.bookings()


async def extract_taken_times(
    exclude_client_id: Optional[str] = None, min_hours: Optional[int] = None
) -> List[str]:
    bookings = await cached_bookings()
    result: List[str] = []
    for booking in bookings:
        dt_key_val = None
        for k, v in booking.items():
            if "дата" in k.lower() and "врем" in k.lower():
                dt_key_val = v
                break
        client_id: Optional[str] = None
        for k in booking.keys():
            if "id" in k.lower() and "клиент" in k.lower():
                client_id = str(booking.get(k))
                break
        if not dt_key_val:
            continue
        try:
            dt = parse_dt(str(dt_key_val))
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
        result.append(str(dt_key_val))
    result.sort(key=lambda t: parse_dt(t))
    return result


def build_main_menu() -> ReplyKeyboardMarkup:
    return build_menu(["📝 Записаться", "📋 Мои записи", "❓ Помощь", "🤖 О боте"], two_columns=True, add_back=False)


async def send_state_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, state: Optional[str]) -> None:
    if state is None:
        await update.message.reply_text("Возврат в меню:", reply_markup=build_main_menu())
        return

    if state == "reg_name":
        await update.message.reply_text("👋 Привет! Как тебя зовут?")
        return

    if state == "reg_last":
        await update.message.reply_text("Фамилия?")
        return

    if state == "reg_phone":
        await update.message.reply_text("Телефон?")
        return

    if state == "choosing_service":
        services = await cached_services()
        if not services:
            await update.message.reply_text("Список услуг пуст")
            return
        await update.message.reply_text("Выберите услугу:", reply_markup=build_menu(list(services.keys())))
        return

    if state == "choosing_time":
        services = await cached_services()  # may be useful later
        _ = services
        all_times = await cached_times()
        taken = await extract_taken_times()
        available: List[Tuple[datetime, str]] = []
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
        keyboard_items = [CATCH_BUTTON] + [t for _, t in available] if available else [CATCH_BUTTON]
        await update.message.reply_text("Выберите время:", reply_markup=build_menu(keyboard_items))
        return

    if state == "choosing_catch":
        slots = await extract_taken_times()
        if not slots:
            await update.message.reply_text("Нет занятых слотов для поимки", reply_markup=build_menu([], add_back=False))
            return
        await update.message.reply_text("Выберите время для поимки:", reply_markup=build_menu(slots))
        return

    if state == "offer_add_catch":
        await update.message.reply_text(
            "Добавить в очередь 'Поймать запись'?",
            reply_markup=build_menu(["➕ Да, добавить", "🔁 Выбрать другое"], two_columns=False),
        )
        return

    if state == "my_records":
        uid = update.effective_user.id
        client = registered.get(uid)
        if not client:
            await update.message.reply_text("Сначала /start", reply_markup=build_main_menu())
            return
        my_booking = None
        for b in await cached_bookings():
            id_key = next((k for k in b.keys() if "id" in k.lower() and "клиент" in k.lower()), None)
            if id_key and str(b.get(id_key, "")) == str(client["id_client"]):
                my_booking = b
                break
        if not my_booking:
            await update.message.reply_text("У вас нет записей", reply_markup=build_main_menu())
            return
        dt_val: Optional[str] = None
        for k in my_booking.keys():
            if "дата" in k.lower() and "врем" in k.lower():
                dt_val = my_booking.get(k)  # type: ignore[assignment]
                break
        status_key = next((k for k in my_booking.keys() if "статус" in k.lower()), None)
        status_val = my_booking.get(status_key, "") if status_key else ""
        await update.message.reply_text(
            f"Ваша запись:\n{dt_val}\nСтатус: {status_val}",
            reply_markup=build_menu(["❌ Отменить запись"], two_columns=False, add_back=True),
        )
        return


# -------------------- Decorator --------------------
def catch_errors(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            return await func(update, context)
        except Exception as e:
            tb = traceback.format_exc()
            log.exception("Handler error: %s", e)
            admin = await sheets.get_setting("ADMIN_CHAT_ID")
            try:
                if update and update.effective_message:
                    await update.effective_message.reply_text("Произошла ошибка. Администратор уведомлён.")
                if admin:
                    await context.bot.send_message(chat_id=int(admin), text=f"ERROR {e}\n\n{tb[:2000]}")
            except Exception:
                log.exception("Failed to report error")
    return wrapper


# -------------------- Core handlers --------------------
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
        set_state(context, "reg_name")
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
    await update.message.reply_text("Выбирай действие:", reply_markup=build_main_menu())


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


def find_my_booking_for_client(client_id: str, bookings: List[dict]) -> Optional[dict]:
    for b in bookings:
        id_key = next((k for k in b.keys() if "id" in k.lower() and "клиент" in k.lower()), None)
        if id_key and str(b.get(id_key, "")) == str(client_id):
            return b
    return None


def extract_dt_and_status(booking: dict) -> Tuple[Optional[str], str]:
    dt_val: Optional[str] = None
    for k in booking.keys():
        if "дата" in k.lower() and "врем" in k.lower():
            dt_val = booking.get(k)  # type: ignore[assignment]
            break
    status_key = next((k for k in booking.keys() if "статус" in k.lower()), None)
    status_val = booking.get(status_key, "") if status_key else ""
    return dt_val, str(status_val)


async def times_for_menu_excluding_taken(min_hours_show: int) -> List[str]:
    all_times = await cached_times()
    taken = await extract_taken_times()
    candidates: List[Tuple[datetime, str]] = []
    for t in all_times:
        try:
            dt = parse_dt(t)
        except Exception:
            continue
        if dt <= now():
            continue
        if dt <= now() + timedelta(hours=min_hours_show):
            continue
        if t in taken:
            continue
        candidates.append((dt, t))
    candidates.sort(key=lambda x: x[0])
    return [t for _, t in candidates]


@catch_errors
async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    uid = update.effective_user.id
    state = context.user_data.get("state")

    if is_back(text):
        if state in ("reg_name", "reg_last", "reg_phone"):
            context.user_data.clear()
            reset_state(context)
            await update.message.reply_text("Отменено. Главное меню:", reply_markup=build_main_menu())
            return
        prev = pop_state(context)
        await send_state_menu(update, context, prev)
        return

    # Registration
    if state == "reg_name":
        context.user_data["reg_name"] = text
        set_state(context, "reg_last")
        await update.message.reply_text("Фамилия?")
        return

    if state == "reg_last":
        context.user_data["reg_last"] = text
        set_state(context, "reg_phone")
        await update.message.reply_text("Телефон?")
        return

    if state == "reg_phone":
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

    # Main menu
    if state is None:
        if text == "📝 Записаться":
            client = registered.get(uid)
            if not client:
                await update.message.reply_text("Сначала /start")
                return
            my_existing = find_my_booking_for_client(client["id_client"], await cached_bookings())
            if my_existing:
                dt_val, _ = extract_dt_and_status(my_existing)
                if dt_val:
                    try:
                        dt_parsed = parse_dt(dt_val)
                        if dt_parsed > now():
                            await update.message.reply_text(
                                f"У вас есть запись на {dt_val}. Отмените сначала.",
                                reply_markup=build_main_menu(),
                            )
                            return
                    except Exception:
                        pass
            services = await cached_services()
            if not services:
                await update.message.reply_text("Список услуг пуст")
                return
            set_state(context, "choosing_service")
            await update.message.reply_text("Выберите услугу:", reply_markup=build_menu(list(services.keys())))
            return

        if text == "📋 Мои записи":
            client = registered.get(uid)
            if not client:
                await update.message.reply_text("Сначала /start", reply_markup=build_main_menu())
                return
            my_booking = find_my_booking_for_client(client["id_client"], await cached_bookings())
            if not my_booking:
                await update.message.reply_text("У вас нет записей", reply_markup=build_main_menu())
                return
            dt_val, status_val = extract_dt_and_status(my_booking)
            set_state(context, "my_records")
            await update.message.reply_text(
                f"Ваша запись:\n{dt_val}\nСтатус: {status_val}",
                reply_markup=build_menu(["❌ Отменить запись"], two_columns=False, add_back=True),
            )
            return

        if text == "🤖 О боте":
            await update.message.reply_text("Бот для записи в салон.", reply_markup=build_main_menu())
            return

        if text == "❓ Помощь":
            await update.message.reply_text(
                "Если возникли вопросы или проблемы, то скорее пиши создателю @imaginaryone.",
                reply_markup=build_main_menu(),
            )
            return

    # choosing_service
    if state == "choosing_service":
        services = await cached_services()
        if text not in services:
            await update.message.reply_text("Выберите услугу:", reply_markup=build_menu(list(services.keys())))
            return
        context.user_data["chosen_service"] = text
        set_state(context, "choosing_time")
        times = await times_for_menu_excluding_taken(DISPLAY_MIN_HOURS)
        keyboard_items = [CATCH_BUTTON] + times if times else [CATCH_BUTTON]
        await update.message.reply_text("Выберите время:", reply_markup=build_menu(keyboard_items))
        return

    # choosing_time
    if state == "choosing_time":
        if is_back(text):
            set_state(context, "choosing_service")
            services = await cached_services()
            await update.message.reply_text("Выберите услугу:", reply_markup=build_menu(list(services.keys())))
            return
        if text == CATCH_BUTTON:
            client = registered.get(uid)
            exclude = client["id_client"] if client else None
            slots = await extract_taken_times(exclude_client_id=exclude, min_hours=CATCH_MIN_HOURS)
            if not slots:
                await update.message.reply_text("Нет занятых слотов для поимки", reply_markup=build_menu([], add_back=False))
                reset_state(context)
                return
            set_state(context, "choosing_catch")
            await update.message.reply_text("Выберите время для поимки:", reply_markup=build_menu(slots))
            return
        taken_now = await extract_taken_times()
        if text in taken_now:
            set_state(context, "offer_add_catch")
            context.user_data["offer_time"] = text
            await update.message.reply_text(
                "Добавить в очередь 'Поймать запись'?",
                reply_markup=build_menu(["➕ Да, добавить", "🔁 Выбрать другое"], two_columns=False),
            )
            return
        valid_times = await times_for_menu_excluding_taken(DISPLAY_MIN_HOURS)
        if text in valid_times:
            client = registered.get(uid)
            if not client:
                await update.message.reply_text("Сначала /start")
                return
            svc_name = context.user_data.get("chosen_service")
            info = (await cached_services()).get(svc_name, {})
            d, p = text.split(" ")
            await sheets.add_booking(
                client["id_client"],
                f"{client.get('name','')} {client.get('lastname','')}",
                d,
                p,
                svc_name or "",
                int(info.get("price", 0)),
                int(info.get("duration", 60)),
            )
            reset_state(context)
            await update.message.reply_text(
                f"✅ Забронировано {text}",
                reply_markup=build_main_menu(),
            )
            admin = await sheets.get_setting("ADMIN_CHAT_ID")
            if admin:
                try:
                    await context.bot.send_message(
                        chat_id=int(admin),
                        text=(
                            f"🆕 Новая запись\n"
                            f"Услуга: {svc_name}\n"
                            f"Время: {text}\n"
                            f"Клиент: {client.get('name','')} {client.get('lastname','')}\n"
                            f"Телефон: {client.get('phone','')}\n"
                            f"Telegram: {client.get('telegram','')}"
                        ),
                    )
                except Exception:
                    pass
            return
        # fallback prompt
        all_times = await cached_times()
        available = [t for t in all_times if t not in await extract_taken_times()]
        await update.message.reply_text("Выберите корректное время", reply_markup=build_menu(available))
        return

    if state == "my_records":
        if text == "❌ Отменить запись":
            client = registered.get(uid)
            if not client:
                await update.message.reply_text("Сначала /start", reply_markup=build_menu(["📝 Записаться"], add_back=False))
                return
            freed_dt, deleted_row = await sheets.remove_booking_by_client(client["id_client"])
            reset_state(context)
            await update.message.reply_text(
                "Ваша запись отменена ✅",
                reply_markup=build_main_menu(),
            )
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

    if state == "offer_add_catch":
        if text == "➕ Да, добавить":
            desired = context.user_data.get("offer_time")
            client = registered.get(uid)
            cid = client["id_client"] if client else ""
            ok = await sheets.add_catch(uid, str(desired), context.user_data.get("chosen_service", ""), cid, update.effective_chat.id)
            reset_state(context)
            await update.message.reply_text("Добавлено" if ok else "Не удалось добавить", reply_markup=build_main_menu())
            return
        if text == "🔁 Выбрать другое":
            set_state(context, "choosing_time")
            times = await times_for_menu_excluding_taken(DISPLAY_MIN_HOURS)
            await update.message.reply_text("Выберите время:", reply_markup=build_menu(times))
            return

    if state == "choosing_catch":
        if is_back(text):
            set_state(context, "choosing_time")
            await update.message.reply_text("Выберите время:", reply_markup=build_menu(await extract_taken_times()))
            return
        taken_slots = await extract_taken_times()
        if text in taken_slots:
            set_state(context, "offer_add_catch")
            context.user_data["offer_time"] = text
            await update.message.reply_text("Добавить в очередь?", reply_markup=build_menu(["➕ Да, добавить", BACK_TEXT], two_columns=False))
            return

    await update.message.reply_text("Не понял, выберите из меню", reply_markup=build_main_menu())


# -------------------- Callbacks --------------------
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
                await context.bot.send_message(chat_id=q.from_user.id, text="Выбирай действие:", reply_markup=build_main_menu())
            except Exception:
                pass
            try:
                admin_chat = await sheets.get_setting("ADMIN_CHAT_ID")
                if admin_chat:
                    await context.bot.send_message(
                        chat_id=int(admin_chat),
                        text=f"🆕 Новый клиент: {name} {last or ''} | {norm} | {tg}",
                    )
            except Exception:
                pass
        else:
            await q.edit_message_text("Ошибка регистрации. Попробуйте /start снова.")
    else:
        set_state(context, "reg_phone")
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
    action = parts[0]
    desired = parts[1]
    uid = q.from_user.id
    if action == "claim":
        hold = holds.get(uid)
        if not hold or hold.get("time") != desired:
            await q.edit_message_text("Истекло")
            return
        if desired in await extract_taken_times():
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
            int(info.get("price", 0)),
            int(info.get("duration", 60)),
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
    if action == "decline":
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
        all_values = await sheets.get_all("Записи")
        header = all_values[0] if all_values else []
        rows = all_values[1:] if len(all_values) > 1 else []
        for i, r in enumerate(rows, start=2):
            if len(r) >= 1 and str(r[0]).strip() == str(bid):
                status_idx = None
                for j, h in enumerate(header):
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


# -------------------- Notify catchers --------------------
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
        timedelta(minutes=CATCH_WINDOW_MIN),
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


# -------------------- Background maintenance --------------------
async def mark_past_bookings(app):
    try:
        all_values = await sheets.get_all("Записи")
        header = all_values[0] if all_values else []
        rows = all_values[1:] if len(all_values) > 1 else []
        st_idx = None
        for j, h in enumerate(header):
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
                    current = r[st_idx - 1].strip().lower() if len(r) >= st_idx else ""
                    if current not in ("отменена", "прошедшая"):
                        await sheets.update_cell("Записи", i, st_idx, "Прошедшая")
            except Exception:
                continue
    except Exception:
        log.exception("mark_past_bookings failed")


async def background_tasks(app):
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

            all_values = await sheets.get_all("Записи")
            header = all_values[0] if all_values else []
            rows = all_values[1:] if len(all_values) > 1 else []
            st_idx = None
            notif_idx = None
            for j, h in enumerate(header):
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
                    already = (
                        r[notif_idx - 1].strip().lower() == "да" if notif_idx and len(r) >= notif_idx else False
                    )
                    if (
                        st_idx
                        and dt
                        and not already
                        and now() >= dt - timedelta(hours=UNAVAILABLE_BEFORE_HOURS)
                        and now() < dt - timedelta(hours=UNAVAILABLE_BEFORE_HOURS) + timedelta(seconds=CLEANUP_INTERVAL)
                    ):
                        id_client = r[1] if len(r) > 1 else ""
                        clients = (await sheets.get_all("Клиенты"))[1:]
                        tg_chat: Optional[int] = None
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
                                kb = InlineKeyboardMarkup(
                                    [[InlineKeyboardButton("✅ Подтвердить", callback_data=f"confirm_booking::{r[0]}")]]
                                )
                                await app.bot.send_message(
                                    chat_id=tg_chat,
                                    text=f"🔔 Напоминание: запись на {dt_str}. Подтвердите.",
                                    reply_markup=kb,
                                )
                                if notif_idx:
                                    await sheets.update_cell("Записи", i, notif_idx, "Да")
                                admin_chat = await sheets.get_setting("ADMIN_CHAT_ID")
                                if admin_chat:
                                    try:
                                        await app.bot.send_message(
                                            chat_id=int(admin_chat),
                                            text=f"Напоминание отправлено клиенту {id_client} для записи {dt_str}",
                                        )
                                    except Exception:
                                        pass
                            except Exception:
                                pass
                    if st_idx and dt and now() >= dt - timedelta(hours=UNAVAILABLE_BEFORE_HOURS) and (
                        len(r) >= st_idx and r[st_idx - 1].strip().lower() == "не подтверждена"
                    ):
                        await sheets.update_cell("Записи", i, st_idx, "Отменена")
                        await notify_catchers_for_time(dt_str, app)
                except Exception:
                    continue
            await mark_past_bookings(app)
        except Exception as e:
            log.exception("Background error: %s", e)
        await asyncio.sleep(CLEANUP_INTERVAL)


# -------------------- App bootstrap --------------------
def build_app():
    if not TELEGRAM_TOKEN:
        log.critical("Set TELEGRAM_TOKEN env var")
        raise SystemExit(1)
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("setadmin", setadmin))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))
    app.add_handler(CallbackQueryHandler(phone_cb, pattern="^phone_confirm::"))
    app.add_handler(CallbackQueryHandler(catch_claim_cb, pattern="^(claim::|decline::)"))
    app.add_handler(CallbackQueryHandler(confirm_booking_cb, pattern="^confirm_booking::"))

    async def post_init(application):
        await sheets._run(sheets._init_sync)
        asyncio.create_task(background_tasks(application))

    app.post_init = post_init
    return app


def main():
    app = build_app()
    log.info("Bot starting✅✅✅")
    app.run_polling()


if __name__ == "__main__":
    main()

