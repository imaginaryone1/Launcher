import os, re, time, asyncio, logging, traceback
from datetime import datetime, timedelta
from functools import wraps
from typing import Any, Dict, List, Optional, Tuple

import pytz, gspread
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters


# -------------------- Logging --------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger("LyuNailsBot")


# -------------------- Config --------------------
class Config:
    TZ = pytz.timezone(os.getenv("TZ", "Europe/Moscow"))
    SPREADSHEET_KEY = os.getenv("SPREADSHEET_KEY", "1aGS6K4vgbV42eHk06PN9kXyo6iZbC7tnfAHrBcre5Bs")
    CREDS_FILE = os.getenv("GSHEET_CREDS", "credentials.json")
    TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
    ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "12345")
    CLEANUP_INTERVAL = int(os.getenv("CLEANUP_INTERVAL", "60"))
    GS_RETRY = int(os.getenv("GS_RETRY", "3"))
    GS_BACKOFF = float(os.getenv("GS_BACKOFF", "0.5"))

    UNAVAILABLE_BEFORE_HOURS = int(os.getenv("UNAVAILABLE_BEFORE_HOURS", "24"))
    DISPLAY_MIN_HOURS = int(os.getenv("DISPLAY_MIN_HOURS", "28"))
    CATCH_WINDOW_MIN = int(os.getenv("CATCH_WINDOW_MIN", "30"))
    CATCH_MIN_HOURS = int(os.getenv("CATCH_MIN_HOURS", "36"))

    BACK_TEXT = "↩️ Назад"
    CATCH_BUTTON = "⏱️ Поймать запись"


# -------------------- Utilities --------------------
def now() -> datetime:
    return datetime.now(Config.TZ)


def parse_dt(s: str) -> datetime:
    return Config.TZ.localize(datetime.strptime(s, "%d.%m.%Y %H:%M"))


def fmt_dt(dt: datetime) -> str:
    return dt.astimezone(Config.TZ).strftime("%d.%m.%Y %H:%M")


def normalize_phone(raw: str):
    if not raw:
        return None
    s = re.sub(r"[^\d+]", "", raw.strip())
    digits = re.sub(r"\D", "", s)
    if not digits:
        return None
    if s.startswith("+") and len(digits) == 11 and digits.startswith("7"):
        return "+" + digits
    if len(digits) == 11 and digits.startswith("8"):
        return "+7" + digits[1:]
    if len(digits) == 11 and digits.startswith("7"):
        return "+" + digits
    if len(digits) == 10 and digits.startswith("9"):
        return "+7" + digits
    return None


def ttl_cache(ttl: int):
    def deco(func):
        cache = {"value": None, "exp": 0}

        @wraps(func)
        async def wrapper(*args, **kwargs):
            if time.time() < cache["exp"] and cache["value"] is not None:
                return cache["value"]
            val = await func(*args, **kwargs)
            cache["value"] = val
            cache["exp"] = time.time() + ttl
            return val

        wrapper._cache = cache
        return wrapper

    return deco


def build_menu_kbd(items: List[str], two: bool = True, back: bool = True) -> ReplyKeyboardMarkup:
    kb: List[List[str]] = []
    if two:
        for i in range(0, len(items), 2):
            row = [items[i]]
            if i + 1 < len(items):
                row.append(items[i + 1])
            kb.append(row)
    else:
        for x in items:
            kb.append([x])
    if back:
        kb.append([Config.BACK_TEXT])
    return ReplyKeyboardMarkup(kb, resize_keyboard=True)


def is_back(text: str) -> bool:
    return bool(text and (text.strip() == Config.BACK_TEXT or text.strip().lower() == "назад"))


class State:
    REG_NAME = "reg_name"
    REG_LAST = "reg_last"
    REG_PHONE = "reg_phone"
    CHOOSING_SERVICE = "choosing_service"
    CHOOSING_TIME = "choosing_time"
    CHOOSING_CATCH = "choosing_catch"
    OFFER_ADD_CATCH = "offer_add_catch"
    MY_RECORDS = "my_records"


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


# -------------------- Sheets wrapper --------------------
class Sheets:
    def __init__(self, creds_file: str, key: str):
        self.creds_file = creds_file
        self.key = key
        self.scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive',
        ]
        self._gc = None
        self._sheet = None

    def _init_sync(self):
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

    async def _run(self, func, *a, **k):
        loop = asyncio.get_running_loop()
        last = None
        for attempt in range(1, Config.GS_RETRY + 1):
            try:
                return await loop.run_in_executor(None, lambda: func(*a, **k))
            except Exception as e:
                log.warning("gspread attempt %s failed: %s", attempt, e)
                await asyncio.sleep(Config.GS_BACKOFF * (2 ** (attempt - 1)))
                last = e
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
        t: List[str] = []
        for r in rows:
            if not r:
                continue
            d = r[0].strip() if len(r) > 0 else ""
            tm = r[1].strip() if len(r) > 1 else ""
            if d and tm:
                t.append(f"{d} {tm}")
        return t

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
        id_client: Any,
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

    async def remove_booking_by_client(self, client_id: str) -> Tuple[Optional[str], Optional[list]]:
        allv = await self.get_all("Записи")
        rows = allv[1:]
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
        if now() >= dt - timedelta(hours=Config.UNAVAILABLE_BEFORE_HOURS):
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


# -------------------- Global state --------------------
sheets = Sheets(Config.CREDS_FILE, Config.SPREADSHEET_KEY)
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


# -------------------- Domain helpers --------------------
async def get_taken_times(
    exclude_client_id: Optional[str] = None, min_hours: Optional[int] = None
) -> List[str]:
    bookings = await cached_bookings()
    out: List[str] = []
    for b in bookings:
        dt_str = None
        client_id = None
        for k, v in b.items():
            if 'дата' in k.lower() and 'врем' in k.lower():
                dt_str = v
                break
        for k in b.keys():
            if 'id' in k.lower() and 'клиент' in k.lower():
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
        if exclude_client_id is not None and client_id is not None and str(client_id).strip() == str(exclude_client_id).strip():
            continue
        out.append(dt_str)
    out.sort(key=lambda t: parse_dt(t))
    return out


async def build_available_time_strings() -> List[str]:
    allt = await cached_times()
    taken = set(await get_taken_times())
    avail: List[str] = []
    for t in allt:
        try:
            dt = parse_dt(t)
        except Exception:
            continue
        if dt <= now():
            continue
        if dt <= now() + timedelta(hours=Config.DISPLAY_MIN_HOURS):
            continue
        if t in taken:
            continue
        avail.append(t)
    avail.sort(key=lambda ts: parse_dt(ts))
    return avail


def main_menu_kbd() -> ReplyKeyboardMarkup:
    return build_menu_kbd(["📝 Записаться", "📋 Мои записи", "❓ Помощь", "🤖 О боте"], two=True, back=False)


async def find_client_for_user(update: Update) -> Optional[dict]:
    uid = update.effective_user.id
    if uid in registered:
        return registered[uid]
    tg = f"@{update.effective_user.username}" if update.effective_user.username else ""
    chat = update.effective_chat.id
    rows = (await sheets.get_all("Клиенты"))[1:]
    for r in rows:
        if len(r) >= 5 and str(r[4]).strip() == tg:
            client = {
                "id_client": str(r[0]),
                "name": r[1] if len(r) > 1 else "",
                "lastname": r[2] if len(r) > 2 else "",
                "phone": r[3] if len(r) > 3 else "",
                "telegram": r[4] if len(r) > 4 else tg,
                "chat_id": int(r[5]) if len(r) > 5 and str(r[5]).strip().isdigit() else chat,
            }
            registered[uid] = client
            return client
        if len(r) >= 6 and str(r[5]).strip().isdigit() and int(str(r[5]).strip()) == chat:
            client = {
                "id_client": str(r[0]),
                "name": r[1] if len(r) > 1 else "",
                "lastname": r[2] if len(r) > 2 else "",
                "phone": r[3] if len(r) > 3 else "",
                "telegram": r[4] if len(r) > 4 else tg,
                "chat_id": int(r[5]) if len(r) > 5 and str(r[5]).strip().isdigit() else chat,
            }
            registered[uid] = client
            return client
    return None


async def get_my_booking(client_id: str) -> Optional[dict]:
    for b in await cached_bookings():
        id_key = next((k for k in b.keys() if "id" in k.lower() and "клиент" in k.lower()), None)
        if id_key and str(b.get(id_key, "")) == str(client_id):
            return b
    return None


def extract_dt_and_status(booking: dict) -> Tuple[Optional[str], Optional[str]]:
    dt = None
    status = None
    for k in booking.keys():
        if 'дата' in k.lower() and 'врем' in k.lower():
            dt = booking.get(k)
            break
    status_key = next((k for k in booking.keys() if 'статус' in k.lower()), None)
    if status_key:
        status = booking.get(status_key, '')
    return dt, status


async def send_state_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, state):
    if state is None:
        await update.message.reply_text("Возврат в меню:", reply_markup=main_menu_kbd())
        return
    if state == State.REG_NAME:
        await update.message.reply_text("👋 Привет! Как тебя зовут?")
        return
    if state == State.REG_LAST:
        await update.message.reply_text("Фамилия?")
        return
    if state == State.REG_PHONE:
        await update.message.reply_text("Телефон?")
        return
    if state == State.CHOOSING_SERVICE:
        sv = await cached_services()
        if not sv:
            await update.message.reply_text("Список услуг пуст")
            return
        await update.message.reply_text("Выберите услугу:", reply_markup=build_menu_kbd(list(sv.keys())))
        return
    if state == State.CHOOSING_TIME:
        avail = await build_available_time_strings()
        kb = [Config.CATCH_BUTTON] + avail if avail else [Config.CATCH_BUTTON]
        await update.message.reply_text("Выберите время:", reply_markup=build_menu_kbd(kb))
        return
    if state == State.CHOOSING_CATCH:
        slots = await get_taken_times()
        if not slots:
            await update.message.reply_text("Нет занятых слотов для поимки", reply_markup=build_menu_kbd([], back=False))
            return
        await update.message.reply_text("Выберите время для поимки:", reply_markup=build_menu_kbd(slots))
        return
    if state == State.OFFER_ADD_CATCH:
        await update.message.reply_text(
            "Добавить в очередь 'Поймать запись'?",
            reply_markup=build_menu_kbd(["➕ Да, добавить", "🔁 Выбрать другое"], two=False),
        )
        return
    if state == State.MY_RECORDS:
        uid = update.effective_user.id
        client = registered.get(uid)
        if not client:
            await update.message.reply_text("Сначала /start", reply_markup=main_menu_kbd())
            return
        my_booking = await get_my_booking(client['id_client'])
        if not my_booking:
            await update.message.reply_text("У вас нет записей", reply_markup=main_menu_kbd())
            return
        dt, status = extract_dt_and_status(my_booking)
        await update.message.reply_text(
            f"Ваша запись:\n{dt}\nСтатус: {status}",
            reply_markup=build_menu_kbd(["❌ Отменить запись"], two=False, back=True),
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
    tg = f"@{user.username}" if user.username else ""
    chat = update.effective_chat.id

    found = await find_client_for_user(update)
    if not found:
        set_state(context, State.REG_NAME)
        await update.message.reply_text("👋 Привет! Как тебя зовут?")
        return

    registered[uid] = {
        "id_client": str(found["id_client"]),
        "name": found.get("name", ""),
        "lastname": found.get("lastname", ""),
        "phone": found.get("phone", ""),
        "telegram": found.get("telegram", tg),
        "chat_id": found.get("chat_id", chat),
    }
    reset_state(context)
    await update.message.reply_text("Выбирай действие:", reply_markup=main_menu_kbd())


@catch_errors
async def setadmin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args or []
    if not args:
        await update.message.reply_text("Использование: /setadmin <пароль>")
        return
    provided = args[0]
    admin_set_password = await sheets.get_setting("ADMIN_SET_PASSWORD")
    if provided != Config.ADMIN_PASSWORD and provided != admin_set_password:
        await update.message.reply_text("Неверный пароль")
        return
    await sheets.set_setting("ADMIN_CHAT_ID", str(update.effective_chat.id))
    await update.message.reply_text("Этот чат отмечен админским")


async def ensure_no_future_booking(client_id: str) -> Optional[str]:
    booking = await get_my_booking(client_id)
    if not booking:
        return None
    dt_str, _ = extract_dt_and_status(booking)
    if not dt_str:
        return None
    try:
        dt = parse_dt(dt_str)
        if dt > now():
            return dt_str
    except Exception:
        return None
    return None


@catch_errors
async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    uid = update.effective_user.id
    state = context.user_data.get("state")

    if is_back(text):
        if state in (State.REG_NAME, State.REG_LAST, State.REG_PHONE):
            context.user_data.clear()
            reset_state(context)
            await update.message.reply_text("Отменено. Главное меню:", reply_markup=main_menu_kbd())
            return
        prev = pop_state(context)
        await send_state_menu(update, context, prev)
        return

    # Registration
    if state == State.REG_NAME:
        context.user_data['reg_name'] = text
        set_state(context, State.REG_LAST)
        await update.message.reply_text("Фамилия?")
        return
    if state == State.REG_LAST:
        context.user_data['reg_last'] = text
        set_state(context, State.REG_PHONE)
        await update.message.reply_text("Телефон?")
        return
    if state == State.REG_PHONE:
        norm = normalize_phone(text)
        if not norm:
            await update.message.reply_text("Нужен формат телефона РФ: +7... или 8...")
            return
        context.user_data['reg_phone_normalized'] = norm
        context.user_data['reg_phone_raw'] = text
        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Подтвердить номер", callback_data="phone_confirm::yes"),
                InlineKeyboardButton("❌ Ввести снова", callback_data="phone_confirm::no"),
            ]
        ])
        await update.message.reply_text(f"Вы ввели номер: {norm}. Подтвердите, пожалуйста.", reply_markup=kb)
        return

    # Main menu
    if state is None:
        if text == "📝 Записаться":
            client = await find_client_for_user(update)
            if not client:
                await update.message.reply_text("Сначала /start")
                return
            future_dt = await ensure_no_future_booking(client['id_client'])
            if future_dt:
                await update.message.reply_text(
                    f"У вас есть запись на {future_dt}. Отмените сначала.", reply_markup=main_menu_kbd()
                )
                return
            sv = await cached_services()
            if not sv:
                await update.message.reply_text("Список услуг пуст")
                return
            set_state(context, State.CHOOSING_SERVICE)
            await update.message.reply_text("Выберите услугу:", reply_markup=build_menu_kbd(list(sv.keys())))
            return

        if text == "📋 Мои записи":
            client = await find_client_for_user(update)
            if not client:
                await update.message.reply_text("Сначала /start", reply_markup=main_menu_kbd())
                return
            my_booking = await get_my_booking(client['id_client'])
            if not my_booking:
                await update.message.reply_text("У вас нет записей", reply_markup=main_menu_kbd())
                return
            dt, status = extract_dt_and_status(my_booking)
            set_state(context, State.MY_RECORDS)
            await update.message.reply_text(
                f"Ваша запись:\n{dt}\nСтатус: {status}",
                reply_markup=build_menu_kbd(["❌ Отменить запись"], two=False, back=True),
            )
            return

        if text == "🤖 О боте":
            await update.message.reply_text("Бот для записи в салон.", reply_markup=main_menu_kbd())
            return

        if text == "❓ Помощь":
            await update.message.reply_text(
                "Если возникли вопросы или проблемы, то скорее пиши создателю @imaginaryone.",
                reply_markup=main_menu_kbd(),
            )
            return

    # Choosing service
    if state == State.CHOOSING_SERVICE:
        sv = await cached_services()
        if text not in sv:
            await update.message.reply_text("Выберите услугу:", reply_markup=build_menu_kbd(list(sv.keys())))
            return
        context.user_data['chosen_service'] = text
        set_state(context, State.CHOOSING_TIME)
        avail = await build_available_time_strings()
        kb = [Config.CATCH_BUTTON] + avail if avail else [Config.CATCH_BUTTON]
        await update.message.reply_text("Выберите время:", reply_markup=build_menu_kbd(kb))
        return

    # Choosing time
    if state == State.CHOOSING_TIME:
        if is_back(text):
            set_state(context, State.CHOOSING_SERVICE)
            sv = await cached_services()
            await update.message.reply_text("Выберите услугу:", reply_markup=build_menu_kbd(list(sv.keys())))
            return
        if text == Config.CATCH_BUTTON:
            client = await find_client_for_user(update)
            exclude = client['id_client'] if client else None
            slots = await get_taken_times(exclude_client_id=exclude, min_hours=Config.CATCH_MIN_HOURS)
            if not slots:
                await update.message.reply_text("Нет занятых слотов для поимки", reply_markup=build_menu_kbd([], back=False))
                reset_state(context)
                return
            set_state(context, State.CHOOSING_CATCH)
            await update.message.reply_text("Выберите время для поимки:", reply_markup=build_menu_kbd(slots))
            return
        taken_now = await get_taken_times()
        if text in taken_now:
            set_state(context, State.OFFER_ADD_CATCH)
            context.user_data['offer_time'] = text
            await update.message.reply_text(
                "Добавить в очередь 'Поймать запись'?",
                reply_markup=build_menu_kbd(["➕ Да, добавить", "🔁 Выбрать другое"], two=False),
            )
            return
        valid = await build_available_time_strings()
        if text in valid:
            client = await find_client_for_user(update)
            if not client:
                await update.message.reply_text("Сначала /start")
                return
            svc = context.user_data.get('chosen_service')
            info = (await cached_services()).get(svc, {})
            d, p = text.split(" ")
            await sheets.add_booking(
                client['id_client'],
                f"{client.get('name', '')} {client.get('lastname', '')}",
                d,
                p,
                svc,
                info.get('price', 0),
                info.get('duration', 60),
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
                            f"Клиент: {client.get('name', '')} {client.get('lastname', '')}\n"
                            f"Телефон: {client.get('phone', '')}\n"
                            f"Telegram: {client.get('telegram', '')}"
                        ),
                    )
                except Exception:
                    pass
            return
        await update.message.reply_text(
            "Выберите корректное время",
            reply_markup=build_menu_kbd([t for t in await cached_times() if t not in await get_taken_times()]),
        )
        return

    # My records
    if state == State.MY_RECORDS:
        if text == "❌ Отменить запись":
            client = await find_client_for_user(update)
            if not client:
                await update.message.reply_text("Сначала /start", reply_markup=build_menu_kbd(["📝 Записаться"], two=True, back=False))
                return
            freed_dt, deleted_row = await sheets.remove_booking_by_client(client['id_client'])
            reset_state(context)
            await update.message.reply_text("Ваша запись отменена ✅", reply_markup=main_menu_kbd())

            if deleted_row:
                try:
                    admin_chat = await sheets.get_setting("ADMIN_CHAT_ID")
                    if admin_chat:
                        svc = deleted_row[6] if len(deleted_row) > 6 else ''
                        dt = deleted_row[5] if len(deleted_row) > 5 else ''
                        client_name = deleted_row[2] if len(deleted_row) > 2 else ''
                        client_phone = deleted_row[3] if len(deleted_row) > 3 else ''
                        client_telegram = deleted_row[4] if len(deleted_row) > 4 else ''
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

    # Offer add catch
    if state == State.OFFER_ADD_CATCH:
        if text == "➕ Да, добавить":
            desired = context.user_data.get('offer_time')
            client = await find_client_for_user(update)
            cid = client['id_client'] if client else ""
            ok = await sheets.add_catch(
                uid,
                desired,
                context.user_data.get('chosen_service', ''),
                cid,
                update.effective_chat.id,
            )
            reset_state(context)
            await update.message.reply_text("Добавлено" if ok else "Не удалось добавить", reply_markup=main_menu_kbd())
            return
        if text == "🔁 Выбрать другое":
            set_state(context, State.CHOOSING_TIME)
            await update.message.reply_text("Выберите время:", reply_markup=build_menu_kbd(await build_available_time_strings()))
            return

    # Choosing catch
    if state == State.CHOOSING_CATCH:
        if is_back(text):
            set_state(context, State.CHOOSING_TIME)
            await update.message.reply_text("Выберите время:", reply_markup=build_menu_kbd(await get_taken_times()))
            return
        taken_slots = await get_taken_times()
        if text in taken_slots:
            set_state(context, State.OFFER_ADD_CATCH)
            context.user_data['offer_time'] = text
            await update.message.reply_text("Добавить в очередь?", reply_markup=build_menu_kbd(["➕ Да, добавить", "↩️ Назад"], two=False))
            return

    await update.message.reply_text("Не понял, выберите из меню", reply_markup=main_menu_kbd())


# -------------------- Callbacks --------------------
@catch_errors
async def phone_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not q.data:
        return
    if q.data == "phone_confirm::yes":
        norm = context.user_data.get('reg_phone_normalized')
        name = context.user_data.get('reg_name')
        last = context.user_data.get('reg_last')
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
        set_state(context, State.REG_PHONE)
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
        if not hold or hold.get('time') != desired:
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
        if dt <= now() + timedelta(hours=Config.UNAVAILABLE_BEFORE_HOURS):
            await q.edit_message_text("Слишком поздно")
            holds.pop(uid, None)
            return
        client = registered.get(uid)
        if not client:
            await q.edit_message_text("Зарегистрируйтесь")
            return
        await sheets.remove_booking_by_client(client['id_client'])
        svc = hold.get('service', '')
        info = (await cached_services()).get(svc, {})
        d, t = desired.split(" ")
        await sheets.add_booking(
            client['id_client'],
            f"{client.get('name', '')} {client.get('lastname', '')}",
            d,
            t,
            svc,
            info.get('price', 0),
            info.get('duration', 60),
        )
        holds.pop(uid, None)
        rid = hold.get('row')
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
                    if 'статус' in h.lower():
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
        timedelta(minutes=Config.CATCH_WINDOW_MIN),
        dt - now() - timedelta(hours=Config.UNAVAILABLE_BEFORE_HOURS),
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
        allv = await sheets.get_all("Записи")
        hdr = allv[0] if allv else []
        rows = allv[1:] if len(allv) > 1 else []
        st_idx = None
        for j, h in enumerate(hdr):
            if 'статус' in h.lower():
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
            allv = await sheets.get_all("Записи")
            hdr = allv[0] if allv else []
            rows = allv[1:] if len(allv) > 1 else []
            st_idx = None
            notif_idx = None
            for j, h in enumerate(hdr):
                if 'статус' in h.lower():
                    st_idx = j + 1
                if 'бот уведом' in h.lower():
                    notif_idx = j + 1
            for i, r in enumerate(rows, start=2):
                try:
                    dt_str = r[5] if len(r) > 5 else ""
                    if not dt_str:
                        continue
                    dt = parse_dt(dt_str)
                    already = r[notif_idx - 1].strip().lower() == "да" if notif_idx and len(r) >= notif_idx else False
                    reminder_window = timedelta(hours=Config.UNAVAILABLE_BEFORE_HOURS)
                    if (
                        st_idx
                        and dt
                        and not already
                        and now() >= dt - reminder_window
                        and now() < dt - reminder_window + timedelta(seconds=Config.CLEANUP_INTERVAL)
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
                                kb = InlineKeyboardMarkup(
                                    [[InlineKeyboardButton("✅ Подтвердить", callback_data=f"confirm_booking::{r[0]}")]]
                                )
                                await app.bot.send_message(
                                    chat_id=tg_chat, text=f"🔔 Напоминание: запись на {dt_str}. Подтвердите.", reply_markup=kb
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
                    if st_idx and dt and now() >= dt - timedelta(hours=Config.UNAVAILABLE_BEFORE_HOURS) and (len(r) >= st_idx and r[st_idx - 1].strip().lower() == "не подтверждена"):
                        await sheets.update_cell("Записи", i, st_idx, "Отменена")
                        await notify_catchers_for_time(dt_str, app)
                except Exception:
                    continue
            await mark_past_bookings(app)
        except Exception as e:
            log.exception("Background error: %s", e)
        await asyncio.sleep(Config.CLEANUP_INTERVAL)


# -------------------- App bootstrap --------------------
def build_app():
    if not Config.TELEGRAM_TOKEN:
        log.critical("Set TELEGRAM_TOKEN env var")
        raise SystemExit(1)
    app = ApplicationBuilder().token(Config.TELEGRAM_TOKEN).build()
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

