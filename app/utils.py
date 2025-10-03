import asyncio
import logging
import re
import time
import traceback
from datetime import datetime
from functools import wraps
from typing import Any, Awaitable, Callable, Dict, Optional

from telegram import Update
from telegram.ext import ContextTypes

from .config import TZ


log = logging.getLogger("LyuNailsBot")


def now() -> datetime:
    return datetime.now(TZ)


def parse_dt(value: str) -> datetime:
    return TZ.localize(datetime.strptime(value, "%d.%m.%Y %H:%M"))


def fmt_dt(value: datetime) -> str:
    return value.astimezone(TZ).strftime("%d.%m.%Y %H:%M")


def ttl_cache(ttl_seconds: int):
    """Simple TTL cache decorator for async functions with small return values."""

    def decorator(func: Callable[..., Awaitable[Any]]):
        cache: Dict[str, Any] = {"value": None, "exp": 0.0}

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


def normalize_phone(raw: str) -> Optional[str]:
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


def catch_errors(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            return await func(update, context)
        except Exception as exc:  # pragma: no cover
            tb = traceback.format_exc()
            log.exception("Handler error: %s", exc)
            from .services import sheets  # lazy import to avoid cycles
            try:
                admin = await sheets.get_setting("ADMIN_CHAT_ID")
            except Exception:
                admin = None
            try:
                if update and update.effective_message:
                    await update.effective_message.reply_text("Произошла ошибка. Администратор уведомлён.")
                if admin:
                    await context.bot.send_message(chat_id=int(admin), text=f"ERROR {exc}\n\n{tb[:2000]}")
            except Exception:
                log.exception("Failed to report error")

    return wrapper