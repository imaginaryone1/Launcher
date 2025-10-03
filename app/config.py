import os
from datetime import timedelta

import pytz


# -------------------- Timezone --------------------
TIMEZONE_NAME = os.getenv("TZ", "Europe/Moscow")
TZ = pytz.timezone(TIMEZONE_NAME)


# -------------------- Google Sheets --------------------
SPREADSHEET_KEY = os.getenv("SPREADSHEET_KEY", "")
CREDS_FILE = os.getenv("GSHEET_CREDS", "credentials.json")
GS_RETRY = int(os.getenv("GS_RETRY", "3"))
GS_BACKOFF = float(os.getenv("GS_BACKOFF", "0.5"))


# -------------------- Telegram --------------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "12345")


# -------------------- Intervals & thresholds --------------------
CLEANUP_INTERVAL = int(os.getenv("CLEANUP_INTERVAL", "60"))
UNAVAILABLE_BEFORE_HOURS = int(os.getenv("UNAVAILABLE_BEFORE_HOURS", "24"))
DISPLAY_MIN_HOURS = int(os.getenv("DISPLAY_MIN_HOURS", "28"))
CATCH_WINDOW_MIN = int(os.getenv("CATCH_WINDOW_MIN", "30"))
CATCH_MIN_HOURS = int(os.getenv("CATCH_MIN_HOURS", "36"))


# -------------------- UI constants --------------------
BACK_TEXT = "↩️ Назад"
CATCH_BUTTON = "⏱️ Поймать запись"
MAIN_MENU_BUTTONS = [
    "📝 Записаться",
    "📋 Мои записи",
    "❓ Помощь",
    "🤖 О боте",
]


# -------------------- Derived --------------------
UNAVAILABLE_BEFORE_DELTA = timedelta(hours=UNAVAILABLE_BEFORE_HOURS)