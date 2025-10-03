import logging

from telegram.ext import ApplicationBuilder, CallbackQueryHandler, CommandHandler, MessageHandler, filters

from .background import background_tasks
from .config import TELEGRAM_TOKEN
from .handlers import catch_claim_cb, confirm_booking_cb, message_handler, phone_cb, set_state, setadmin, start
from .services import sheets


log = logging.getLogger("LyuNailsBot")


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
        import asyncio

        asyncio.create_task(background_tasks(application))

    app.post_init = post_init
    return app


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    app = build_app()
    log.info("Bot starting✅✅✅")
    app.run_polling()


if __name__ == "__main__":
    main()

