from telegram.ext import Application, CommandHandler, MessageHandler, filters
from .handlers import start, handle_cian_link
import logging

logger = logging.getLogger(__name__)

class TelegramBot:
    def __init__(self, token: str):
        self.token = token
        self.app = None

    async def startup(self):
        self.app = Application.builder().token(self.token).build()
        self._register_handlers()
        await self.app.initialize()
        await self.app.start()
        await self.app.updater.start_polling()
        logger.info("Bot started")

    async def shutdown(self):
        if self.app:
            await self.app.updater.stop()
            await self.app.stop()
            await self.app.shutdown()

    def _register_handlers(self):
        self.app.add_handler(CommandHandler("start", start))
        self.app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_cian_link))

