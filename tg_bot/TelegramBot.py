from telegram.ext import Application, CommandHandler, MessageHandler, filters
import logging
import asyncio
import os
import json
import aio_pika

from handlers import start, handle_cian_link, initialize_mq_for_bot, format_prediction, mq_manager_instance 

logger = logging.getLogger(__name__)

# --- Новый класс для потребления уведомлений Telegram ---
class TelegramNotificationConsumer:
    def __init__(self, app_for_bot: Application): # amqp_url больше не нужен, берем из mq_manager_instance
        self.app = app_for_bot
        self.mq_manager = mq_manager_instance 
        self.notification_queue_name = "notification_queue"
        self.notification_exchange_name = "notification_exchange"

    async def initialize(self):
        try:
            await self.mq_manager.declare_exchange(self.notification_exchange_name, type=aio_pika.ExchangeType.TOPIC)
            await self.mq_manager.declare_queue(self.notification_queue_name)
            await self.mq_manager.bind_queue_to_exchange(
                self.notification_queue_name, self.notification_exchange_name, "notify.user"
            )
            logger.info("Telegram Notification Consumer: RabbitMQ queues for notifications initialized.")
        except Exception as e:
            logger.error(f"Telegram Notification Consumer: Failed to initialize RabbitMQ queues: {e}")
            raise

    async def start_consuming(self):
        if not self.mq_manager or not self.mq_manager.channel:
            logger.error("Telegram Notification Consumer: MQ manager or channel not available. Cannot start consuming.")
            return

        try:
            queue = await self.mq_manager.channel.get_queue(self.notification_queue_name)
            await queue.consume(self.process_notification_message, no_ack=False)
            logger.info(f"Telegram Notification Consumer: Started consuming from '{self.notification_queue_name}'.")
            await asyncio.Future() # Keep consumer alive
        except Exception as e:
            logger.error(f"Telegram Notification Consumer: Error during consumption: {e}", exc_info=True)

    async def process_notification_message(self, message: aio_pika.IncomingMessage):
        async with message.process():
            try:
                msg_body = json.loads(message.body.decode('utf-8'))
                chat_id = msg_body.get("chat_id")
                request_id = msg_body.get("request_id", "N/A")

                if not chat_id:
                    logger.warning(f"[{request_id}] Notification message missing 'chat_id'. Cannot send Telegram message.")
                    return

                logger.info(f"[{request_id}] Telegram Notification Consumer: Received analysis results for chat_id: {chat_id}")

                telegram_message = format_prediction(msg_body) # Используем format_prediction из handlers

                await self.app.bot.send_message(chat_id=chat_id, text=telegram_message, parse_mode="Markdown")
                logger.info(f"[{request_id}] Telegram Notification Consumer: Notification sent to chat_id: {chat_id}.")

            except json.JSONDecodeError:
                logger.error(f"[{request_id}] Telegram Notification Consumer: Received invalid JSON: {message.body.decode('utf-8')}", exc_info=True)
            except Exception as e:
                logger.error(f"[{request_id}] Telegram Notification Consumer: Error processing notification: {e}", exc_info=True)


class TelegramBot:
    def __init__(self, token: str):
        self.token = token
        self.app: Application | None = None
        self.notification_consumer: TelegramNotificationConsumer | None = None

    async def startup(self):
        # 1. Инициализируем соединение RabbitMQ для отправки запросов (handlers)
        await initialize_mq_for_bot()

        # 2. Инициализируем Telegram Application
        self.app = Application.builder().token(self.token).build()
        self._register_handlers()
        await self.app.initialize()

        # 3. Инициализируем потребитель уведомлений Telegram
        self.notification_consumer = TelegramNotificationConsumer(self.app)
        await self.notification_consumer.initialize()

        await self.app.updater.start_polling()
        await self.app.start()

        self.consumer_task = asyncio.create_task(self.notification_consumer.start_consuming())

        logger.info("Bot and Notification Consumer started. Press Ctrl+C to stop.")

        await self.app.run_polling()

        logger.info("Telegram Bot application stopped.")

    async def shutdown(self):
        if self.app:
            logger.info("Stopping Telegram Bot application...")
            await self.app.updater.stop()
            await self.app.stop()
            await self.app.shutdown()
        if self.notification_consumer and self.consumer_task:
            logger.info("Cancelling Telegram Notification Consumer task...")
            self.consumer_task.cancel()
            try:
                await self.consumer_task
            except asyncio.CancelledError:
                logger.info("Telegram Notification Consumer task cancelled successfully.")

        if mq_manager_instance and mq_manager_instance.connection:
            logger.info("Closing RabbitMQ connection for bot.")
            await mq_manager_instance.close()

        logger.info("Bot and Notification Consumer shutdown complete.")

    def _register_handlers(self):
        self.app.add_handler(CommandHandler("start", start)) 
        self.app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_cian_link)) 

async def main():
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not bot_token:
        logger.error("TELEGRAM_BOT_TOKEN environment variable not set. Please set it.")
        raise ValueError("TELEGRAM_BOT_TOKEN is required.")

    # AMQP_URL читается MessageQueueManager из os.getenv
    bot = TelegramBot(token=bot_token)
    try:
        await bot.startup()
    except Exception as e:
        logger.critical(f"Fatal error during bot startup: {e}", exc_info=True)
    finally:
        await bot.shutdown()

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    asyncio.run(main())