from telegram import Update
from telegram.ext import ContextTypes
import logging
import json
import asyncio

from posterData import PosterData 
from message_queue_manager import MessageQueueManager 


hui = PosterData

logger = logging.getLogger(__name__)

# --- Инициализация и менеджер очередей ---
mq_manager_instance: MessageQueueManager | None = None

async def initialize_mq_for_bot():
    """
    Инициализирует подключение к RabbitMQ и объявляет очередь для отправки запросов.
    Будет вызван при запуске бота.
    """
    global mq_manager_instance
    if mq_manager_instance is None:
        mq_manager_instance = MessageQueueManager()
        try:
            await mq_manager_instance.connect()
            await mq_manager_instance.declare_exchange("parsing_exchange", type='topic')
            logger.info("Handlers: RabbitMQ connection and exchanges initialized for bot.")
        except Exception as e:
            logger.error(f"Handlers: Failed to initialize RabbitMQ connection: {e}")
            raise 

def format_prediction(data: dict) -> str:
    predicted_price = data.get('predicted_price', 'N/A')
    address = data.get('address', 'N/A')
    url = data.get('url', 'N/A')
    request_id = data.get('request_id', 'N/A')

    message_parts = [
        f"*Прогноз стоимости аренды* (Запрос: `{request_id}`)\n",
        f"🏠 *Адрес:* {address}\n",
        f"💰 *Справедливая цена:* {predicted_price} ₽/мес\n",
        f"[Посмотреть объявление]({url})"
    ]
    return "\n".join(message_parts)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🏡 Привет! Отправь ссылку на объявление с ЦИАН для оценки аренды.")

async def handle_cian_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    url = update.message.text
    chat_id = update.message.chat_id
    request_id = str(chat_id) + "_" + str(update.message.message_id) # Простой request_id для отслеживания

    if not mq_manager_instance or not mq_manager_instance.connection:
        logger.error("RabbitMQ manager not initialized or connected.")
        await update.message.reply_text("🔧 Система временно недоступна. Попробуйте позже.")
        return

    try:
        # Создаем сообщение для парсера
        message_to_send = {
            "url": url,
            "chat_id": chat_id, # Передаем chat_id по пайплайну
            "request_id": request_id
        }

        # Публикуем сообщение в очередь парсера
        await mq_manager_instance.publish_message(
            exchange_name="parsing_exchange",
            routing_key="parse.cian_flat_rent",
            message_body=json.dumps(message_to_send)
        )
        logger.info(f"[{request_id}] URL '{url}' sent to parsing queue.")
        await update.message.reply_text("⏳ Ваша ссылка принята в обработку. Пожалуйста, подождите результат.")

    except Exception as e:
        logger.error(f"[{request_id}] Error sending URL to MQ: {e}", exc_info=True)
        await update.message.reply_text("❌ Произошла ошибка при отправке ссылки. Пожалуйста, попробуйте еще раз.")