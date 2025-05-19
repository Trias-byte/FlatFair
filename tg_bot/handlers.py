from telegram import Update
from telegram.ext import ContextTypes
import aiohttp
import logging
import _config 

logger = logging.getLogger(__name__)
BACKEND_URL = _config.backend_url + '/api/predict'


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🏡 Привет! Отправь ссылку на объявление с ЦИАН для оценки аренды.")

async def handle_cian_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    url = update.message.text
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(BACKEND_URL, json={"url": url}) as response:
                if response.status == 200:
                    data = await response.json()
                    message = format_prediction(data)
                    await update.message.reply_text(message, parse_mode="Markdown")
                else:
                    await update.message.reply_text("❌ Ошибка сервера. Попробуйте позже.")
    except Exception as e:
        logger.error(f"Error: {e}")
        await update.message.reply_text("🔧 Произошла техническая ошибка.")

def format_prediction(data: dict) -> str:
    return (
        f"*Прогноз стоимости аренды*\n\n"
        f"🏠 Справедливая цена: {data['predicted_price']} ₽/мес\n"
        f"📊 Отклонение: {data['deviation']}\n"
        f"🔍 Основные факторы:\n- {data['top_features']}"
    )