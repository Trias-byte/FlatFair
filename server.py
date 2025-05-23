from fastapi import FastAPI
from tg_bot.TelegramBot import TelegramBot
import uvicorn
import asyncio
from contextlib import asynccontextmanager
import _config


bot = TelegramBot(token=_config.bot_token)

@asynccontextmanager
async def lifespan(app: FastAPI):
    await bot.startup()
    yield
    await bot.shutdown()

app = FastAPI(lifespan=lifespan)

@app.get("/health")
async def health():
    return {"status": "OK"}

@app.post("/api/predict")
async def predict_rent_price(url: str) -> dict:
    # Ваша ML-логика здесь
    return {
        "predicted_price": 72000,
        "deviation": "14%",
        "top_features": ["метро > 1 км", "отсутствие ремонта"]
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)