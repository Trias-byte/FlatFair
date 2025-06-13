import asyncio
import json
from typing import Dict, Any, Optional

import aio_pika
from aio_pika.abc import IncomingMessage
from motor.motor_asyncio import AsyncIOMotorClient

from message_queue_manager import MessageQueueManager
from posterData import PosterData

class RealEstateModel:
    async def analyze(self, poster_data: PosterData) -> Dict[str, Any]:
        print(f"  [AnalysisModel] Анализ объявления ID: {poster_data.id}")
        
        score = 0
        if poster_data.price and poster_data.area_total and poster_data.area_total > 0:
            price_per_sqm = poster_data.price / poster_data.area_total
            if price_per_sqm < 130000:
                score += 3
            elif price_per_sqm < 160000:
                score += 2
            else:
                score += 1

        if poster_data.rooms is not None and poster_data.rooms <= 2:
            score += 1

        if poster_data.district_info:
            if poster_data.district_info.metro_distance is not None and poster_data.district_info.metro_distance < 1.0:
                score += 2
            if poster_data.district_info.crime_rate is not None and poster_data.district_info.crime_rate < 0.04:
                score += 1

        if poster_data.economic_data and poster_data.economic_data.unemployment_rate is not None and poster_data.economic_data.unemployment_rate < 3.5:
            score += 1

        investment_attractiveness = "Низкая"
        if score >= 6:
            investment_attractiveness = "Высокая"
        elif score >= 4:
            investment_attractiveness = "Средняя"
        
        return {
            "ad_id": poster_data.id,
            "analysis_score": score,
            "investment_attractiveness": investment_attractiveness,
            "estimated_rent_yield": f"{score * 0.5}%"
        }


class AnalysisWorker:
    def __init__(self, amqp_url: str, mongo_uri: str, db_name: str, collection_name: str,
                 analysis_queue_name: str = "analysis_queue",
                 notification_queue_name: str = "notification_queue"):
        self.mq_manager = MessageQueueManager(amqp_url)
        self.db_client = AsyncIOMotorClient(mongo_uri)
        self.db = self.db_client[db_name]
        self.collection = self.db[collection_name]

        self.analysis_queue_name = analysis_queue_name
        self.notification_queue_name = notification_queue_name
        
        self.data_flow_exchange_name = "data_flow_exchange"
        self.notification_exchange_name = "notification_exchange"
        self.notification_routing_key = "notify.user"

        self.real_estate_model = RealEstateModel()

    async def initialize(self):
        await self.mq_manager.connect()
        await self.mq_manager.declare_queue(self.analysis_queue_name)
        await self.mq_manager.declare_exchange(self.data_flow_exchange_name, type=aio_pika.ExchangeType.TOPIC)
        await self.mq_manager.bind_queue_to_exchange(
            self.analysis_queue_name, self.data_flow_exchange_name, "analyze.ad"
        )

        await self.mq_manager.declare_exchange(self.notification_exchange_name, type=aio_pika.ExchangeType.TOPIC)
        await self.mq_manager.declare_queue(self.notification_queue_name)
        await self.mq_manager.bind_queue_to_exchange(
            self.notification_queue_name, self.notification_exchange_name, self.notification_routing_key
        )
        print(f"Аналитический воркер готов к работе. Слушает '{self.analysis_queue_name}', публикует в '{self.notification_queue_name}'.")

    async def process_message(self, message: IncomingMessage):
        async with message.process():
            try:
                msg_body = json.loads(message.body.decode('utf-8'))
                ad_id = msg_body.get("ad_id")
                mongo_id = msg_body.get("mongo_id")
                request_id = msg_body.get("request_id", "N/A")
                chat_id = msg_body.get("chat_id") # <- Извлекаем chat_id

                if not ad_id:
                    print(f"[{request_id}] [Analysis] Сообщение без 'ad_id'. Пропускаем.")
                    return

                print(f"[{request_id}] [Analysis] Получен запрос на анализ для ID: {ad_id}, MongoDB _id: {mongo_id} (chat_id: {chat_id})")

                poster_data_dict = await self.collection.find_one({"id": ad_id})
                
                if not poster_data_dict:
                    print(f"[{request_id}] [Analysis] Данные для ID {ad_id} не найдены в базе данных.")
                    return
                
                poster_data = PosterData(**poster_data_dict)

                analysis_results = await self.real_estate_model.analyze(poster_data)
                analysis_results["request_id"] = request_id
                analysis_results["original_ad_url"] = poster_data.url
                if chat_id is not None: # <- Добавляем chat_id, если он есть
                    analysis_results["chat_id"] = chat_id

                print(f"[{request_id}] [Analysis] Анализ для ID {ad_id} завершен. Результаты: {analysis_results.get('investment_attractiveness')}")

                await self.mq_manager.publish_message(
                    self.notification_exchange_name, self.notification_routing_key, analysis_results
                )
                print(f"[{request_id}] [Analysis] Результаты анализа для ID {ad_id} отправлены в очередь уведомлений.")

            except json.JSONDecodeError:
                print(f"[{request_id}] [Analysis] Получено некорректное JSON сообщение: {message.body.decode('utf-8')}")
            except Exception as e:
                print(f"[{request_id}] [Analysis] Неизвестная ошибка: {e}")

    async def start_consuming(self):
        await self.mq_manager.consume_messages(self.analysis_queue_name, self.process_message)
        print(f"Аналитический воркер слушает очередь '{self.analysis_queue_name}'...")
        try:
            while True:
                await asyncio.sleep(3600) 
        except asyncio.CancelledError:
            print("Аналитический воркер остановлен.")
        except KeyboardInterrupt:
            print("Аналитический воркер остановлен (KeyboardInterrupt).")
        finally:
            self.db_client.close()
            await self.mq_manager.close()


async def main_analysis_worker():
    amqp_url = "amqp://guest:guest@localhost:5672/"
    mongo_uri = "mongodb://localhost:27017/"
    db_name = "real_estate_db"
    collection_name = "posters"

    analysis_worker = AnalysisWorker(amqp_url, mongo_uri, db_name, collection_name)
    await analysis_worker.initialize()
    await analysis_worker.start_consuming()

if __name__ == "__main__":
    print("Запуск AnalysisWorker. Убедитесь, что все предыдущие сервисы запущены.")
    asyncio.run(main_analysis_worker())
