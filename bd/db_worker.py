# db_worker.py

import asyncio
import json
from typing import Dict, Any, Optional

import aio_pika
from aio_pika.abc import IncomingMessage
from motor.motor_asyncio import AsyncIOMotorClient

from message_queue_manager import MessageQueueManager
from posterData import PosterData 


class DatabaseService:
    def __init__(self, mongo_uri: str, db_name: str, collection_name: str):
        self.client = AsyncIOMotorClient(mongo_uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
        print(f"Подключено к MongoDB: DB='{db_name}', Collection='{collection_name}'")

    async def save_poster_data(self, poster_data_dict: Dict[str, Any]) -> str:
        await self.collection.create_index("id", unique=True)

        ad_id = poster_data_dict.get("id")
        if not ad_id:
            raise ValueError("PosterData dictionary must contain an 'id' field for saving.")

        # УБЕДИТЕСЬ, ЧТО chat_id НЕ ВХОДИТ В poster_data_dict, КОТОРЫЙ СОХРАНЯЕТСЯ
        # Он должен быть удален, если случайно попал, но мы позаботились об этом,
        # передавая его отдельно от PosterData.to_dict()
        data_to_save = {k: v for k, v in poster_data_dict.items() if k not in ["request_id", "chat_id"]}

        result = await self.collection.update_one(
            {"id": ad_id},               
            {"$set": data_to_save},  # <- Используем data_to_save
            upsert=True                  
        )

        if result.upserted_id:
            print(f"  [DBService] Вставлен новый документ с ID объявления: {ad_id}, MongoDB _id: {result.upserted_id}")
            return str(result.upserted_id)
        elif result.modified_count > 0:
            print(f"  [DBService] Обновлен существующий документ с ID объявления: {ad_id}")
            existing_doc = await self.collection.find_one({"id": ad_id}, {"_id": 1})
            return str(existing_doc["_id"])
        else:
            print(f"  [DBService] Документ с ID объявления: {ad_id} не изменился (данные идентичны).")
            existing_doc = await self.collection.find_one({"id": ad_id}, {"_id": 1})
            return str(existing_doc["_id"])


    async def close(self):
        self.client.close()
        print("Соединение с MongoDB закрыто.")


class DatabaseWorker:
    def __init__(self, amqp_url: str, mongo_uri: str, db_name: str, collection_name: str,
                 db_save_queue_name: str = "db_save_queue",
                 analysis_queue_name: str = "analysis_queue"):
        self.mq_manager = MessageQueueManager(amqp_url)
        self.db_save_queue_name = db_save_queue_name
        self.analysis_queue_name = analysis_queue_name
        self.db_service = DatabaseService(mongo_uri, db_name, collection_name)
        
        self.data_flow_exchange_name = "data_flow_exchange"
        self.analysis_routing_key = "analyze.ad"

    async def initialize(self):
        await self.mq_manager.connect()
        await self.mq_manager.declare_queue(self.db_save_queue_name)
        await self.mq_manager.declare_exchange("enrichment_exchange", type=aio_pika.ExchangeType.TOPIC)
        await self.mq_manager.bind_queue_to_exchange(
            self.db_save_queue_name, "enrichment_exchange", "data.save"
        )

        await self.mq_manager.declare_exchange(self.data_flow_exchange_name, type=aio_pika.ExchangeType.TOPIC)
        await self.mq_manager.declare_queue(self.analysis_queue_name)
        await self.mq_manager.bind_queue_to_exchange(
            self.analysis_queue_name, self.data_flow_exchange_name, self.analysis_routing_key
        )
        print(f"DB-воркер готов к работе. Слушает '{self.db_save_queue_name}', публикует в '{self.analysis_queue_name}'.")

    async def process_message(self, message: IncomingMessage):
        async with message.process():
            try:
                msg_body = json.loads(message.body.decode('utf-8'))
                request_id = msg_body.get("request_id", "N/A")
                chat_id = msg_body.get("chat_id") # <- Извлекаем chat_id

                poster_data_dict_for_db = {k: v for k, v in msg_body.items() if k not in ["request_id", "chat_id"]} # <- Фильтруем перед сохранением

                print(f"[{request_id}] [DBWorker] Получены данные для сохранения ID: {poster_data_dict_for_db.get('id')} (chat_id: {chat_id})")

                mongo_id = await self.db_service.save_poster_data(poster_data_dict_for_db)
                
                analysis_message = {
                    "ad_id": poster_data_dict_for_db.get("id"),
                    "mongo_id": mongo_id, 
                    "request_id": request_id,
                    "url": poster_data_dict_for_db.get("url")
                }
                if chat_id is not None: # <- Добавляем chat_id, если он есть
                    analysis_message["chat_id"] = chat_id

                await self.mq_manager.publish_message(
                    self.data_flow_exchange_name, self.analysis_routing_key, analysis_message
                )
                print(f"[{request_id}] [DBWorker] ID {poster_data_dict_for_db.get('id')} отправлен в очередь анализа '{self.analysis_queue_name}'.")

            except json.JSONDecodeError:
                print(f"[{request_id}] [DBWorker] Получено некорректное JSON сообщение: {message.body.decode('utf-8')}")
            except Exception as e:
                print(f"[{request_id}] [DBWorker] Неизвестная ошибка: {e}")

    async def start_consuming(self):
        await self.mq_manager.consume_messages(self.db_save_queue_name, self.process_message)
        print(f"DB-воркер слушает очередь '{self.db_save_queue_name}'...")
        try:
            while True:
                await asyncio.sleep(3600) 
        except asyncio.CancelledError:
            print("DB-воркер остановлен.")
        except KeyboardInterrupt:
            print("DB-воркер остановлен (KeyboardInterrupt).")
        finally:
            await self.db_service.close()
            await self.mq_manager.close()


async def main_db_worker():
    amqp_url = "amqp://guest:guest@localhost:5672/"
    mongo_uri = "mongodb://localhost:27017/"
    db_name = "real_estate_db"
    collection_name = "posters"

    db_worker = DatabaseWorker(amqp_url, mongo_uri, db_name, collection_name)
    await db_worker.initialize()
    await db_worker.start_consuming()

if __name__ == "__main__":
    print("Запуск DBWorker. Убедитесь, что RabbitMQ, MongoDB и все предыдущие сервисы запущены.")
    asyncio.run(main_db_worker())