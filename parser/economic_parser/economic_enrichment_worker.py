import asyncio
import json
from typing import Dict, Any, Optional

import aio_pika
from aio_pika.abc import IncomingMessage

from message_queue_manager import MessageQueueManager
from posterData import PosterData, EconomicData

class EconomicDataService:
    async def get_economic_data(self, region_name: str) -> Optional[EconomicData]:
        print(f"  [EcoService] Запрос экономических данных для региона: '{region_name}'")
        if "Санкт-Петербург" in region_name:
            return EconomicData(
                region_name=region_name,
                avg_life_expectancy=74.5,
                key_interest_rate=16.0, 
                credit_approval_rate=0.65,
                avg_earnings=100000.0, 
                gdp_per_capita=25000.0, 
                unemployment_rate=3.0
            )
        elif "Москва" in region_name:
            return EconomicData(
                region_name=region_name,
                avg_life_expectancy=78.0,
                key_interest_rate=16.0,
                credit_approval_rate=0.70,
                avg_earnings=150000.0,
                gdp_per_capita=40000.0,
                unemployment_rate=2.5
            )
        elif "Ленинградская область" in region_name:
            return EconomicData(
                region_name=region_name,
                avg_life_expectancy=72.0,
                key_interest_rate=16.0,
                credit_approval_rate=0.60,
                avg_earnings=70000.0,
                gdp_per_capita=18000.0,
                unemployment_rate=4.0
            )
        return None


class EconomicEnrichmentWorker:
    def __init__(self, amqp_url: str,
                 economic_enrichment_queue_name: str = "economic_enrichment_queue",
                 db_save_queue_name: str = "db_save_queue"):
        self.mq_manager = MessageQueueManager(amqp_url)
        self.economic_enrichment_queue_name = economic_enrichment_queue_name
        self.db_save_queue_name = db_save_queue_name
        self.enrichment_exchange_name = "enrichment_exchange"
        self.db_save_routing_key = "data.save"
        
        self.economic_service = EconomicDataService()

    async def initialize(self):
        await self.mq_manager.connect()
        await self.mq_manager.declare_queue(self.economic_enrichment_queue_name)
        await self.mq_manager.declare_exchange(self.enrichment_exchange_name, type=aio_pika.ExchangeType.TOPIC)
        await self.mq_manager.bind_queue_to_exchange(
            self.economic_enrichment_queue_name, self.enrichment_exchange_name, "enrich.economic"
        )

        await self.mq_manager.declare_queue(self.db_save_queue_name)
        await self.mq_manager.bind_queue_to_exchange(
            self.db_save_queue_name, self.enrichment_exchange_name, self.db_save_routing_key
        )
        print(f"Экономический обогатитель готов к работе. Слушает '{self.economic_enrichment_queue_name}', публикует в '{self.db_save_queue_name}'.")

    async def process_message(self, message: IncomingMessage):
        async with message.process():
            try:
                msg_body = json.loads(message.body.decode('utf-8'))
                request_id = msg_body.get("request_id", "N/A")
                chat_id = msg_body.get("chat_id") # <- Извлекаем chat_id
                
                poster_data = PosterData(**msg_body) 

                print(f"[{request_id}] [EcoEnrich] Получены данные для ID: {poster_data.id}, URL: {poster_data.url} (chat_id: {chat_id})")

                region_name_for_eco = None
                if poster_data.district_info and poster_data.district_info.region_name:
                    region_name_for_eco = poster_data.district_info.region_name
                elif poster_data.address: 
                    if "Санкт-Петербург" in poster_data.address:
                        region_name_for_eco = "Санкт-Петербург"
                    elif "Москва" in poster_data.address:
                        region_name_for_eco = "Москва"

                if region_name_for_eco:
                    economic_data = await self.economic_service.get_economic_data(region_name_for_eco)
                    if economic_data:
                        poster_data.economic_data = economic_data
                        print(f"[{request_id}] [EcoEnrich] Экономические данные обогащены для ID: {poster_data.id}")
                    else:
                        print(f"[{request_id}] [EcoEnrich] Не удалось получить экономические данные для региона: {region_name_for_eco}")
                else:
                    print(f"[{request_id}] [EcoEnrich] Нет информации о регионе для экономического обогащения для ID: {poster_data.id}")
                
                enriched_data_dict = poster_data.to_dict()
                enriched_data_dict["request_id"] = request_id 
                if chat_id is not None: # <- Добавляем chat_id, если он есть
                    enriched_data_dict["chat_id"] = chat_id

                await self.mq_manager.publish_message(
                    self.enrichment_exchange_name, self.db_save_routing_key, enriched_data_dict
                )
                print(f"[{request_id}] [EcoEnrich] Обогащенные данные для ID {poster_data.id} отправлены в очередь '{self.db_save_queue_name}'.")

            except json.JSONDecodeError:
                print(f"[{request_id}] [EcoEnrich] Получено некорректное JSON сообщение: {message.body.decode('utf-8')}")
            except Exception as e:
                print(f"[{request_id}] [EcoEnrich] Неизвестная ошибка: {e}")

    async def start_consuming(self):
        await self.mq_manager.consume_messages(self.economic_enrichment_queue_name, self.process_message)
        print(f"Экономический обогатитель слушает очередь '{self.economic_enrichment_queue_name}'...")
        try:
            while True:
                await asyncio.sleep(3600) 
        except asyncio.CancelledError:
            print("Экономический обогатитель остановлен.")
        except KeyboardInterrupt:
            print("Экономический обогатитель остановлен (KeyboardInterrupt).")
        finally:
            await self.mq_manager.close()


async def main_economic_enrichment_worker():
    amqp_url = "amqp://guest:guest@localhost:5672/"
    economic_worker = EconomicEnrichmentWorker(amqp_url)
    await economic_worker.initialize()
    await economic_worker.start_consuming()

if __name__ == "__main__":
    print("Запуск EconomicEnrichmentWorker. Убедитесь, что все предыдущие сервисы запущены.")
    asyncio.run(main_economic_enrichment_worker())