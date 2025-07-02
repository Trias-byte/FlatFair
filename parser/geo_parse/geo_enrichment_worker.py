import asyncio
import json
from typing import Dict, Any, Optional
from urllib.parse import urlparse # Для парсинга URL, если потребуется

import aio_pika
from aio_pika.abc import IncomingMessage

from message_queue_manager import MessageQueueManager
from posterData import PosterData, DistrictInfo # Импортируем PosterData и DistrictInfo

class GeolocationService:
    async def get_district_info(self, address: str, coordinates: Optional[Dict[str, float]]) -> Optional[DistrictInfo]:
        print(f"  [GeoService] Запрос гео-данных для адреса: '{address}'")
        if "Санкт-Петербург" in address:
            region_name = "Санкт-Петербург"
            city_name = "Санкт-Петербург"
            if "Невский" in address:
                district_name = "Невский район"
                population = 520000
                metro_distance = 2.5 
                crime_rate = 0.05
                green_area_percentage = 0.15
            elif "Московский" in address:
                district_name = "Московский район"
                population = 350000
                metro_distance = 1.0
                crime_rate = 0.03
                green_area_percentage = 0.20
            else:
                district_name = "Неизвестный район"
                population = 0
                metro_distance = 10.0
                crime_rate = 0.1
                green_area_percentage = 0.05
            
            return DistrictInfo(
                region_name=region_name,
                city_name=city_name,
                district_name=district_name,
                population=population,
                avg_price_per_sqm=150000.0, 
                schools_count=20,
                hospitals_count=5,
                crime_rate=crime_rate,
                metro_distance=metro_distance,
                public_transport_accessibility=0.8,
                green_area_percentage=green_area_percentage,
                commercial_density=0.6
            )
        elif "Москва" in address:
            return DistrictInfo(
                region_name="Москва",
                city_name="Москва",
                district_name="Центральный АО", 
                population=1000000,
                avg_price_per_sqm=300000.0,
                schools_count=50,
                hospitals_count=10,
                crime_rate=0.04,
                metro_distance=0.5,
                public_transport_accessibility=0.95,
                green_area_percentage=0.10,
                commercial_density=0.8
            )
        return None


class GeoEnrichmentWorker:
    def __init__(self, amqp_url: str,
                 geo_enrichment_queue_name: str = "geo_enrichment_queue",
                 economic_enrichment_queue_name: str = "economic_enrichment_queue"):
        self.mq_manager = MessageQueueManager(amqp_url)
        self.geo_enrichment_queue_name = geo_enrichment_queue_name
        self.economic_enrichment_queue_name = economic_enrichment_queue_name
        self.enrichment_exchange_name = "enrichment_exchange"
        self.economic_enrichment_routing_key = "enrich.economic" 
        
        self.geolocation_service = GeolocationService()

    async def initialize(self):
        await self.mq_manager.connect()
        await self.mq_manager.declare_queue(self.geo_enrichment_queue_name)
        await self.mq_manager.declare_exchange(self.enrichment_exchange_name, type=aio_pika.ExchangeType.TOPIC)
        await self.mq_manager.bind_queue_to_exchange(
            self.geo_enrichment_queue_name, self.enrichment_exchange_name, "enrich.geo"
        )

        await self.mq_manager.declare_queue(self.economic_enrichment_queue_name)
        await self.mq_manager.bind_queue_to_exchange(
            self.economic_enrichment_queue_name, self.enrichment_exchange_name, self.economic_enrichment_routing_key
        )
        print(f"Гео-обогатитель готов к работе. Слушает '{self.geo_enrichment_queue_name}', публикует в '{self.economic_enrichment_queue_name}'.")

    async def process_message(self, message: IncomingMessage):
        async with message.process():
            try:
                msg_body = json.loads(message.body.decode('utf-8'))
                request_id = msg_body.get("request_id", "N/A")
                chat_id = msg_body.get("chat_id") # <- Извлекаем chat_id

                poster_data = PosterData(**msg_body) 

                print(f"[{request_id}] [GeoEnrich] Получены данные для ID: {poster_data.id}, URL: {poster_data.url} (chat_id: {chat_id})")

                if poster_data.address:
                    district_info = await self.geolocation_service.get_district_info(
                        poster_data.address, poster_data.coordinates
                    )
                    if district_info:
                        poster_data.district_info = district_info
                        print(f"[{request_id}] [GeoEnrich] Гео-данные обогащены для ID: {poster_data.id}")
                    else:
                        print(f"[{request_id}] [GeoEnrich] Не удалось получить гео-данные для адреса: {poster_data.address}")
                else:
                    print(f"[{request_id}] [GeoEnrich] Нет адреса для гео-обогащения для ID: {poster_data.id}")
                
                enriched_data_dict = poster_data.to_dict()
                enriched_data_dict["request_id"] = request_id 
                if chat_id is not None: # <- Добавляем chat_id, если он есть
                    enriched_data_dict["chat_id"] = chat_id

                await self.mq_manager.publish_message(
                    self.enrichment_exchange_name, self.economic_enrichment_routing_key, enriched_data_dict
                )
                print(f"[{request_id}] [GeoEnrich] Обогащенные гео-данными данные для ID {poster_data.id} отправлены в очередь '{self.economic_enrichment_queue_name}'.")

            except json.JSONDecodeError:
                print(f"[{request_id}] [GeoEnrich] Получено некорректное JSON сообщение: {message.body.decode('utf-8')}")
            except Exception as e:
                print(f"[{request_id}] [GeoEnrich] Неизвестная ошибка: {e}")

    async def start_consuming(self):
        await self.mq_manager.consume_messages(self.geo_enrichment_queue_name, self.process_message)
        print(f"Гео-обогатитель слушает очередь '{self.geo_enrichment_queue_name}'...")
        try:
            while True:
                await asyncio.sleep(3600) 
        except asyncio.CancelledError:
            print("Гео-обогатитель остановлен.")
        except KeyboardInterrupt:
            print("Гео-обогатитель остановлен (KeyboardInterrupt).")
        finally:
            await self.mq_manager.close()


async def main_geo_enrichment_worker():
    amqp_url = "amqp://guest:guest@localhost:5672/"
    geo_worker = GeoEnrichmentWorker(amqp_url)
    await geo_worker.initialize()
    await geo_worker.start_consuming()

if __name__ == "__main__":
    print("Запуск GeoEnrichmentWorker. Убедитесь, что RabbitMQ, RequestService и ParserWorker запущены.")
    asyncio.run(main_geo_enrichment_worker())