import re
import asyncio
import json
import uuid # Для генерации request_id
from typing import Dict, Any, Optional
from urllib.parse import urlparse

import aio_pika
from aio_pika.abc import IncomingMessage

from message_queue_manager import MessageQueueManager
from poster_parse.cian_parser import CianFlatRentParser
from posterData import PosterData, ResidentialComplex # Импортируем PosterData и ResidentialComplex

# Заглушка для асинхронного HTTP-запроса
import aiohttp
async def fetch_html_content(url: str) -> Optional[str]:
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=15) as response:
                response.raise_for_status()
                return await response.text()
    except aiohttp.ClientError as e:
        print(f"Ошибка при получении HTML с {url}: {e}")
        return None

from bs4 import BeautifulSoup

class ParserWorker:
    """
    Парсит объявления по URL, обогащает первичными данными (PosterData)
    и отправляет в очередь для гео-обогащения.
    """
    def __init__(self, amqp_url: str, 
                 parse_queue_name: str = "parse_queue",
                 geo_enrichment_queue_name: str = "geo_enrichment_queue"):
        self.mq_manager = MessageQueueManager(amqp_url)
        self.parse_queue_name = parse_queue_name
        self.geo_enrichment_queue_name = geo_enrichment_queue_name
        
        self.parsing_exchange_name = "parsing_exchange" # Exchange, откуда приходят запросы
        self.parse_routing_key = "parse_ad" # Маршрутизация для парсинга
        
        self.enrichment_exchange_name = "enrichment_exchange" # Exchange для цепочки обогащения
        self.geo_enrichment_routing_key = "enrich.geo" # Маршрутизация для гео-обогащения

        # Инициализация парсеров
        self.parsers = {
            "cian.ru/rent/flat": CianFlatRentParser(),
            # Добавьте другие парсеры здесь (avito, yandex_realty и т.д.)
        }

    async def initialize(self):
        """Инициализирует сервис: подключается к MQ, объявляет exchange и очереди."""
        await self.mq_manager.connect()
        # Объявляем очередь, из которой будем потреблять URL-ы (от RequestService/TelegramBot)
        await self.mq_manager.declare_queue(self.parse_queue_name)
        # BINDING для потребления из 'parsing_exchange'
        await self.mq_manager.declare_exchange(self.parsing_exchange_name, type=aio_pika.ExchangeType.TOPIC)
        await self.mq_manager.bind_queue_to_exchange(
            self.parse_queue_name, self.parsing_exchange_name, self.parse_routing_key
        )

        # Объявляем очередь и привязку для публикации в сервис гео-обогащения
        await self.mq_manager.declare_queue(self.geo_enrichment_queue_name)
        await self.mq_manager.declare_exchange(self.enrichment_exchange_name, type=aio_pika.ExchangeType.TOPIC)
        await self.mq_manager.bind_queue_to_exchange(
            self.geo_enrichment_queue_name, self.enrichment_exchange_name, self.geo_enrichment_routing_key
        )
        print(f"Парсер готов к работе. Слушает '{self.parse_queue_name}', публикует в '{self.geo_enrichment_queue_name}'.")

    def _get_parser_for_url(self, url: str):
        domain = urlparse(url).netloc
        path = urlparse(url).path
        
        if "cian.ru" in domain:
            if "rent/flat" in path:
                return self.parsers.get("cian.ru/rent/flat")
            # Добавьте другие условия для Циана (продажа, дома и т.д.)
        # Добавьте логику для других доменов (avito, yandex_realty)
        return None

    async def process_message(self, message: IncomingMessage):
        """Обрабатывает одно входящее сообщение."""
        async with message.process():
            try:
                msg_body = json.loads(message.body.decode('utf-8'))
                url = msg_body.get("url")
                request_id = msg_body.get("request_id", str(uuid.uuid4())) # Генерируем, если нет
                chat_id = msg_body.get("chat_id") # <- Извлекаем chat_id

                if not url:
                    print(f"[{request_id}] [Parser] Получено сообщение без URL. Пропускаем.")
                    return

                print(f"[{request_id}] [Parser] Получен URL: {url} (chat_id: {chat_id})")

                parser = self._get_parser_for_url(url)
                if not parser:
                    print(f"[{request_id}] [Parser] Парсер для URL '{url}' не найден. Пропускаем.")
                    # TODO: Отправить в очередь ошибок или нотификацию о невозможности обработки
                    return

                html_content = await fetch_html_content(url)
                if not html_content:
                    print(f"[{request_id}] [Parser] Не удалось получить HTML-контент для URL: {url}. Пропускаем.")
                    return

                soup = BeautifulSoup(html_content, 'html.parser')
                parsed_data = parser.parse(soup)

                # Заполняем обязательные поля PosterData
                # 'id' объявления обычно извлекается из URL или из данных страницы.
                # Для примера, извлечем из URL
                ad_id_match = re.search(r'(\d+)(?:/$|$)', url)
                ad_id = ad_id_match.group(1) if ad_id_match else str(uuid.uuid4()) # Fallback ID

                # Определяем section и property_type на основе URL
                section = "rent" if "rent" in url else "purchase"
                property_type = "flat" if "flat" in url else "house" # Упрощенно

                poster_data = PosterData(
                    id=ad_id,
                    url=url,
                    section=section,
                    property_type=property_type,
                    **parsed_data
                )
                
                # Сериализуем PosterData в словарь и добавляем request_id и chat_id
                poster_data_dict = poster_data.to_dict()
                poster_data_dict["request_id"] = request_id
                if chat_id is not None: # <- Добавляем chat_id, если он есть
                    poster_data_dict["chat_id"] = chat_id

                await self.mq_manager.publish_message(
                    self.enrichment_exchange_name, self.geo_enrichment_routing_key, poster_data_dict
                )
                print(f"[{request_id}] [Parser] Данные для ID {ad_id} отправлены в очередь '{self.geo_enrichment_queue_name}'.")

            except json.JSONDecodeError:
                print(f"[{request_id}] [Parser] Получено некорректное JSON сообщение: {message.body.decode('utf-8')}")
            except Exception as e:
                print(f"[{request_id}] [Parser] Неизвестная ошибка: {e}")
                # TODO: Логирование и обработка ошибок

    async def start_consuming(self):
        """Запускает потребление сообщений из очереди."""
        await self.mq_manager.consume_messages(self.parse_queue_name, self.process_message)
        print(f"Парсер слушает очередь '{self.parse_queue_name}'...")
        try:
            while True:
                await asyncio.sleep(3600) 
        except asyncio.CancelledError:
            print("Парсер остановлен.")
        except KeyboardInterrupt:
            print("Парсер остановлен (KeyboardInterrupt).")
        finally:
            await self.mq_manager.close()


async def main_parser_worker():
    amqp_url = "amqp://guest:guest@localhost:5672/"
    parser_worker = ParserWorker(amqp_url)
    await parser_worker.initialize()
    await parser_worker.start_consuming()

if __name__ == "__main__":
    # Порядок запуска:
    # 1. Запустить RabbitMQ
    # 2. Запустить этот скрипт (parser_worker.py)
    print("Запуск ParserWorker. Убедитесь, что RabbitMQ запущен.")
    asyncio.run(main_parser_worker())