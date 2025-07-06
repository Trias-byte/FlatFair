import asyncio
import json
import uuid
from typing import Dict, Any, Optional
from urllib.parse import urlparse
import logging
from datetime import datetime
from dataclasses import dataclass
from enum import Enum
import re

from aio_pika import IncomingMessage
import aiohttp
from bs4 import BeautifulSoup

from posterData import PosterData, ProcessingStatus
from message_queue_manager import MessageQueueManager
from parser.poster_parse.cian_parser import CianFlatRentParser

logger = logging.getLogger(__name__)

class ParseError(Exception):
    """Базовое исключение для ошибок парсинга"""
    pass

class NetworkError(ParseError):
    """Ошибка сети"""
    pass

class ParserNotFoundError(ParseError):
    """Парсер не найден"""
    pass

class ContentError(ParseError):
    """Ошибка контента"""
    pass

@dataclass
class ParseResult:
    """Результат парсинга"""
    success: bool
    data: Optional[PosterData] = None
    error: Optional[str] = None
    retry_count: int = 0

class ParserWorker:
    """Улучшенный воркер парсинга с обработкой ошибок и retry логикой"""
    
    def __init__(self, amqp_url: str, 
                 parse_queue_name: str = "parse_queue",
                 geo_enrichment_queue_name: str = "geo_enrichment_queue",
                 dead_letter_queue_name: str = "parse_dead_letter_queue",
                 max_retries: int = 3,
                 retry_delay: int = 5):
        
        self.mq_manager = MessageQueueManager(amqp_url, max_retries, retry_delay)
        self.parse_queue_name = parse_queue_name
        self.geo_enrichment_queue_name = geo_enrichment_queue_name
        self.dead_letter_queue_name = dead_letter_queue_name
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        
        # Статистика
        self.processed_count = 0
        self.error_count = 0
        self.retry_count = 0
        
        # Настройки exchange и routing keys
        self.parsing_exchange_name = "parsing_exchange"
        self.parse_routing_key = "parse.ad"
        self.enrichment_exchange_name = "enrichment_exchange"
        self.geo_enrichment_routing_key = "enrich.geo"
        self.dead_letter_routing_key = "parse.failed"
        
        # Инициализация парсеров
        self.parsers = {
            "cian.ru/rent/flat": CianFlatRentParser(),
            # Здесь можно добавить другие парсеры
        }
        
        # HTTP клиент настройки
        self.http_timeout = aiohttp.ClientTimeout(total=30)
        self.http_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
    
    async def initialize(self):
        """Инициализация воркера с настройкой всех очередей"""
        await self.mq_manager.connect()
        
        # Объявляем основные очереди
        await self.mq_manager.declare_queue(self.parse_queue_name)
        await self.mq_manager.declare_queue(self.geo_enrichment_queue_name)
        await self.mq_manager.declare_queue(self.dead_letter_queue_name)
        
        # Объявляем exchanges
        await self.mq_manager.declare_exchange(self.parsing_exchange_name)
        await self.mq_manager.declare_exchange(self.enrichment_exchange_name)
        
        # Привязываем очереди к exchanges
        await self.mq_manager.bind_queue_to_exchange(
            self.parse_queue_name, 
            self.parsing_exchange_name, 
            self.parse_routing_key
        )
        
        await self.mq_manager.bind_queue_to_exchange(
            self.geo_enrichment_queue_name,
            self.enrichment_exchange_name,
            self.geo_enrichment_routing_key
        )
        
        await self.mq_manager.bind_queue_to_exchange(
            self.dead_letter_queue_name,
            self.parsing_exchange_name,
            self.dead_letter_routing_key
        )
        
        logger.info(f"Парсер воркер инициализирован. Очереди: {self.parse_queue_name} -> {self.geo_enrichment_queue_name}")
    
    def _get_parser_for_url(self, url: str):
        """Определение парсера для URL"""
        try:
            parsed_url = urlparse(url)
            domain = parsed_url.netloc.lower()
            path = parsed_url.path.lower()
            
            if "cian.ru" in domain:
                if "rent/flat" in path:
                    return self.parsers.get("cian.ru/rent/flat")
                elif "sale/flat" in path:
                    # Добавить парсер для продажи
                    return None
            elif "avito.ru" in domain:
                # Добавить парсер Avito
                return None
            elif "yandex.ru" in domain and "realty" in domain:
                # Добавить парсер Яндекс.Недвижимость
                return None
            
            return None
        except Exception as e:
            logger.error(f"Ошибка при определении парсера для URL {url}: {e}")
            return None
    
    async def _fetch_html_content(self, url: str) -> str:
        """Получение HTML контента с retry логикой"""
        for attempt in range(self.max_retries):
            try:
                async with aiohttp.ClientSession(
                    timeout=self.http_timeout,
                    headers=self.http_headers
                ) as session:
                    async with session.get(url) as response:
                        if response.status == 200:
                            content = await response.text()
                            if len(content) > 1000:  # Базовая проверка на валидность контента
                                return content
                            else:
                                raise ContentError(f"Контент слишком короткий: {len(content)} символов")
                        elif response.status == 404:
                            raise ContentError(f"Страница не найдена: {response.status}")
                        elif response.status == 403:
                            raise ContentError(f"Доступ запрещен: {response.status}")
                        else:
                            raise NetworkError(f"HTTP ошибка: {response.status}")
                            
            except aiohttp.ClientError as e:
                if attempt < self.max_retries - 1:
                    logger.warning(f"Попытка {attempt + 1} получения HTML неуспешна: {e}. Повтор через {self.retry_delay}с")
                    await asyncio.sleep(self.retry_delay)
                else:
                    raise NetworkError(f"Не удалось получить HTML после {self.max_retries} попыток: {e}")
            except Exception as e:
                raise NetworkError(f"Неожиданная ошибка при получении HTML: {e}")
        
        raise NetworkError("Превышено максимальное количество попыток")
    
    async def _parse_content(self, html_content: str, parser, url: str) -> Dict[str, Any]:
        """Парсинг контента с обработкой ошибок"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Проверяем, что страница загрузилась корректно
            if not soup.find('body'):
                raise ContentError("HTML не содержит тег body")
            
            # Проверяем на блокировку/каптчу
            if soup.find(text=lambda text: text and 'captcha' in text.lower()):
                raise ContentError("Обнаружена капча")
            
            if soup.find(text=lambda text: text and 'blocked' in text.lower()):
                raise ContentError("IP заблокирован")
            
            parsed_data = parser.parse(soup)
            
            # Валидация результата парсинга
            if not parsed_data:
                raise ContentError("Парсер не вернул данные")
            
            return parsed_data
            
        except Exception as e:
            logger.error(f"Ошибка парсинга контента: {e}")
            raise ContentError(f"Ошибка парсинга: {e}")
    
    async def _extract_ad_id(self, url: str) -> str:
        try:
            cian_match = re.search(r'/(\d+)/?', url)
            if cian_match :
                return cian_match.group(1)
            avito_match = re.search(r'_(\d+)', url)
            if avito_match:
                return avito_match.group(1)
            
            # Fallback - хеш URL
            return str(abs(hash(url)))
        except Exception as e:
            logger.warning(f"Не удалось извлечь ID из URL {url}: {e}")
            return str(uuid.uuid4())
    
    async def _determine_section_and_type(self, url: str) -> tuple:
        """Определение section и property_type из URL"""
        try:
            url_lower = url.lower()
            
            # Определяем section
            if "rent" in url_lower or "arenda" in url_lower:
                section = "rent"
            elif "sale" in url_lower or "prodazha" in url_lower:
                section = "purchase"
            else:
                section = "unknown"
            
            # Определяем property_type
            if "flat" in url_lower or "kvartira" in url_lower:
                property_type = "flat"
            elif "house" in url_lower or "dom" in url_lower:
                property_type = "house"
            elif "commercial" in url_lower or "kommercheskaya" in url_lower:
                property_type = "commercial"
            else:
                property_type = "unknown"
            
            return section, property_type
            
        except Exception as e:
            logger.error(f"Ошибка определения типа из URL {url}: {e}")
            return "unknown", "unknown"
    
    async def _create_poster_data(self, parsed_data: Dict[str, Any], url: str) -> PosterData:
        """Создание объекта PosterData из распарсенных данных"""
        try:
            ad_id = await self._extract_ad_id(url)
            section, property_type = await self._determine_section_and_type(url)
            
            # Создаем объект PosterData
            poster_data = PosterData(
                id=ad_id,
                url=url,
                section=section,
                property_type=property_type,
                processing_status=ProcessingStatus.PARSING,
                **parsed_data
            )
            
            # Проверяем валидность
            if not poster_data.is_valid():
                logger.warning(f"Созданный PosterData для {url} не прошел валидацию: {poster_data.error_messages}")
            
            poster_data.set_status(ProcessingStatus.COMPLETED)
            return poster_data
            
        except Exception as e:
            logger.error(f"Ошибка создания PosterData для {url}: {e}")
            raise ContentError(f"Ошибка создания PosterData: {e}")
    
    async def _send_to_next_stage(self, poster_data: PosterData, request_id: str, chat_id: Optional[str] = None):
        """Отправка данных на следующий этап обработки"""
        try:
            poster_data_dict = poster_data.to_dict()
            poster_data_dict["request_id"] = request_id
            if chat_id:
                poster_data_dict["chat_id"] = chat_id
            
            await self.mq_manager.publish_message(
                self.enrichment_exchange_name,
                self.geo_enrichment_routing_key,
                poster_data_dict
            )
            
            logger.info(f"[{request_id}] Данные для ID {poster_data.id} отправлены на гео-обогащение")
            
        except Exception as e:
            logger.error(f"[{request_id}] Ошибка отправки данных на следующий этап: {e}")
            raise
    
    async def _send_to_dead_letter(self, original_message: Dict[str, Any], error: str):
        """Отправка сообщения в Dead Letter Queue"""
        try:
            dead_letter_data = {
                **original_message,
                "error": error,
                "failed_at": datetime.now().isoformat(),
                "service": "parser_worker"
            }
            
            await self.mq_manager.publish_message(
                self.parsing_exchange_name,
                self.dead_letter_routing_key,
                dead_letter_data
            )
            
            logger.error(f"Сообщение отправлено в Dead Letter Queue: {error}")
            
        except Exception as e:
            logger.error(f"Ошибка отправки в Dead Letter Queue: {e}")
    
    async def process_message(self, message: IncomingMessage):
        """Обработка входящего сообщения"""
        async with message.process():
            request_id = "unknown"
            url = "unknown"
            
            try:
                # Парсинг сообщения
                msg_body = json.loads(message.body.decode('utf-8'))
                url = msg_body.get("url", "")
                request_id = msg_body.get("request_id", str(uuid.uuid4()))
                chat_id = msg_body.get("chat_id")
                retry_count = msg_body.get("retry_count", 0)
                
                if not url:
                    raise ParseError("URL не указан в сообщении")
                
                logger.info(f"[{request_id}] Начинаем обработку URL: {url} (попытка {retry_count + 1})")
                
                # Получаем парсер
                parser = self._get_parser_for_url(url)
                if not parser:
                    raise ParserNotFoundError(f"Парсер для URL {url} не найден")
                
                # Получаем HTML контент
                html_content = await self._fetch_html_content(url)
                
                # Парсим контент
                parsed_data = await self._parse_content(html_content, parser, url)
                
                # Создаем PosterData
                poster_data = await self._create_poster_data(parsed_data, url)
                
                # Отправляем на следующий этап
                await self._send_to_next_stage(poster_data, request_id, chat_id)
                
                self.processed_count += 1
                logger.info(f"[{request_id}] Успешно обработан URL: {url}")
                
            except (NetworkError, ContentError) as e:
                self.error_count += 1
                
                # Проверяем, можно ли повторить
                if retry_count < self.max_retries:
                    self.retry_count += 1
                    logger.warning(f"[{request_id}] Повторная попытка для {url}: {e}")
                    
                    # Отправляем сообщение обратно в очередь с увеличенным счетчиком
                    retry_message = {
                        "url": url,
                        "request_id": request_id,
                        "chat_id": msg_body.get("chat_id"),
                        "retry_count": retry_count + 1
                    }
                    
                    # Задержка перед повтором
                    await asyncio.sleep(self.retry_delay * (retry_count + 1))
                    
                    await self.mq_manager.publish_message(
                        self.parsing_exchange_name,
                        self.parse_routing_key,
                        retry_message
                    )
                else:
                    # Максимум попыток достигнут
                    logger.error(f"[{request_id}] Максимум попыток достигнут для {url}: {e}")
                    await self._send_to_dead_letter(msg_body, str(e))
                    
            except Exception as e:
                self.error_count += 1
                logger.error(f"[{request_id}] Критическая ошибка при обработке {url}: {e}")
                await self._send_to_dead_letter(msg_body, str(e))
    
    async def start_consuming(self):
        """Запуск потребления сообщений"""
        try:
            await self.mq_manager.consume_messages(self.parse_queue_name, self.process_message)
            logger.info(f"Парсер воркер запущен и слушает очередь {self.parse_queue_name}")
            
            # Бесконечный цикл работы
            while True:
                await asyncio.sleep(60)  # Проверяем статистику каждую минуту
                logger.info(f"Статистика: обработано={self.processed_count}, ошибок={self.error_count}, повторов={self.retry_count}")
                
        except asyncio.CancelledError:
            logger.info("Парсер воркер остановлен")
        except Exception as e:
            logger.error(f"Критическая ошибка в парсер воркере: {e}")
        finally:
            await self.mq_manager.close()
    
    def get_stats(self) -> Dict[str, Any]:
        """Получение статистики воркера"""
        return {
            "service": "parser_worker",
            "processed_count": self.processed_count,
            "error_count": self.error_count,
            "retry_count": self.retry_count,
            "success_rate": self.processed_count / (self.processed_count + self.error_count) if (self.processed_count + self.error_count) > 0 else 0
        }

# Основная функция для запуска воркера
async def main():
    """Основная функция запуска"""
    import os
    
    # Конфигурация
    amqp_url = os.getenv("AMQP_URL", "amqp://guest:guest@localhost:5672/")
    
    # Создаем и запускаем воркер
    worker = ParserWorker(amqp_url)
    
    try:
        await worker.initialize()
        await worker.start_consuming()
    except KeyboardInterrupt:
        logger.info("Получен сигнал остановки")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
    finally:
        logger.info("Парсер воркер завершил работу")

if __name__ == "__main__":
    import re
    
    # Настройка логирования
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    asyncio.run(main())