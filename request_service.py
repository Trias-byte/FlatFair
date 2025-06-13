import asyncio
from typing import Dict, Any, Optional

from message_queue_manager import MessageQueueManager

class RequestService:
    """
    Принимает запросы от пользователя (URL) и отправляет их в очередь парсинга.
    Представляет собой "Входной шлюз" и "Сервис запросов".
    """
    def __init__(self, amqp_url: str, parse_queue_name: str = "parse_queue"):
        self.mq_manager = MessageQueueManager(amqp_url)
        self.parse_queue_name = parse_queue_name
        self.parse_exchange_name = "parsing_exchange" # Exchange для отправки запросов на парсинг
        self.parse_routing_key = "parse_ad" # Ключ маршрутизации для запросов на парсинг

    async def initialize(self):
        """Инициализирует сервис: подключается к MQ, объявляет exchange и очередь."""
        await self.mq_manager.connect()
        await self.mq_manager.declare_exchange(self.parse_exchange_name)
        await self.mq_manager.declare_queue(self.parse_queue_name)
        await self.mq_manager.bind_queue_to_exchange(
            self.parse_queue_name, self.parse_exchange_name, self.parse_routing_key
        )
        print("RequestService успешно инициализирован.")

    async def process_user_request(self, url: str) -> Dict[str, Any]:
        """
        Принимает URL от пользователя и отправляет его в очередь для парсинга.
        """
        try:
            # Генерация простого request_id для сквозной трассировки
            request_id = f"req_{hash(url)}_{asyncio.current_task()._coro.__name__}" 
            message_body = {
                "url": url,
                "request_id": request_id 
            }
            await self.mq_manager.publish_message(
                self.parse_exchange_name, self.parse_routing_key, message_body
            )
            print(f"[{request_id}] Запрос на парсинг URL '{url}' отправлен в очередь '{self.parse_queue_name}'.")
            return {"status": "success", "message": "Запрос на парсинг отправлен.", "request_id": request_id}
        except Exception as e:
            print(f"Ошибка при отправке запроса в очередь: {e}")
            return {"status": "error", "message": f"Ошибка: {e}"}

    async def close(self):
        """Закрывает соединение с RabbitMQ."""
        await self.mq_manager.close()

# --- Пример использования (как это могло бы быть в API) ---
async def main_request_service():
    amqp_url = "amqp://guest:guest@localhost:5672/" # Убедитесь, что это ваш RabbitMQ URL
    request_service = RequestService(amqp_url)
    await request_service.initialize()

    print("\nОтправка тестовых запросов на парсинг...")
    urls_to_process = [
        "https://spb.cian.ru/rent/flat/305548024/",
        "https://spb.cian.ru/rent/flat/290374661/", 
        "https://spb.cian.ru/rent/flat/287042578/"
    ]

    tasks = [request_service.process_user_request(url) for url in urls_to_process]
    results = await asyncio.gather(*tasks)

    print("\n--- Результаты отправки запросов ---")
    for url, result in zip(urls_to_process, results):
        print(f"  URL: {url}")
        print(f"  Result: {result}")
        print("-" * 30)

    await request_service.close()

if __name__ == "__main__":
    # Для запуска:
    # Убедитесь, что RabbitMQ запущен. Например, через Docker:
    # docker run -d --hostname my-rabbit --name some-rabbit -p 5672:5672 -p 15672:15672 rabbitmq:3-management
    print("Запуск RequestService. Убедитесь, что RabbitMQ запущен.")
    asyncio.run(main_request_service())