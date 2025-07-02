import asyncio
import json
from typing import Dict, Any

import aio_pika
from aio_pika.abc import IncomingMessage

from message_queue_manager import MessageQueueManager


class NotificationService:
    async def send_notification(self, notification_data: Dict[str, Any]):
        request_id = notification_data.get("request_id", "N/A")
        ad_id = notification_data.get("ad_id", "N/A")
        attractiveness = notification_data.get("investment_attractiveness", "Неизвестно")
        yield_estimate = notification_data.get("estimated_rent_yield", "N/A")
        original_url = notification_data.get("original_ad_url", "N/A")
        chat_id = notification_data.get("chat_id", "N/A") # <- Извлекаем chat_id

        print(f"\n--- Уведомление для запроса [{request_id}] (chat_id: {chat_id}) ---")
        print(f"  Новое объявление ID: {ad_id}")
        print(f"  URL: {original_url}")
        print(f"  Инвестиционная привлекательность: {attractiveness}")
        print(f"  Ожидаемая доходность: {yield_estimate}")
        print("-----------------------------------------\n")


class NotificationWorker:
    def __init__(self, amqp_url: str, notification_queue_name: str = "notification_queue"):
        self.mq_manager = MessageQueueManager(amqp_url)
        self.notification_queue_name = notification_queue_name
        self.notification_exchange_name = "notification_exchange"
        
        self.notification_service = NotificationService()

    async def initialize(self):
        await self.mq_manager.connect()
        await self.mq_manager.declare_queue(self.notification_queue_name)
        await self.mq_manager.declare_exchange(self.notification_exchange_name, type=aio_pika.ExchangeType.TOPIC)
        await self.mq_manager.bind_queue_to_exchange(
            self.notification_queue_name, self.notification_exchange_name, "notify.user"
        )
        print(f"Воркер уведомлений готов к работе. Слушает '{self.notification_queue_name}'.")

    async def process_message(self, message: IncomingMessage):
        async with message.process():
            try:
                msg_body = json.loads(message.body.decode('utf-8'))
                request_id = msg_body.get("request_id", "N/A")
                ad_id = msg_body.get("ad_id", "N/A")
                chat_id = msg_body.get("chat_id") # <- Извлекаем chat_id

                print(f"[{request_id}] [Notification] Получены результаты анализа для ID: {ad_id} (chat_id: {chat_id})")
                
                await self.notification_service.send_notification(msg_body)

            except json.JSONDecodeError:
                print(f"[{request_id}] [Notification] Получено некорректное JSON сообщение: {message.body.decode('utf-8')}")
            except Exception as e:
                print(f"[{request_id}] [Notification] Неизвестная ошибка: {e}")

    async def start_consuming(self):
        await self.mq_manager.consume_messages(self.notification_queue_name, self.process_message)
        print(f"Воркер уведомлений слушает очередь '{self.notification_queue_name}'...")
        try:
            while True:
                await asyncio.sleep(3600) 
        except asyncio.CancelledError:
            print("Воркер уведомлений остановлен.")
        except KeyboardInterrupt:
            print("Воркер уведомлений остановлен (KeyboardInterrupt).")
        finally:
            await self.mq_manager.close()


async def main_notification_worker():
    amqp_url = "amqp://guest:guest@localhost:5672/"
    notification_worker = NotificationWorker(amqp_url)
    await notification_worker.initialize()
    await notification_worker.start_consuming()

if __name__ == "__main__":
    print("Запуск NotificationWorker. Убедитесь, что все предыдущие сервисы запущены.")
    asyncio.run(main_notification_worker())
