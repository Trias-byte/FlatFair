import asyncio
import json
from typing import Dict, Any, Optional, Callable
import aio_pika
from aio_pika import connect_robust, Message, ExchangeType, Channel, Connection, Queue
from aio_pika.abc import AbstractRobustConnection, AbstractChannel, AbstractQueue, AbstractExchange, IncomingMessage

import logging


class MessageQueueManager:
    """
    Класс для управления подключением и взаимодействием с RabbitMQ.
    Предоставляет методы для подключения, объявления exchanges/очередей,
    публикации и потребления сообщений.
    """
    def __init__(self, amqp_url: str):
        self.amqp_url = amqp_url
        self._connection: Optional[AbstractRobustConnection] = None
        self._channel: Optional[AbstractChannel] = None
        self._exchanges: Dict[str, AbstractExchange] = {}
        self._queues: Dict[str, AbstractQueue] = {}

    async def connect(self):
        """Устанавливает или восстанавливает соединение с RabbitMQ."""
        if not self._connection or self._connection.is_closed:
            try:
                self._connection = await connect_robust(self.amqp_url)
                self._channel = await self._connection.channel()
                print("Успешно подключились к RabbitMQ.")
            except aio_pika.exceptions.AMQPConnectionError as e:
                print(f"Ошибка подключения к RabbitMQ: {e}")
                raise

    async def declare_exchange(self, name: str, type: ExchangeType = ExchangeType.DIRECT, durable: bool = True):
        """Объявляет exchange."""
        if name not in self._exchanges:
            exchange = await self._channel.declare_exchange(name, type, durable=durable)
            self._exchanges[name] = exchange
            print(f"Объявлен exchange: {name} (тип: {type.value})")
        return self._exchanges[name]

    async def declare_queue(self, name: str, durable: bool = True):
        """Объявляет очередь."""
        if name not in self._queues:
            queue = await self._channel.declare_queue(name, durable=durable)
            self._queues[name] = queue
            print(f"Объявлена очередь: {name}") # TODO log
        return self._queues[name]

    async def bind_queue_to_exchange(self, queue_name: str, exchange_name: str, routing_key: str):
        """Привязывает очередь к exchange с заданным ключом маршрутизации."""
        queue = self._queues.get(queue_name)
        exchange = self._exchanges.get(exchange_name)
        if queue and exchange:
            await queue.bind(exchange, routing_key)
            print(f"Очередь {queue_name} привязана к exchange {exchange_name} с ключом {routing_key}")
        else:
            print(f"Ошибка привязки: очередь {queue_name} или exchange {exchange_name} не найден. Проверьте, что они объявлены.")

    async def publish_message(self, exchange_name: str, routing_key: str, message_body: Dict[str, Any]):
        """Публикует сообщение в exchange."""
        if exchange_name not in self._exchanges:
            raise ValueError(f"Exchange '{exchange_name}' не объявлен. Пожалуйста, вызовите declare_exchange() сначала.")
        
        exchange = self._exchanges[exchange_name]
        
        message = Message(
            body=json.dumps(message_body, ensure_ascii=False).encode('utf-8'),
            content_type='application/json',
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT
        )
        await exchange.publish(message, routing_key=routing_key)

    async def consume_messages(self, queue_name: str, callback: Callable[[IncomingMessage], Any]):
        """Начинает потребление сообщений из очереди, вызывая callback для каждого сообщения."""
        if queue_name not in self._queues:
            raise ValueError(f"Очередь '{queue_name}' не объявлена. Пожалуйста, вызовите declare_queue() сначала.")
        
        queue = self._queues[queue_name]
        print(f"Начинаем потребление сообщений из очереди '{queue_name}'...")
        await queue.consume(callback)

    async def close(self):
        """Закрывает соединение с RabbitMQ."""
        if self._channel:
            await self._channel.close()
            self._channel = None
        if self._connection:
            await self._connection.close()
            self._connection = None
        print("Соединение с RabbitMQ закрыто.")