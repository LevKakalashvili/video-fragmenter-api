from faststream.kafka import KafkaBroker

from src.core.settings import settings


class KafkaProducer:
    def __init__(self, broker: KafkaBroker):
        self.broker = broker

    async def start(self) -> None:
        # Продюсер использует общее подключение брокера.
        return None

    async def stop(self) -> None:
        return None

    async def publish(self, message: dict, topic: str) -> None:
        await self.broker.publish(message, topic=topic)

    async def publish_progress(self, message: dict) -> None:
        await self.publish(message=message, topic=settings.kafka.topic_progress)
