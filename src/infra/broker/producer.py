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

    async def publish(self, message: dict, topic: str, key: str | bytes | None = None) -> None:
        key_bytes: bytes | None
        if key is None:
            key_bytes = None
        elif isinstance(key, bytes):
            key_bytes = key
        else:
            key_bytes = key.encode("utf-8")
        await self.broker.publish(message, topic=topic, key=key_bytes)

    async def publish_progress(self, message: dict, key: str | bytes | None = None) -> None:
        await self.publish(message=message, topic=settings.kafka.topic_progress, key=key)
