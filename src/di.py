from dishka import Provider, Scope, make_async_container, provide

from src.core.settings import settings
from src.infra.broker.consumer import KafkaConsumer
from src.infra.broker.kafka import kafka_consumer, kafka_producer
from src.infra.broker.producer import KafkaProducer
from src.infra.s3.repository import S3Repository
from src.services.chunking.registry import ChunkingStrategyRegistry
from src.services.chunking.resolver import ChunkingStrategyResolver
from src.services.chunking.strategies.fast_keyframe_aligned import FastKeyframeAlignedChunkingStrategy
from src.services.in_memory_scheduler import InMemoryVideoScheduler
from src.services.s3_storage import S3StorageService
from src.services.video_job import VideoJobService


class KafkaProvider(Provider):
    # Продюсер Kafka создается один раз на все приложение.
    @provide(scope=Scope.APP)
    def get_producer(self) -> KafkaProducer:
        return kafka_producer

    # Консьюмер Kafka также создается один раз на все приложение.
    @provide(scope=Scope.APP)
    def get_consumer(self) -> KafkaConsumer:
        return kafka_consumer

    @provide(scope=Scope.APP)
    def get_chunking_strategy_registry(self) -> ChunkingStrategyRegistry:
        registry = ChunkingStrategyRegistry()
        registry.register(FastKeyframeAlignedChunkingStrategy())
        return registry

    @provide(scope=Scope.APP)
    def get_in_memory_video_scheduler(self) -> InMemoryVideoScheduler:
        return InMemoryVideoScheduler(max_videos_in_progress=settings.app.max_videos_in_progress)


def get_service_provider() -> Provider:
    # Основной провайдер сервисов на уровень запроса/сообщения.
    provider = Provider(scope=Scope.REQUEST)
    # S3-репозиторий держим на уровне приложения, чтобы переиспользовать клиентские ресурсы.
    provider.provide(S3Repository, scope=Scope.APP)
    provider.provide(ChunkingStrategyResolver, scope=Scope.APP)
    # Сервисы оркестрации создаются на каждый запрос/сообщение.
    provider.provide(S3StorageService)
    provider.provide(VideoJobService)
    return provider


# Корневой DI-контейнер приложения.
container = make_async_container(
    KafkaProvider(),
    get_service_provider(),
)
