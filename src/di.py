from dishka import Provider, Scope, make_async_container, provide
from redis.asyncio import Redis

from src.core.settings import settings
from src.infra.broker.consumer import KafkaConsumer
from src.infra.broker.kafka import kafka_consumer, kafka_producer
from src.infra.broker.producer import KafkaProducer
from src.infra.redis.client import redis_client
from src.infra.s3.repository import S3Repository
from src.services.chunk_execution import FfmpegChunkExecutionService
from src.services.chunk_upload import S3ChunkUploadService
from src.services.chunking.registry import ChunkingStrategyRegistry
from src.services.chunking.resolver import ChunkingStrategyResolver
from src.services.chunking.strategies.fast_keyframe_aligned import (
    FastKeyframeAlignedChunkingStrategy,
)
from src.services.in_memory_scheduler import InMemoryVideoScheduler
from src.services.job_state import JobStateService
from src.services.manifest import S3ManifestService
from src.services.s3_storage import S3StorageService
from src.services.video_job import VideoJobService
from src.services.video_probe import FfprobeVideoProbeService


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
    def get_redis_client(self) -> Redis:
        return redis_client

    @provide(scope=Scope.APP)
    def get_job_state_service(self, producer: KafkaProducer, redis: Redis) -> JobStateService:
        return JobStateService(producer=producer, redis=redis)

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
    provider.provide(FfmpegChunkExecutionService, scope=Scope.APP)
    provider.provide(S3ChunkUploadService, scope=Scope.APP)
    provider.provide(S3ManifestService, scope=Scope.APP)
    provider.provide(FfprobeVideoProbeService, scope=Scope.APP)
    provider.provide(S3StorageService, scope=Scope.APP)
    # Сервисы оркестрации создаются на каждый запрос/сообщение.
    provider.provide(VideoJobService)
    return provider


# Корневой DI-контейнер приложения.
container = make_async_container(
    KafkaProvider(),
    get_service_provider(),
)
