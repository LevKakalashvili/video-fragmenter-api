from uuid import UUID

from redis.asyncio import Redis

from src.infra.broker.producer import KafkaProducer
from src.infra.broker.schemas import KafkaVideoJobInSchema
from src.services.job_state_handlers import JobStateHandler, KafkaJobStateHandler, RedisJobStateHandler


class JobStateService:
    """Единая точка публикации состояния job через зарегистрированные handler-ы."""

    def __init__(self, producer: KafkaProducer, redis: Redis):
        self.redis_handler = RedisJobStateHandler(redis=redis)
        self.kafka_handler = KafkaJobStateHandler(producer=producer)
        self.handlers: list[JobStateHandler] = [
            self.redis_handler,
            self.kafka_handler,
        ]

    async def reset_job_state(self, job_id: UUID, payload: KafkaVideoJobInSchema) -> None:
        for handler in self.handlers:
            await handler.reset_job_state(job_id=job_id, payload=payload)

    async def is_current_job(self, file_id: UUID, job_id: UUID) -> bool:
        for handler in self.handlers:
            if not await handler.is_current_job(file_id=file_id, job_id=job_id):
                return False
        return True

    async def mark_job_started(self, job_id: UUID, file_id: UUID, chunks_total_estimate: int) -> None:
        if not await self.is_current_job(file_id=file_id, job_id=job_id):
            return
        for handler in self.handlers:
            await handler.mark_job_started(
                job_id=job_id,
                file_id=file_id,
                chunks_total_estimate=chunks_total_estimate,
            )

    async def mark_chunk_processing(
        self,
        job_id: UUID,
        file_id: UUID,
        chunk_index: int,
        chunks_total_estimate: int,
    ) -> None:
        if not await self.is_current_job(file_id=file_id, job_id=job_id):
            return
        for handler in self.handlers:
            await handler.mark_chunk_processing(
                job_id=job_id,
                file_id=file_id,
                chunk_index=chunk_index,
                chunks_total_estimate=chunks_total_estimate,
            )

    async def mark_chunk_uploaded(
        self,
        job_id: UUID,
        file_id: UUID,
        chunk_index: int,
        chunk_key: str,
        chunks_done: int,
        chunks_total_estimate: int,
    ) -> None:
        if not await self.is_current_job(file_id=file_id, job_id=job_id):
            return
        for handler in self.handlers:
            await handler.mark_chunk_uploaded(
                job_id=job_id,
                file_id=file_id,
                chunk_index=chunk_index,
                chunk_key=chunk_key,
                chunks_done=chunks_done,
                chunks_total_estimate=chunks_total_estimate,
            )

    async def mark_chunk_failed(
        self,
        job_id: UUID,
        file_id: UUID,
        chunk_index: int,
        error_text: str,
        chunks_done: int,
        chunks_total_estimate: int,
    ) -> None:
        if not await self.is_current_job(file_id=file_id, job_id=job_id):
            return
        for handler in self.handlers:
            await handler.mark_chunk_failed(
                job_id=job_id,
                file_id=file_id,
                chunk_index=chunk_index,
                error_text=error_text,
                chunks_done=chunks_done,
                chunks_total_estimate=chunks_total_estimate,
            )

    async def mark_job_completed(self, job_id: UUID, file_id: UUID, chunks_done: int, manifest_key: str) -> None:
        if not await self.is_current_job(file_id=file_id, job_id=job_id):
            return
        for handler in self.handlers:
            await handler.mark_job_completed(
                job_id=job_id,
                file_id=file_id,
                chunks_done=chunks_done,
                manifest_key=manifest_key,
            )
        # Active-индекс удаляем только после успешной отправки финального статуса во все handler-ы.
        await self.redis_handler.clear_active_job(file_id=file_id, job_id=job_id)

    async def mark_job_failed(
        self,
        job_id: UUID,
        file_id: UUID,
        chunks_done: int,
        chunks_total_estimate: int,
        error_text: str,
    ) -> None:
        for handler in self.handlers:
            await handler.mark_job_failed(
                job_id=job_id,
                file_id=file_id,
                chunks_done=chunks_done,
                chunks_total_estimate=chunks_total_estimate,
                error_text=error_text,
            )
        # Для failed-job active-индекс также убираем только после финального Kafka failed-сообщения.
        await self.redis_handler.clear_active_job(file_id=file_id, job_id=job_id)
