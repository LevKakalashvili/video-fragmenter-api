from datetime import UTC, datetime
from uuid import UUID

from src.infra.broker.producer import KafkaProducer
from src.infra.broker.schemas import (
    KafkaVideoJobFinalEventSchema,
    KafkaVideoJobInSchema,
    KafkaVideoJobProgressEventSchema,
)
from src.services.job_state_handlers.base import JobStateHandler


class KafkaJobStateHandler(JobStateHandler):
    """Публикует состояние job в Kafka progress topic."""

    def __init__(self, producer: KafkaProducer):
        self.producer = producer

    async def reset_job_state(self, job_id: UUID, payload: KafkaVideoJobInSchema) -> None:
        # Kafka не хранит состояние, поэтому reset здесь не нужен.
        return None

    async def mark_job_started(
        self, job_id: UUID, file_id: UUID, chunks_total_estimate: int
    ) -> None:
        # Публикуем только внешний progress event без локального состояния.
        event = KafkaVideoJobProgressEventSchema(
            job_id=job_id,
            file_id=file_id,
            status="started",
            chunk_index=0,
            chunk_key="",
            chunks_done=0,
            chunks_total_estimate=chunks_total_estimate,
            error="",
            ts=datetime.now(UTC),
        )
        await self.producer.publish_progress(event.model_dump(mode="json"), key=str(file_id))

    async def mark_chunk_processing(
        self,
        job_id: UUID,
        file_id: UUID,
        chunk_index: int,
        chunks_total_estimate: int,
    ) -> None:
        # Промежуточный processing-статус чанка в Kafka не публикуем.
        return None

    async def mark_chunk_uploaded(
        self,
        job_id: UUID,
        file_id: UUID,
        chunk_index: int,
        chunk_key: str,
        chunks_done: int,
        chunks_total_estimate: int,
    ) -> None:
        # Для клиентов Kafka важен только детерминированный факт upload чанка.
        event = KafkaVideoJobProgressEventSchema(
            job_id=job_id,
            file_id=file_id,
            status="chunk_uploaded",
            chunk_index=chunk_index,
            chunk_key=chunk_key,
            chunks_done=chunks_done,
            chunks_total_estimate=chunks_total_estimate,
            error="",
            ts=datetime.now(UTC),
        )
        await self.producer.publish_progress(event.model_dump(mode="json"), key=str(file_id))

    async def mark_chunk_failed(
        self,
        job_id: UUID,
        file_id: UUID,
        chunk_index: int,
        error_text: str,
        chunks_done: int,
        chunks_total_estimate: int,
    ) -> None:
        # Ошибку чанка агрегируем на уровень job_failed, чтобы не дублировать события.
        return None

    async def mark_job_completed(
        self, job_id: UUID, file_id: UUID, chunks_done: int, manifest_key: str
    ) -> None:
        # Финальное completed-событие отправляется отдельной схемой.
        event = KafkaVideoJobFinalEventSchema(
            job_id=job_id,
            file_id=file_id,
            status="completed",
            chunks_total=chunks_done,
            manifest_key=manifest_key,
            error="",
            ts=datetime.now(UTC),
        )
        await self.producer.publish_progress(event.model_dump(mode="json"), key=str(file_id))

    async def mark_job_failed(
        self,
        job_id: UUID,
        file_id: UUID,
        chunks_done: int,
        chunks_total_estimate: int,
        error_text: str,
    ) -> None:
        # При провале публикуем и progress failed, и final failed для совместимости потребителей.
        progress_event = KafkaVideoJobProgressEventSchema(
            job_id=job_id,
            file_id=file_id,
            status="failed",
            chunk_index=0,
            chunk_key="",
            chunks_done=chunks_done,
            chunks_total_estimate=chunks_total_estimate,
            error=error_text,
            ts=datetime.now(UTC),
        )
        final_event = KafkaVideoJobFinalEventSchema(
            job_id=job_id,
            file_id=file_id,
            status="failed",
            chunks_total=chunks_done,
            manifest_key="",
            error=error_text,
            ts=datetime.now(UTC),
        )
        await self.producer.publish_progress(
            progress_event.model_dump(mode="json"), key=str(file_id)
        )
        await self.producer.publish_progress(final_event.model_dump(mode="json"), key=str(file_id))
