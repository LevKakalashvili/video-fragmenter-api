from abc import ABC, abstractmethod
from uuid import UUID

from src.infra.broker.schemas import KafkaVideoJobInSchema


class JobStateHandler(ABC):
    """Контракт обработчика состояния job для конкретного транспорта/хранилища."""

    @abstractmethod
    async def reset_job_state(self, job_id: UUID, payload: KafkaVideoJobInSchema) -> None:
        """Сбрасывает старое состояние job и инициализирует новое."""
        pass

    @abstractmethod
    async def mark_job_started(self, job_id: UUID, file_id: UUID, chunks_total_estimate: int) -> None:
        """Фиксирует переход job в состояние started."""
        pass

    @abstractmethod
    async def mark_chunk_processing(
        self,
        job_id: UUID,
        file_id: UUID,
        chunk_index: int,
        chunks_total_estimate: int,
    ) -> None:
        """Фиксирует начало обработки конкретного чанка."""
        pass

    @abstractmethod
    async def mark_chunk_uploaded(
        self,
        job_id: UUID,
        file_id: UUID,
        chunk_index: int,
        chunk_key: str,
        chunks_done: int,
        chunks_total_estimate: int,
    ) -> None:
        """Фиксирует успешную загрузку чанка."""
        pass

    @abstractmethod
    async def mark_chunk_failed(
        self,
        job_id: UUID,
        file_id: UUID,
        chunk_index: int,
        error_text: str,
        chunks_done: int,
        chunks_total_estimate: int,
    ) -> None:
        """Фиксирует ошибку обработки чанка."""
        pass

    @abstractmethod
    async def mark_job_completed(self, job_id: UUID, file_id: UUID, chunks_done: int, manifest_key: str) -> None:
        """Фиксирует успешное завершение job."""
        pass

    @abstractmethod
    async def mark_job_failed(
        self,
        job_id: UUID,
        file_id: UUID,
        chunks_done: int,
        chunks_total_estimate: int,
        error_text: str,
    ) -> None:
        """Фиксирует ошибку всей job."""
        pass

    async def is_current_job(self, file_id: UUID, job_id: UUID) -> bool:
        """Опционально проверяет, что обновление относится к актуальной job."""
        return True
