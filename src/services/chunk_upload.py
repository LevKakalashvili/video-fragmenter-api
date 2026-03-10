import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path

from loguru import logger

from src.domain.errors import ChunkUploadError
from src.services.s3_storage import S3StorageService


@dataclass(slots=True, frozen=True)
class ChunkUploadRequest:
    """Контракт на загрузку одного готового локального чанка."""

    chunk_index: int
    local_path: Path
    output_bucket: str
    output_prefix: str
    cleanup_local_file: bool = True


@dataclass(slots=True, frozen=True)
class ChunkUploadResult:
    """Нормализованный результат загрузки одного чанка."""

    chunk_index: int
    bucket: str
    chunk_key: str
    file_size_bytes: int
    elapsed_ms: int


class ChunkUploader(ABC):
    """Интерфейс сервиса загрузки одного чанка."""

    @abstractmethod
    async def upload(self, request: ChunkUploadRequest) -> ChunkUploadResult:
        """Загружает один чанк и возвращает нормализованный результат."""


class S3ChunkUploadService(ChunkUploader):
    """Сервис загрузки одного чанка в S3/MinIO с локальным cleanup."""

    def __init__(self, s3_storage: S3StorageService):
        self.s3_storage = s3_storage

    async def upload(self, request: ChunkUploadRequest) -> ChunkUploadResult:
        chunk_key = self._build_chunk_key(
            output_prefix=request.output_prefix, chunk_index=request.chunk_index
        )
        file_size_bytes = self._get_file_size(request=request, chunk_key=chunk_key)

        logger.info(
            f"Старт загрузки чанка: chunk_index={request.chunk_index}, "
            f"bucket={request.output_bucket}, chunk_key={chunk_key}, "
            f"local_path={request.local_path}"
        )
        started_at = time.perf_counter()
        try:
            await self.s3_storage.upload_file(
                bucket=request.output_bucket,
                object_key=chunk_key,
                local_path=request.local_path,
            )
        except Exception as exc:
            logger.error(
                f"Ошибка загрузки чанка: chunk_index={request.chunk_index}, "
                f"bucket={request.output_bucket}, chunk_key={chunk_key}, error={exc}"
            )
            raise ChunkUploadError(
                chunk_index=request.chunk_index,
                local_path=str(request.local_path),
                bucket=request.output_bucket,
                chunk_key=chunk_key,
                message="не удалось загрузить локальный файл чанка в S3/MinIO",
                original_error=exc,
            ) from exc

        elapsed_ms = int((time.perf_counter() - started_at) * 1000)

        # Локальный файл удаляем только после успешной загрузки.
        if request.cleanup_local_file:
            self._cleanup_local_file(request=request, chunk_key=chunk_key)

        logger.info(
            f"Чанк загружен: chunk_index={request.chunk_index}, bucket={request.output_bucket}, "
            f"chunk_key={chunk_key}, elapsed_ms={elapsed_ms}, file_size_bytes={file_size_bytes}"
        )
        return ChunkUploadResult(
            chunk_index=request.chunk_index,
            bucket=request.output_bucket,
            chunk_key=chunk_key,
            file_size_bytes=file_size_bytes,
            elapsed_ms=elapsed_ms,
        )

    def _build_chunk_key(self, output_prefix: str, chunk_index: int) -> str:
        """Централизованно строит ключ чанка в S3/MinIO."""
        normalized_prefix = output_prefix.rstrip("/")
        return f"{normalized_prefix}/chunk_{chunk_index:06d}.mp4"

    def _get_file_size(self, request: ChunkUploadRequest, chunk_key: str) -> int:
        """Проверяет наличие локального файла и возвращает его размер."""
        if not request.local_path.exists():
            raise ChunkUploadError(
                chunk_index=request.chunk_index,
                local_path=str(request.local_path),
                bucket=request.output_bucket,
                chunk_key=chunk_key,
                message="локальный файл чанка не найден перед загрузкой",
            )
        return request.local_path.stat().st_size

    def _cleanup_local_file(self, request: ChunkUploadRequest, chunk_key: str) -> None:
        """Удаляет локальный файл чанка после успешной загрузки."""
        try:
            request.local_path.unlink(missing_ok=True)
            logger.info(
                f"Локальный файл чанка удален: chunk_index={request.chunk_index}, "
                f"local_path={request.local_path}"
            )
        except Exception as exc:
            logger.warning(
                f"Не удалось удалить локальный файл чанка: chunk_index={request.chunk_index}, "
                f"local_path={request.local_path}, error={exc}"
            )
            raise ChunkUploadError(
                chunk_index=request.chunk_index,
                local_path=str(request.local_path),
                bucket=request.output_bucket,
                chunk_key=chunk_key,
                message="загрузка завершилась успешно, но cleanup локального файла не удался",
                original_error=exc,
            ) from exc
