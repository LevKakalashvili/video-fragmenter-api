import json
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from loguru import logger

from src.domain.errors import ManifestBuildError
from src.infra.s3.repository import S3Repository


@dataclass(slots=True, frozen=True)
class ManifestChunkReference:
    """Минимальная запись о чанке внутри manifest."""

    chunk_index: int
    chunk_key: str


@dataclass(slots=True, frozen=True)
class ManifestBuildRequest:
    """Контракт на сборку и загрузку manifest.json."""

    job_id: UUID
    file_id: UUID
    output_bucket: str
    output_prefix: str
    chunk_seconds: int
    chunk_mode: str
    chunk_keys: tuple[ManifestChunkReference, ...]
    input_bucket: str | None = None
    input_key: str | None = None
    created_at: datetime | None = None


@dataclass(slots=True, frozen=True)
class ManifestBuildResult:
    """Нормализованный результат сборки и загрузки manifest."""

    bucket: str
    manifest_key: str
    chunks_total: int
    manifest_payload: dict
    elapsed_ms: int
    size_bytes: int


class ManifestWriter(ABC):
    """Интерфейс сервиса финализации manifest."""

    @abstractmethod
    async def build_and_upload(self, request: ManifestBuildRequest) -> ManifestBuildResult:
        """Собирает manifest и загружает его в хранилище."""


class S3ManifestService(ManifestWriter):
    """Сервис сборки и загрузки manifest.json в S3/MinIO."""

    def __init__(self, repository: S3Repository):
        self.repository = repository

    async def build_and_upload(self, request: ManifestBuildRequest) -> ManifestBuildResult:
        manifest_key = self._build_manifest_key(output_prefix=request.output_prefix)
        logger.info(
            f"Старт сборки manifest: job_id={request.job_id}, file_id={request.file_id}, "
            f"manifest_key={manifest_key}, chunks_total={len(request.chunk_keys)}"
        )

        started_at = time.perf_counter()
        manifest_payload = self._build_manifest_payload(request=request)

        try:
            payload_bytes = json.dumps(manifest_payload, ensure_ascii=False).encode("utf-8")
            logger.info(
                f"Manifest сериализован: job_id={request.job_id}, file_id={request.file_id}, "
                f"manifest_key={manifest_key}, size_bytes={len(payload_bytes)}"
            )
        except Exception as exc:
            raise ManifestBuildError(
                job_id=str(request.job_id),
                file_id=str(request.file_id),
                manifest_key=manifest_key,
                message="не удалось сериализовать manifest в JSON",
                original_error=exc,
            ) from exc

        try:
            await self.repository.put_object_bytes(
                bucket=request.output_bucket,
                key=manifest_key,
                payload=payload_bytes,
                content_type="application/json",
            )
        except Exception as exc:
            logger.error(
                f"Ошибка загрузки manifest: job_id={request.job_id}, file_id={request.file_id}, "
                f"manifest_key={manifest_key}, error={exc}"
            )
            raise ManifestBuildError(
                job_id=str(request.job_id),
                file_id=str(request.file_id),
                manifest_key=manifest_key,
                message="не удалось загрузить manifest в S3/MinIO",
                original_error=exc,
            ) from exc

        elapsed_ms = int((time.perf_counter() - started_at) * 1000)
        logger.info(
            f"Manifest загружен: job_id={request.job_id}, file_id={request.file_id}, "
            f"manifest_key={manifest_key}, elapsed_ms={elapsed_ms}"
        )
        return ManifestBuildResult(
            bucket=request.output_bucket,
            manifest_key=manifest_key,
            chunks_total=len(request.chunk_keys),
            manifest_payload=manifest_payload,
            elapsed_ms=elapsed_ms,
            size_bytes=len(payload_bytes),
        )

    def _build_manifest_key(self, output_prefix: str) -> str:
        """Централизованно строит ключ manifest-файла."""
        normalized_prefix = output_prefix.rstrip("/")
        return f"{normalized_prefix}/manifest.json"

    def _build_manifest_payload(self, request: ManifestBuildRequest) -> dict:
        """Формирует стабильную структуру manifest как интеграционный контракт."""
        created_at = request.created_at or datetime.now(UTC)
        payload = {
            "job_id": str(request.job_id),
            "file_id": str(request.file_id),
            "chunk_seconds": request.chunk_seconds,
            "chunk_mode": request.chunk_mode,
            "chunks_total": len(request.chunk_keys),
            "created_at": created_at.isoformat(),
            "chunks": [
                {
                    "chunk_index": chunk.chunk_index,
                    "chunk_key": chunk.chunk_key,
                }
                for chunk in request.chunk_keys
            ],
        }
        if request.input_bucket is not None:
            payload["input_bucket"] = request.input_bucket
        if request.input_key is not None:
            payload["input_key"] = request.input_key
        payload["output_bucket"] = request.output_bucket
        payload["output_prefix"] = request.output_prefix
        return payload
