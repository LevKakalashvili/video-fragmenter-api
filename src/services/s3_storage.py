import json
from pathlib import Path

from src.core.settings import settings
from src.domain.errors import S3FileTooLargeError
from src.infra.s3.repository import S3ObjectMeta, S3Repository


class S3StorageService:
    """Сервис S3/MinIO с правилами pipeline для видео-фрагментации."""

    def __init__(self, repository: S3Repository):
        self.repository = repository

    async def get_file_head(self, bucket: str, key: str) -> S3ObjectMeta:
        """Читает метаданные входного видео через S3 HEAD."""
        return await self.repository.head_object(bucket=bucket, key=key)

    async def validate_input_size(
        self,
        bucket: str,
        key: str,
        max_bytes: int | None = None,
    ) -> S3ObjectMeta:
        """Проверяет, что входной файл не превышает лимит."""
        meta = await self.get_file_head(bucket=bucket, key=key)
        allowed_size = max_bytes if max_bytes is not None else settings.app.max_file_bytes
        if meta.size_bytes > allowed_size:
            raise S3FileTooLargeError(
                bucket=bucket,
                key=key,
                actual_size=meta.size_bytes,
                max_size=allowed_size,
            )
        return meta

    async def download_file(self, input_bucket: str, input_key: str, destination: Path) -> Path:
        """Скачивает входной файл в указанный локальный путь."""
        return await self.repository.download_file(
            bucket=input_bucket,
            key=input_key,
            destination=destination,
        )

    async def upload_file(
        self,
        bucket: str,
        object_key: str,
        local_path: Path,
    ) -> None:
        """Загружает файл в S3/MinIO по заданному ключу."""
        await self.repository.upload_file(
            bucket=bucket,
            key=object_key,
            source=local_path,
            content_type="video/mp4",
        )

    async def upload_manifest(
        self,
        bucket: str,
        object_key: str,
        data: dict,
    ) -> None:
        """Сериализует и загружает data (dict -> json file)."""
        payload = json.dumps(data, ensure_ascii=False).encode("utf-8")
        await self.repository.put_object_bytes(
            bucket=bucket,
            key=object_key,
            payload=payload,
            content_type="application/json",
        )
