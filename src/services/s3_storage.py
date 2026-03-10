import json
from pathlib import Path

from src.core.settings import settings
from src.domain.errors import S3FileTooLargeError, UnsupportedVideoFormatError
from src.infra.s3.repository import S3ObjectMeta, S3Repository


class S3StorageService:
    """Сервис S3/MinIO с правилами pipeline для видео-фрагментации."""

    SUPPORTED_VIDEO_CONTENT_TYPES: dict[str, tuple[str, ...]] = {
        "mp4": ("video/mp4", "application/mp4"),
    }

    def __init__(self, repository: S3Repository):
        """Инициализирует сервис оберткой над S3-репозиторием."""
        self.repository = repository

    async def get_file_head(self, bucket: str, input_key: str) -> S3ObjectMeta:
        """Читает метаданные входного видео через S3 HEAD."""
        return await self.repository.head_object(bucket=bucket, key=input_key)

    async def validate_input_size(
        self,
        bucket: str,
        input_key: str,
        max_bytes: int | None = None,
    ) -> S3ObjectMeta:
        """Проверяет, что входной файл не превышает лимит."""
        meta = await self.get_file_head(bucket=bucket, input_key=input_key)
        # Лимит из сообщения имеет приоритет над системной настройкой.
        allowed_size = max_bytes if max_bytes is not None else settings.app.max_file_bytes
        if meta.size_bytes > allowed_size:
            raise S3FileTooLargeError(
                bucket=bucket,
                key=input_key,
                actual_size=meta.size_bytes,
                max_size=allowed_size,
            )
        self.validate_file_extension(bucket=bucket, input_key=input_key, meta=meta)
        self.validate_content_type(bucket=bucket, input_key=input_key, meta=meta)
        return meta

    def validate_file_extension(self, bucket: str, input_key: str, meta: S3ObjectMeta) -> None:
        supported_formats = tuple(settings.app.supported_video_formats)
        detected_extension = Path(input_key).suffix.lstrip(".").lower()
        if detected_extension not in supported_formats:
            raise UnsupportedVideoFormatError(
                bucket=bucket,
                key=input_key,
                file_extension=detected_extension or None,
                content_type=meta.content_type,
                supported_formats=supported_formats,
            )

    def validate_content_type(self, bucket: str, input_key: str, meta: S3ObjectMeta) -> None:
        supported_formats = tuple(settings.app.supported_video_formats)
        supported_content_types: set[str] = set()
        unknown_formats: list[str] = []
        for video_format in supported_formats:
            content_types = self.SUPPORTED_VIDEO_CONTENT_TYPES.get(video_format)
            if content_types is None:
                unknown_formats.append(video_format)
                continue
            supported_content_types.update(content_types)

        if unknown_formats:
            unknown = ", ".join(sorted(unknown_formats))
            raise ValueError(f"Неизвестные форматы в APP_SUPPORTED_VIDEO_FORMATS: {unknown}")

        normalized_content_type = (meta.content_type or "").split(";", 1)[0].strip().lower()
        if normalized_content_type not in supported_content_types:
            raise UnsupportedVideoFormatError(
                bucket=bucket,
                key=input_key,
                file_extension=Path(input_key).suffix.lstrip(".").lower() or None,
                content_type=meta.content_type,
                supported_formats=supported_formats,
            )

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
        # Manifest формируется в памяти, без промежуточного временного файла.
        payload = json.dumps(data, ensure_ascii=False).encode("utf-8")
        await self.repository.put_object_bytes(
            bucket=bucket,
            key=object_key,
            payload=payload,
            content_type="application/json",
        )
