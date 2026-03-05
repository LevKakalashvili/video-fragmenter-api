from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import aioboto3
from botocore.config import Config
from botocore.exceptions import ClientError, ConnectTimeoutError, EndpointConnectionError, ReadTimeoutError

from src.core.settings import settings
from src.domain.errors import DomainError, S3ObjectNotFoundError


@dataclass(slots=True, frozen=True)
class S3ObjectMeta:
    """Метаданные объекта из S3/MinIO."""

    bucket: str
    key: str
    size_bytes: int
    etag: str | None
    content_type: str | None
    last_modified: datetime | None


class S3Repository:
    """Низкоуровневый репозиторий для операций с S3/MinIO."""

    def __init__(self):
        self._session = aioboto3.Session()
        self._client_kwargs = {
            "service_name": "s3",
            "endpoint_url": settings.minio.url,
            "aws_access_key_id": settings.minio.root_user,
            "aws_secret_access_key": settings.minio.root_password,
            "region_name": settings.minio.region_name,
            "use_ssl": settings.minio.secure,
            "config": Config(
                s3={"addressing_style": settings.minio.addressing_style},
                connect_timeout=settings.minio.connect_timeout_seconds,
                read_timeout=settings.minio.read_timeout_seconds,
            ),
        }

    async def head_object(self, bucket: str, key: str) -> S3ObjectMeta:
        try:
            async with self._session.client(**self._client_kwargs) as client:
                data = await client.head_object(Bucket=bucket, Key=key)
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            if code in {"NoSuchKey", "404", "NotFound"}:
                raise S3ObjectNotFoundError(bucket=bucket, key=key) from exc
            raise DomainError(f"Ошибка S3 при HEAD s3://{bucket}/{key}. Код={code}. Детали: {exc}") from exc
        except (EndpointConnectionError, ConnectTimeoutError, ReadTimeoutError, TimeoutError) as exc:
            raise DomainError(
                "Таймаут/ошибка соединения при HEAD "
                f"s3://{bucket}/{key}. connect_timeout={settings.minio.connect_timeout_seconds}s, "
                f"read_timeout={settings.minio.read_timeout_seconds}s. Детали: {exc}",
            ) from exc
        except Exception as exc:
            raise DomainError(f"Ошибка получения метаданных объекта {bucket}/{key}. Детали: {exc}") from exc

        return S3ObjectMeta(
            bucket=bucket,
            key=key,
            size_bytes=int(data.get("ContentLength", 0)),
            etag=data.get("ETag"),
            content_type=data.get("ContentType"),
            last_modified=data.get("LastModified"),
        )

    async def download_file(self, bucket: str, key: str, destination: Path) -> Path:
        destination.parent.mkdir(parents=True, exist_ok=True)
        try:
            async with self._session.client(**self._client_kwargs) as client:
                await client.download_file(Bucket=bucket, Key=key, Filename=str(destination))
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            if code in {"NoSuchKey", "404", "NotFound"}:
                raise S3ObjectNotFoundError(bucket=bucket, key=key) from exc
            raise DomainError(f"Ошибка S3 при скачивании s3://{bucket}/{key}. Код={code}. Детали: {exc}") from exc
        except (EndpointConnectionError, ConnectTimeoutError, ReadTimeoutError, TimeoutError) as exc:
            raise DomainError(
                "Таймаут/ошибка соединения при скачивании "
                f"s3://{bucket}/{key}. connect_timeout={settings.minio.connect_timeout_seconds}s, "
                f"read_timeout={settings.minio.read_timeout_seconds}s. Детали: {exc}",
            ) from exc
        return destination

    async def upload_file(
        self,
        bucket: str,
        key: str,
        source: Path,
        content_type: str | None = None,
        extra_args: dict[str, Any] | None = None,
    ) -> None:
        upload_extra_args = dict(extra_args or {})
        if content_type:
            upload_extra_args["ContentType"] = content_type

        async with self._session.client(**self._client_kwargs) as client:
            await client.upload_file(
                Filename=str(source),
                Bucket=bucket,
                Key=key,
                ExtraArgs=upload_extra_args or None,
            )

    async def put_object_bytes(
        self,
        bucket: str,
        key: str,
        payload: bytes,
        content_type: str = "application/octet-stream",
    ) -> None:
        async with self._session.client(**self._client_kwargs) as client:
            await client.put_object(
                Bucket=bucket,
                Key=key,
                Body=payload,
                ContentType=content_type,
            )

    async def object_exists(self, bucket: str, key: str) -> bool:
        try:
            await self.head_object(bucket=bucket, key=key)
        except S3ObjectNotFoundError:
            return False
        return True

    async def ensure_bucket_available(self, bucket: str | None = None) -> None:
        target_bucket = bucket or settings.minio.bucket
        try:
            async with self._session.client(**self._client_kwargs) as client:
                await client.head_bucket(Bucket=target_bucket)
        except Exception as exc:
            raise DomainError(f"Недоступен bucket s3://{target_bucket}. Детали: {exc}") from exc

    async def is_bucket_available(self, bucket: str | None = None) -> bool:
        try:
            await self.ensure_bucket_available(bucket=bucket)
        except DomainError:
            return False
        return True
