from datetime import UTC, datetime
import shutil
import tempfile
from pathlib import Path
from uuid import UUID

from src.infra.broker.schemas import KafkaVideoJobFinalEventSchema, KafkaVideoJobProgressEventSchema
from src.infra.broker.producer import KafkaProducer
from src.infra.s3.repository import S3ObjectMeta
from src.services.s3_storage import S3StorageService


class VideoJobService:
    def __init__(self, producer: KafkaProducer, s3_storage: S3StorageService):
        self.producer = producer
        self.s3_storage = s3_storage
        self.tmp_root = Path(tempfile.gettempdir())

    async def publish_progress_event(self, event: KafkaVideoJobProgressEventSchema) -> None:
        await self.producer.publish_progress(event.model_dump(mode="json"))

    async def publish_final_event(self, event: KafkaVideoJobFinalEventSchema) -> None:
        await self.producer.publish_progress(event.model_dump(mode="json"))

    async def publish_started_event(self, job_id: UUID, file_id: UUID) -> None:
        event = KafkaVideoJobProgressEventSchema(
            job_id=job_id,
            file_id=file_id,
            status="started",
            chunk_index=0,
            chunk_key="",
            chunks_done=0,
            chunks_total_estimate=0,
            ts=datetime.now(UTC),
        )
        await self.publish_progress_event(event)

    async def validate_input_size(self, bucket: str, key: str, max_bytes: int | None = None) -> S3ObjectMeta:
        return await self.s3_storage.validate_input_size(bucket=bucket, key=key, max_bytes=max_bytes)

    async def download_input_video(self, job_id: UUID, input_bucket: str, input_key: str) -> Path:
        destination = self.build_local_video_path(job_id=job_id, input_key=input_key)
        return await self.s3_storage.download_file(
            input_bucket=input_bucket,
            input_key=input_key,
            destination=destination,
        )

    async def upload_chunk(self, output_bucket: str, output_prefix: str, chunk_index: int, local_path: Path) -> str:
        chunk_key = self.build_chunk_key(output_prefix=output_prefix, chunk_index=chunk_index)
        await self.s3_storage.upload_file(output_bucket=output_bucket, object_key=chunk_key, local_chunk_path=local_path)
        return chunk_key

    async def upload_manifest(self, output_bucket: str, output_prefix: str, manifest: dict) -> str:
        manifest_key = self.build_manifest_key(output_prefix=output_prefix)
        await self.s3_storage.upload_manifest(output_bucket=output_bucket, manifest_key=manifest_key, manifest=manifest)
        return manifest_key

    def build_job_tmp_dir(self, job_id: UUID) -> Path:
        """Возвращает рабочую временную директорию задачи."""
        return self.tmp_root / str(job_id)

    def build_local_video_path(self, job_id: UUID, input_key: str) -> Path:
        """Строит локальный путь для входного видео в директории задачи."""
        job_dir = self.build_job_tmp_dir(job_id=job_id)
        job_dir.mkdir(parents=True, exist_ok=True)
        return job_dir / Path(input_key).name

    def build_chunk_key(self, output_prefix: str, chunk_index: int) -> str:
        """Строит ключ чанка в формате `chunk_000001.mp4`."""
        normalized_prefix = output_prefix.rstrip("/")
        return f"{normalized_prefix}/chunk_{chunk_index:06d}.mp4"

    def build_manifest_key(self, output_prefix: str) -> str:
        """Строит ключ manifest-файла для задачи."""
        normalized_prefix = output_prefix.rstrip("/")
        return f"{normalized_prefix}/manifest.json"

    def remove_local_file(self, file_path: Path) -> None:
        """Удаляет локальный файл, если он существует."""
        file_path.unlink(missing_ok=True)

    def cleanup_job_tmp_dir(self, job_id: UUID) -> None:
        """Удаляет временную директорию задачи целиком."""
        shutil.rmtree(self.build_job_tmp_dir(job_id=job_id), ignore_errors=True)
