import asyncio
import math
import shutil
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

from src.core.settings import settings
from src.infra.broker.producer import KafkaProducer
from src.infra.broker.schemas import KafkaVideoJobFinalEventSchema, KafkaVideoJobProgressEventSchema
from src.infra.s3.repository import S3ObjectMeta
from src.services.chunking.base import ChunkingStrategy
from src.services.chunking.resolver import ChunkingStrategyResolver
from src.services.s3_storage import S3StorageService


class VideoJobService:
    """Сервис оркестрации шагов обработки видео-задачи."""

    def __init__(
        self,
        producer: KafkaProducer,
        s3_storage: S3StorageService,
        chunking_strategy_resolver: ChunkingStrategyResolver,
    ):
        """Инициализирует зависимости сервиса обработки видео."""
        self.producer = producer
        self.s3_storage = s3_storage
        self.chunking_strategy_resolver = chunking_strategy_resolver
        # Все временные артефакты задачи складываются в системный temp-каталог.
        self.tmp_root = Path(tempfile.gettempdir())

    def prepare_local_unix_command(self, input_path: Path, command: list[str]) -> list[str]:
        """Проверяет локальный файл и исполняемый файл команды, возвращает нормализованную команду."""
        if not input_path.exists():
            raise FileNotFoundError(f"Локальный входной файл не найден: {input_path}")

        executable = str(command[0])
        normalized_command = [executable, *[str(arg) for arg in command[1:]]]
        if shutil.which(executable) is None:
            raise FileNotFoundError(f"Не найден исполняемый файл: {executable}")

        return normalized_command

    async def publish_progress_event(self, event: KafkaVideoJobProgressEventSchema) -> None:
        """Публикует промежуточное событие прогресса в Kafka."""
        await self.producer.publish_progress(event.model_dump(mode="json"))

    async def publish_final_event(self, event: KafkaVideoJobFinalEventSchema) -> None:
        """Публикует финальное событие задачи в Kafka."""
        await self.producer.publish_progress(event.model_dump(mode="json"))

    async def publish_started_event(self, job_id: UUID, file_id: UUID, chunks_total_estimate: int) -> None:
        """Формирует и публикует событие старта обработки."""
        event = KafkaVideoJobProgressEventSchema(
            job_id=job_id,
            file_id=file_id,
            status="started",
            chunk_index=0,
            chunk_key="",
            chunks_done=0,
            chunks_total_estimate=chunks_total_estimate,
            ts=datetime.now(UTC),
        )
        await self.publish_progress_event(event)

    async def estimate_chunks_total(self, input_path: Path, chunk_seconds: int) -> int:
        """Оценивает общее число чанков по длительности видео через ffprobe."""
        if chunk_seconds <= 0:
            raise ValueError("chunk_seconds должен быть > 0")

        ffprobe_command = self.prepare_local_unix_command(
            input_path=input_path,
            command=[
                settings.app.ffprobe_command,
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                str(input_path),
            ],
        )

        process = await asyncio.create_subprocess_exec(
            *ffprobe_command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()

        if process.returncode != 0:
            error_text = stderr.decode("utf-8", errors="replace").strip()
            raise RuntimeError(f"Ошибка ffprobe (code={process.returncode}): {error_text}")

        duration_text = stdout.decode("utf-8", errors="replace").strip()
        if not duration_text:
            raise RuntimeError("ffprobe не вернул длительность видео")

        duration_seconds = float(duration_text)
        return max(1, math.ceil(duration_seconds / chunk_seconds))

    async def validate_input_size(self, bucket: str, input_key: str, max_bytes: int | None = None) -> S3ObjectMeta:
        """Проверяет размер входного объекта через S3 HEAD."""
        return await self.s3_storage.validate_input_size(bucket=bucket, input_key=input_key, max_bytes=max_bytes)

    async def download_input_video(self, job_id: UUID, input_bucket: str, input_key: str) -> Path:
        """Скачивает исходное видео в рабочую временную директорию задачи."""
        destination = self.build_local_video_path(job_id=job_id, input_key=input_key)
        return await self.s3_storage.download_file(
            input_bucket=input_bucket,
            input_key=input_key,
            destination=destination,
        )

    async def upload_chunk(self, output_bucket: str, output_prefix: str, chunk_index: int, local_path: Path) -> str:
        """Загружает локальный чанк в S3 и возвращает его ключ."""
        chunk_key = self.build_chunk_key(output_prefix=output_prefix, chunk_index=chunk_index)
        await self.s3_storage.upload_file(bucket=output_bucket, object_key=chunk_key, local_path=local_path)
        return chunk_key

    async def upload_manifest(self, output_bucket: str, output_prefix: str, manifest: dict) -> str:
        """Загружает manifest в S3 и возвращает ключ manifest-файла."""
        manifest_key = self.build_manifest_key(output_prefix=output_prefix)
        await self.s3_storage.upload_manifest(bucket=output_bucket, object_key=manifest_key, data=manifest)
        return manifest_key

    def resolve_chunking_strategy(self, mode: str) -> ChunkingStrategy:
        """Разрешает стратегию фрагментации по значению `mode`."""
        return self.chunking_strategy_resolver.resolve(mode=mode)

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
