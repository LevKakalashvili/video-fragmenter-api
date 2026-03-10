import asyncio
import math
import shutil
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

from loguru import logger

from src.core.settings import settings
from src.infra.broker.producer import KafkaProducer
from src.infra.broker.schemas import (
    KafkaVideoJobFinalEventSchema,
    KafkaVideoJobInSchema,
    KafkaVideoJobProgressEventSchema,
)
from src.infra.s3.repository import S3ObjectMeta
from src.services.chunking.base import ChunkingStrategy
from src.services.chunking.resolver import ChunkingStrategyResolver
from src.services.s3_storage import S3StorageService


class VideoJobService:
    """
    Сервис полного жизненного цикла обработки одного video job.

    Отвечает за:
    - валидацию входного файла;
    - скачивание и локальную временную рабочую директорию;
    - запуск ffprobe/ffmpeg;
    - параллельную обработку чанков с лимитом `M`;
    - загрузку чанков и manifest в S3;
    - публикацию progress/final событий в Kafka;
    - гарантированную очистку временных артефактов.
    """

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
        # Защита от запуска ffmpeg/ffprobe по несуществующему пути.
        if not input_path.exists():
            raise FileNotFoundError(f"Локальный входной файл не найден: {input_path}")

        executable = str(command[0])
        # Нормализуем все аргументы в str, чтобы избежать смешения Path/str при subprocess.
        normalized_command = [executable, *[str(arg) for arg in command[1:]]]
        # Fail-fast проверка: бинарник должен быть доступен в PATH или по абсолютному пути.
        if shutil.which(executable) is None:
            raise FileNotFoundError(f"Не найден исполняемый файл: {executable}")

        return normalized_command

    async def publish_progress_event(self, event: KafkaVideoJobProgressEventSchema) -> None:
        """Публикует промежуточное событие прогресса в Kafka."""
        await self.producer.publish_progress(
            event.model_dump(mode="json"),
            key=str(event.file_id),
        )

    async def publish_final_event(self, event: KafkaVideoJobFinalEventSchema) -> None:
        """Публикует финальное событие задачи в Kafka."""
        await self.producer.publish_progress(
            event.model_dump(mode="json"),
            key=str(event.file_id),
        )

    async def publish_started_event(self, job_id: UUID, file_id: UUID, chunks_total_estimate: int) -> None:
        """Формирует и публикует событие старта обработки."""
        # started-событие фиксирует, что job прошёл pre-check шаги и принят к выполнению.
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
        duration_seconds = await self.get_video_duration_seconds(input_path=input_path)
        # Всегда минимум 1 чанк, даже для очень коротких роликов.
        return max(1, math.ceil(duration_seconds / chunk_seconds))

    async def get_video_duration_seconds(self, input_path: Path) -> float:
        """Возвращает длительность видео (секунды) через ffprobe."""
        # Используем ffprobe как источник истины по duration,
        # чтобы корректно рассчитать количество chunk jobs.
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

        # Любой ненулевой код возврата ffprobe считаем ошибкой входных данных или окружения.
        if process.returncode != 0:
            error_text = stderr.decode("utf-8", errors="replace").strip()
            raise RuntimeError(f"Ошибка ffprobe (code={process.returncode}): {error_text}")

        duration_text = stdout.decode("utf-8", errors="replace").strip()
        if not duration_text:
            raise RuntimeError("ffprobe не вернул длительность видео")

        # Возвращаем float, а округление делается на уровне бизнес-логики.
        return float(duration_text)

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

    async def process_video_job(self, job_id: UUID, payload: KafkaVideoJobInSchema) -> None:
        """Выполняет полную обработку видео с per-file очередью чанков и лимитом M."""
        # Держим ссылки заранее, чтобы в finally гарантированно очистить ресурсы.
        local_video_path: Path | None = None
        output_dir: Path | None = None
        # Счётчик фактически завершённых и загруженных чанков.
        chunks_done = 0

        # HEAD-проверка размера перед download, чтобы не качать файл целиком
        await self.validate_input_size(
            bucket=payload.input.bucket,
            input_key=payload.input.key,
            max_bytes=payload.limits.max_bytes,
        )
        # Выбираем chunking-стратегию согласно mode из входного сообщения.
        chunking_strategy = self.resolve_chunking_strategy(mode=payload.chunking.mode)
        try:
            # Копируем исходный файл в tmp-каталог конкретной задачи.
            local_video_path = await self.download_input_video(
                job_id=job_id,
                input_bucket=payload.input.bucket,
                input_key=payload.input.key,
            )
            output_dir = self.build_job_tmp_dir(job_id=job_id)

            # Оценка total чанков нужна для прогресс-событий и планирования очереди.
            duration_seconds = await self.get_video_duration_seconds(input_path=local_video_path)

            chunks_total_estimate = max(1, math.ceil(duration_seconds / payload.chunking.seconds))
            await self.publish_started_event(
                job_id=job_id,
                file_id=payload.file_id,
                chunks_total_estimate=chunks_total_estimate,
            )

            logger.info(
                "Старт chunk scheduler: job_id={}, file_id={}, chunks_total={}, M={}",
                job_id,
                payload.file_id,
                chunks_total_estimate,
                settings.app.max_chunk_processes_per_video,
            )
            chunks_done = await self._run_chunk_queue(
                job_id=job_id,
                payload=payload,
                input_path=local_video_path,
                output_dir=output_dir,
                chunking_strategy=chunking_strategy,
                chunks_total_estimate=chunks_total_estimate,
            )

            manifest_key = await self.upload_manifest(
                output_bucket=payload.output.bucket,
                output_prefix=payload.output.prefix,
                manifest={
                    "job_id": str(job_id),
                    "file_id": str(payload.file_id),
                    "chunk_seconds": payload.chunking.seconds,
                    "chunks_total": chunks_done,
                },
            )
            await self.publish_final_event(
                KafkaVideoJobFinalEventSchema(
                    job_id=job_id,
                    file_id=payload.file_id,
                    status="completed",
                    chunks_total=chunks_done,
                    manifest_key=manifest_key,
                    ts=datetime.now(UTC),
                )
            )
            logger.info(
                "Видео job завершен: job_id={}, file_id={}, chunks_done={}", job_id, payload.file_id, chunks_done
            )
        except Exception:
            # При любой ошибке публикуем failed-события и отдаём исключение выше,
            # чтобы subscriber/scheduler могли корректно зафиксировать провал job.
            await self.publish_final_event(
                KafkaVideoJobFinalEventSchema(
                    job_id=job_id,
                    file_id=payload.file_id,
                    status="failed",
                    chunks_total=chunks_done,
                    manifest_key="",
                    ts=datetime.now(UTC),
                )
            )
            await self.publish_progress_event(
                KafkaVideoJobProgressEventSchema(
                    job_id=job_id,
                    file_id=payload.file_id,
                    status="failed",
                    chunk_index=0,
                    chunk_key="",
                    chunks_done=chunks_done,
                    chunks_total_estimate=chunks_done,
                    ts=datetime.now(UTC),
                )
            )
            raise
        finally:
            # Защитная уборка: если локальные chunk-файлы остались, удаляем их.
            if output_dir is not None:
                for chunk_path in output_dir.glob("chunk_*.mp4"):
                    self.remove_local_file(chunk_path)
            # Удаляем скачанный исходник.
            if local_video_path is not None:
                self.remove_local_file(local_video_path)
            # Чистим корневой tmp-каталог задачи.
            self.cleanup_job_tmp_dir(job_id=job_id)

    async def _run_chunk_queue(
        self,
        job_id: UUID,
        payload: KafkaVideoJobInSchema,
        input_path: Path,
        output_dir: Path,
        chunking_strategy: ChunkingStrategy,
        chunks_total_estimate: int,
    ) -> int:
        # Очередь chunk job-ов одного видео.
        # Каждый элемент = индекс чанка, по которому worker вычисляет offset.
        chunk_queue: asyncio.Queue[int | None] = asyncio.Queue()
        for chunk_index in range(chunks_total_estimate):
            chunk_queue.put_nowait(chunk_index)

        # Количество ffmpeg worker-ов для одного файла ограничено M.
        workers_total = max(1, settings.app.max_chunk_processes_per_video)
        # `None`-сентинелы завершают каждый worker после обработки всех real jobs.
        for _ in range(workers_total):
            chunk_queue.put_nowait(None)

        # Количество уже отправленных в Kafka chunk_uploaded событий.
        chunks_done = 0
        # Отдельный лимитер upload-операций (может быть меньше/больше M).
        upload_limit = asyncio.Semaphore(max(1, settings.app.max_upload_concurrency))
        # Буфер готовых чанков: worker кладет сюда результат, publisher забирает по порядку индексов.
        ready_chunks: dict[int, str] = {}
        # Condition координирует producer/consumer между worker-ами и publisher-корутинами.
        ready_chunks_condition = asyncio.Condition()

        async def worker() -> None:
            while True:
                chunk_index = await chunk_queue.get()
                try:
                    if chunk_index is None:
                        # Сентинел: завершаем worker без ошибки.
                        return

                    # Старт каждого чанка вычисляем детерминированно от индекса.
                    chunk_start_seconds = chunk_index * payload.chunking.seconds
                    local_chunk_path = output_dir / f"chunk_{chunk_index:06d}.mp4"
                    # Строим ffmpeg-команду "один чанк = один subprocess".
                    ffmpeg_command = chunking_strategy.build_ffmpeg_chunk_command(
                        input_path=input_path,
                        output_path=local_chunk_path,
                        chunk_seconds=payload.chunking.seconds,
                        chunk_start_seconds=chunk_start_seconds,
                    )
                    ffmpeg_command = self.prepare_local_unix_command(input_path=input_path, command=ffmpeg_command)

                    # Запускаем ffmpeg subprocess для конкретного чанка.
                    process = await asyncio.create_subprocess_exec(
                        *ffmpeg_command,
                        stdout=asyncio.subprocess.DEVNULL,
                        stderr=asyncio.subprocess.PIPE,
                    )
                    _, stderr = await process.communicate()
                    if process.returncode != 0:
                        error_text = stderr.decode("utf-8", errors="replace").strip()
                        raise RuntimeError(
                            f"Ошибка ffmpeg (code={process.returncode}) для chunk={chunk_index}: {error_text}"
                        )

                    # Upload ограничиваем отдельным семафором, чтобы не перегружать S3.
                    async with upload_limit:
                        chunk_key = await self.upload_chunk(
                            output_bucket=payload.output.bucket,
                            output_prefix=payload.output.prefix,
                            chunk_index=chunk_index,
                            local_path=local_chunk_path,
                        )

                    # Локальный чанк больше не нужен после успешной загрузки.
                    self.remove_local_file(local_chunk_path)
                    # После upload чанк "готов к публикации", но само событие отправим
                    # отдельной корутиной строго по chunk_index.
                    async with ready_chunks_condition:
                        ready_chunks[chunk_index] = chunk_key
                        ready_chunks_condition.notify_all()
                finally:
                    # task_done обязателен в finally: и при успехе, и при ошибке.
                    chunk_queue.task_done()

        async def publish_chunks_in_order() -> None:
            nonlocal chunks_done
            for chunk_index in range(chunks_total_estimate):
                async with ready_chunks_condition:
                    # Ждём именно текущий индекс, чтобы сообщения в Kafka шли в детерминированном порядке.
                    while chunk_index not in ready_chunks:
                        await ready_chunks_condition.wait()
                    chunk_key = ready_chunks.pop(chunk_index)

                chunks_done += 1
                await self.publish_progress_event(
                    KafkaVideoJobProgressEventSchema(
                        job_id=job_id,
                        file_id=payload.file_id,
                        status="chunk_uploaded",
                        chunk_index=chunk_index,
                        chunk_key=chunk_key,
                        chunks_done=chunks_done,
                        chunks_total_estimate=chunks_total_estimate,
                        ts=datetime.now(UTC),
                    )
                )
                logger.info(
                    f"Chunk обработан: job_id={job_id}, file_id={payload.file_id}, "
                    f"chunk_index={chunk_index}, chunks_done={chunks_done}/{chunks_total_estimate}"
                )

        # TaskGroup упрощает fan-out/fan-in:
        # если один worker падает, группа отменяет остальные и пробрасывает ошибку наружу.
        async with asyncio.TaskGroup() as task_group:
            for _ in range(workers_total):
                task_group.create_task(worker())
            task_group.create_task(publish_chunks_in_order())

        return chunks_done
