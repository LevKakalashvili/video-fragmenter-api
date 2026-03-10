import asyncio
import math
import shutil
import tempfile
from pathlib import Path
from uuid import UUID

from loguru import logger

from src.core.settings import settings
from src.infra.broker.schemas import KafkaVideoJobInSchema
from src.infra.s3.repository import S3ObjectMeta
from src.services.chunk_execution import ChunkExecutionRequest, FfmpegChunkExecutionService
from src.services.chunk_upload import ChunkUploadRequest, S3ChunkUploadService
from src.services.job_state import JobStateService
from src.services.manifest import ManifestBuildRequest, ManifestChunkReference, S3ManifestService
from src.services.s3_storage import S3StorageService
from src.services.video_probe import FfprobeVideoProbeService, VideoProbeRequest


class VideoJobService:
    """
    Сервис полного жизненного цикла обработки одного video job.

    Отвечает за:
    - валидацию входного файла;
    - скачивание и локальную временную рабочую директорию;
    - запуск ffprobe/ffmpeg;
    - параллельную обработку чанков с лимитом `M`;
    - загрузку чанков и manifest в S3;
    - публикацию состояния через JobStateService;
    - гарантированную очистку временных артефактов.
    """

    def __init__(
        self,
        s3_storage: S3StorageService,
        job_state_service: JobStateService,
        chunk_execution_service: FfmpegChunkExecutionService,
        chunk_upload_service: S3ChunkUploadService,
        manifest_service: S3ManifestService,
        video_probe_service: FfprobeVideoProbeService,
    ):
        """Инициализирует зависимости сервиса обработки видео."""
        self.s3_storage = s3_storage
        self.job_state_service = job_state_service
        self.chunk_execution_service = chunk_execution_service
        self.chunk_upload_service = chunk_upload_service
        self.manifest_service = manifest_service
        self.video_probe_service = video_probe_service
        # Все временные артефакты задачи складываются в системный temp-каталог.
        self.tmp_root = Path(tempfile.gettempdir())

    async def publish_started_event(
        self, job_id: UUID, file_id: UUID, chunks_total_estimate: int
    ) -> None:
        """Публикует событие старта обработки через единый state service."""
        await self.job_state_service.mark_job_started(
            job_id=job_id,
            file_id=file_id,
            chunks_total_estimate=chunks_total_estimate,
        )

    async def validate_input_size(
        self, bucket: str, input_key: str, max_bytes: int | None = None
    ) -> S3ObjectMeta:
        """Проверяет размер входного объекта через S3 HEAD."""
        return await self.s3_storage.validate_input_size(
            bucket=bucket, input_key=input_key, max_bytes=max_bytes
        )

    async def download_input_video(self, job_id: UUID, input_bucket: str, input_key: str) -> Path:
        """Скачивает исходное видео в рабочую временную директорию задачи."""
        destination = self.build_local_video_path(job_id=job_id, input_key=input_key)
        return await self.s3_storage.download_file(
            input_bucket=input_bucket,
            input_key=input_key,
            destination=destination,
        )

    def build_job_tmp_dir(self, job_id: UUID) -> Path:
        """Возвращает рабочую временную директорию задачи."""
        return self.tmp_root / str(job_id)

    def build_local_video_path(self, job_id: UUID, input_key: str) -> Path:
        """Строит локальный путь для входного видео в директории задачи."""
        job_dir = self.build_job_tmp_dir(job_id=job_id)
        job_dir.mkdir(parents=True, exist_ok=True)
        return job_dir / Path(input_key).name

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
        chunks_total_estimate = 0

        try:
            # HEAD-проверка размера и content-type перед download, чтобы не качать файл целиком.
            await self.validate_input_size(
                bucket=payload.input.bucket,
                input_key=payload.input.key,
                max_bytes=payload.limits.max_bytes,
            )
            # Копируем исходный файл в tmp-каталог конкретной задачи.
            local_video_path = await self.download_input_video(
                job_id=job_id,
                input_bucket=payload.input.bucket,
                input_key=payload.input.key,
            )
            logger.info(
                f"Исходный файл скачан: job_id={job_id}, file_id={payload.file_id}, "
                f"path={local_video_path}"
            )
            output_dir = self.build_job_tmp_dir(job_id=job_id)

            # Probe-сервис является единственным источником metadata по локальному видеофайлу.
            probe_result = await self.video_probe_service.probe(
                request=VideoProbeRequest(
                    input_path=local_video_path,
                )
            )
            logger.info(
                f"Результат probe получен: job_id={job_id}, file_id={payload.file_id}, "
                f"duration_seconds={probe_result.duration_seconds}"
            )

            # Планирование чанков опирается только на нормализованный ProbeResult.
            chunks_total_estimate = max(
                1, math.ceil(probe_result.duration_seconds / payload.chunking.seconds)
            )
            await self.publish_started_event(
                job_id=job_id,
                file_id=payload.file_id,
                chunks_total_estimate=chunks_total_estimate,
            )

            logger.info(
                f"Старт планирования чанков: job_id={job_id}, file_id={payload.file_id}, "
                f"chunks_total={chunks_total_estimate}, "
                f"M={settings.app.max_chunk_processes_per_video}"
            )
            chunk_references = await self._run_chunk_queue(
                job_id=job_id,
                payload=payload,
                input_path=local_video_path,
                output_dir=output_dir,
                chunks_total_estimate=chunks_total_estimate,
            )
            chunks_done = len(chunk_references)

            logger.info(
                f"Все чанки загружены: job_id={job_id}, file_id={payload.file_id}, "
                f"chunks_total={chunks_done}"
            )
            manifest_result = await self.manifest_service.build_and_upload(
                request=ManifestBuildRequest(
                    job_id=job_id,
                    file_id=payload.file_id,
                    output_bucket=payload.output.bucket,
                    output_prefix=payload.output.prefix,
                    chunk_seconds=payload.chunking.seconds,
                    chunk_mode=payload.chunking.mode,
                    chunk_keys=tuple(chunk_references),
                    input_bucket=payload.input.bucket,
                    input_key=payload.input.key,
                )
            )
            logger.info(
                f"Manifest создан: job_id={job_id}, file_id={payload.file_id}, "
                f"manifest_key={manifest_result.manifest_key}"
            )
            await self.job_state_service.mark_job_completed(
                job_id=job_id,
                file_id=payload.file_id,
                chunks_done=chunks_done,
                manifest_key=manifest_result.manifest_key,
            )
            logger.info(
                f"Видео job завершен: job_id={job_id}, file_id={payload.file_id}, "
                f"chunks_done={chunks_done}"
            )
        except Exception as exc:
            # При любой ошибке публикуем failed-события и отдаём исключение выше,
            # чтобы subscriber/scheduler могли корректно зафиксировать провал job.
            await self.job_state_service.mark_job_failed(
                job_id=job_id,
                file_id=payload.file_id,
                chunks_done=chunks_done,
                chunks_total_estimate=chunks_total_estimate,
                error_text=str(exc),
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
        chunks_total_estimate: int,
    ) -> list[ManifestChunkReference]:
        # Очередь chunk job-ов одного видео.
        chunk_queue: asyncio.Queue[ChunkExecutionRequest | None] = asyncio.Queue()
        for chunk_index in range(chunks_total_estimate):
            chunk_request = ChunkExecutionRequest(
                chunk_index=chunk_index,
                input_path=input_path,
                output_path=output_dir / f"chunk_{chunk_index:06d}.mp4",
                start_seconds=chunk_index * payload.chunking.seconds,
                duration_seconds=payload.chunking.seconds,
                chunk_mode=payload.chunking.mode,
            )
            logger.info(
                f"Чанк поставлен в очередь: job_id={job_id}, file_id={payload.file_id}, "
                f"chunk_index={chunk_index}, output_path={chunk_request.output_path}"
            )
            chunk_queue.put_nowait(chunk_request)

        # Количество ffmpeg worker-ов для одного файла ограничено M.
        workers_total = max(1, settings.app.max_chunk_processes_per_video)
        # `None`-сентинелы завершают каждый worker после обработки всех real jobs.
        for _ in range(workers_total):
            chunk_queue.put_nowait(None)

        # Количество уже отправленных в Kafka chunk_uploaded событий.
        chunks_done = 0
        uploaded_chunk_references: list[ManifestChunkReference] = []
        # Отдельный лимитер upload-операций (может быть меньше/больше M).
        upload_limit = asyncio.Semaphore(max(1, settings.app.max_upload_concurrency))
        # Буфер готовых upload-результатов: publisher забирает их строго по порядку индексов.
        ready_chunks: dict[int, str] = {}
        # Condition координирует producer/consumer между worker-ами и publisher-корутинами.
        ready_chunks_condition = asyncio.Condition()

        async def worker() -> None:
            while True:
                request = await chunk_queue.get()
                try:
                    if request is None:
                        # Сентинел: завершаем worker без ошибки.
                        return

                    try:
                        await self.job_state_service.mark_chunk_processing(
                            job_id=job_id,
                            file_id=payload.file_id,
                            chunk_index=request.chunk_index,
                            chunks_total_estimate=chunks_total_estimate,
                        )
                        result = await self.chunk_execution_service.execute(request=request)

                        # Upload ограничиваем отдельным семафором, чтобы не перегружать S3.
                        async with upload_limit:
                            upload_result = await self.chunk_upload_service.upload(
                                request=ChunkUploadRequest(
                                    chunk_index=result.chunk_index,
                                    local_path=result.output_path,
                                    output_bucket=payload.output.bucket,
                                    output_prefix=payload.output.prefix,
                                    cleanup_local_file=True,
                                )
                            )

                        logger.info(
                            f"Чанк загружен: job_id={job_id}, file_id={payload.file_id}, "
                            f"chunk_index={upload_result.chunk_index}, "
                            f"chunk_key={upload_result.chunk_key}"
                        )
                        # После upload чанк "готов к публикации", но само событие отправим
                        # отдельной корутиной строго по chunk_index.
                        async with ready_chunks_condition:
                            ready_chunks[upload_result.chunk_index] = upload_result.chunk_key
                            ready_chunks_condition.notify_all()
                    except Exception as exc:
                        await self.job_state_service.mark_chunk_failed(
                            job_id=job_id,
                            file_id=payload.file_id,
                            chunk_index=request.chunk_index,
                            error_text=str(exc),
                            chunks_done=chunks_done,
                            chunks_total_estimate=chunks_total_estimate,
                        )
                        raise
                finally:
                    # task_done обязателен в finally: и при успехе, и при ошибке.
                    chunk_queue.task_done()

        async def publish_chunks_in_order() -> None:
            nonlocal chunks_done
            for chunk_index in range(chunks_total_estimate):
                async with ready_chunks_condition:
                    # Ждём именно текущий индекс, чтобы сообщения в Kafka
                    # шли в детерминированном порядке.
                    while chunk_index not in ready_chunks:
                        await ready_chunks_condition.wait()
                    chunk_key = ready_chunks.pop(chunk_index)

                chunks_done += 1
                await self.job_state_service.mark_chunk_uploaded(
                    job_id=job_id,
                    file_id=payload.file_id,
                    chunk_index=chunk_index,
                    chunk_key=chunk_key,
                    chunks_done=chunks_done,
                    chunks_total_estimate=chunks_total_estimate,
                )
                uploaded_chunk_references.append(
                    ManifestChunkReference(
                        chunk_index=chunk_index,
                        chunk_key=chunk_key,
                    )
                )
                logger.info(
                    f"Чанк отмечен как загруженный: job_id={job_id}, file_id={payload.file_id}, "
                    f"chunk_index={chunk_index}, chunks_done={chunks_done}/{chunks_total_estimate}"
                )

        # TaskGroup упрощает fan-out/fan-in:
        # если один worker падает, группа отменяет остальные и пробрасывает ошибку наружу.
        async with asyncio.TaskGroup() as task_group:
            for _ in range(workers_total):
                task_group.create_task(worker())
            task_group.create_task(publish_chunks_in_order())

        return uploaded_chunk_references
