import asyncio
import warnings
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from faststream.kafka import KafkaRouter
from loguru import logger

from src.core.settings import settings
from src.infra.broker.schemas import KafkaVideoJobInSchema, KafkaVideoJobProgressEventSchema
from src.services.video_job import VideoJobService

try:
    from dishka_faststream import FromDishka
except ImportError:
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="The integration has been moved to the dishka-faststream package*",
            category=DeprecationWarning,
        )
        from dishka.integrations.faststream import FromDishka

router = KafkaRouter()


@router.subscriber(
    settings.kafka.topic_in,
    group_id=settings.kafka.group_id,
    title="Обработка событий задач на фрагментацию видео ",
    description="Обработка событий задач на фрагментацию видео",
)
async def handle_video_job_event(
    data: KafkaVideoJobInSchema,
    video_job_service: FromDishka[VideoJobService],
) -> None:
    job_id = uuid4()
    # Входной ключ должен быть полным S3 object key файла, например: "videos/in/file.mp4".
    input_object_key = data.input.key
    # Ссылки на локальные артефакты держим заранее, чтобы гарантированно почистить их в finally.
    local_video_path: Path | None = None
    output_dir: Path | None = None

    # Фиксируем входящее сообщение задачи для трассировки пайплайна.
    logger.info(f"Получено событие задачи на фрагментацию видео: {data}, сгенерированный job_id={job_id}")

    # Проверяем размер файла до скачивания, чтобы не тратить ресурсы на недопустимые объекты.
    await video_job_service.validate_input_size(
        bucket=data.input.bucket,
        input_key=input_object_key,
        max_bytes=data.limits.max_bytes,
    )
    # Выбираем стратегию фрагментации по параметру mode из Kafka-сообщения.
    chunking_strategy = video_job_service.resolve_chunking_strategy(mode=data.chunking.mode)
    try:
        # Скачиваем исходное видео во временную директорию конкретной задачи.
        local_video_path = await video_job_service.download_input_video(
            job_id=job_id,
            input_bucket=data.input.bucket,
            input_key=input_object_key,
        )
        chunks_total_estimate = await video_job_service.estimate_chunks_total(
            input_path=local_video_path,
            chunk_seconds=data.chunking.seconds,
        )
        # Публикуем событие старта обработки.
        await video_job_service.publish_started_event(
            job_id=job_id,
            file_id=data.file_id,
            chunks_total_estimate=chunks_total_estimate,
        )

        # Логируем путь локального файла и выбранный режим фрагментации для диагностики.
        logger.info(f"Видеофайл скачан во временный путь: {local_video_path}")
        logger.info(f"Выбрана стратегия фрагментации: {chunking_strategy.mode}")
        logger.info(f"Оценка общего числа чанков: {chunks_total_estimate}")

        # Рабочая директория совпадает с job tmp dir; туда ffmpeg положит chunk_*.mp4.
        output_dir = video_job_service.build_job_tmp_dir(job_id=job_id)
        # Конкретная ffmpeg-команда строится выбранной стратегией.
        ffmpeg_command = chunking_strategy.build_ffmpeg_command(
            input_path=local_video_path,
            output_dir=output_dir,
            chunk_seconds=data.chunking.seconds,
        )
        ffmpeg_command = video_job_service.prepare_local_unix_command(
            input_path=local_video_path,
            command=ffmpeg_command,
        )

        logger.info(f"Запуск ffmpeg для чанкирования: {' '.join(ffmpeg_command)}")
        try:
            # Запускаем ffmpeg и параллельно подхватываем готовые чанки для немедленной загрузки.
            process = await asyncio.create_subprocess_exec(
                *ffmpeg_command,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
        except Exception as exc:
            logger.exception(f"Не удалось запустить subprocess ffmpeg. Команда: {' '.join(ffmpeg_command)}")
            raise RuntimeError(f"Ошибка запуска ffmpeg subprocess: {exc}") from exc
        # Отдельная task позволяет одновременно ждать завершение ffmpeg и загружать чанки.
        ffmpeg_wait_task = asyncio.create_task(process.wait())
        # Множество уже загруженных локальных файлов, чтобы не обрабатывать один и тот же чанк повторно.
        uploaded_chunks: set[Path] = set()
        # Счетчики прогресса для Kafka-событий.
        chunks_done = 0
        highest_seen_index = -1

        while True:
            # Сканируем файлы чанков на текущий момент.
            current_chunks = sorted(output_dir.glob("chunk_*.mp4"))
            # Проверяем завершился ли ffmpeg, не блокируя цикл.
            ffmpeg_finished = ffmpeg_wait_task.done()

            # Пока ffmpeg работает, последний чанк может быть недописан — пропускаем его.
            chunks_to_upload = current_chunks if ffmpeg_finished else current_chunks[:-1]

            for chunk_path in chunks_to_upload:
                if chunk_path in uploaded_chunks:
                    # Чанк уже был загружен на одной из предыдущих итераций.
                    continue
                # Берем индекс из имени chunk_000123.mp4, чтобы сохранить стабильную нумерацию.
                chunk_index = int(chunk_path.stem.split("_")[-1])
                highest_seen_index = max(highest_seen_index, chunk_index)
                chunk_key = await video_job_service.upload_chunk(
                    output_bucket=data.output.bucket,
                    output_prefix=data.output.prefix,
                    chunk_index=chunk_index,
                    local_path=chunk_path,
                )
                chunks_done += 1
                # Событие отправляется на каждый успешно загруженный чанк.
                await video_job_service.publish_progress_event(
                    KafkaVideoJobProgressEventSchema(
                        job_id=job_id,
                        file_id=data.file_id,
                        status="chunk_uploaded",
                        chunk_index=chunk_index,
                        chunk_key=chunk_key,
                        chunks_done=chunks_done,
                        chunks_total_estimate=chunks_total_estimate,
                        ts=datetime.now(UTC),
                    )
                )
                # Удаляем локальный чанк сразу после успешной загрузки в S3.
                video_job_service.remove_local_file(chunk_path)
                uploaded_chunks.add(chunk_path)

            if ffmpeg_finished:
                # После завершения ffmpeg выходим из polling-цикла и проверяем код возврата.
                break
            # Небольшая пауза, чтобы не делать слишком частый опрос файловой системы.
            await asyncio.sleep(0.2)

        # Получаем итоговый код завершения ffmpeg (0 = успех).
        ffmpeg_return_code = ffmpeg_wait_task.result()
        # Любой ненулевой код возврата считаем ошибкой обработки.
        if ffmpeg_return_code != 0:
            error_text = await process.stderr.read() if process.stderr is not None else b""
            decoded_error = error_text.decode("utf-8", errors="replace").strip()
            raise RuntimeError(f"Ошибка ffmpeg (code={ffmpeg_return_code}): {decoded_error}")

        logger.info(f"Чанкирование завершено, загружено чанков: {chunks_done}")
    finally:
        # Если часть чанков осталась локально из-за ошибки, удаляем их в cleanup.
        if output_dir is not None:
            for chunk_path in output_dir.glob("chunk_*.mp4"):
                video_job_service.remove_local_file(chunk_path)
        # Удаляем исходный локальный файл после завершения или ошибки.
        if local_video_path is not None:
            video_job_service.remove_local_file(local_video_path)
        # Чистим рабочую временную директорию задачи целиком.
        video_job_service.cleanup_job_tmp_dir(job_id=job_id)
