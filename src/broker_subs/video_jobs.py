import warnings
from uuid import uuid4

from faststream.kafka import KafkaRouter
from loguru import logger

from src.core.settings import settings
from src.infra.broker.schemas import KafkaVideoJobInSchema
from src.services.in_memory_scheduler import InMemoryVideoScheduler
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
    video_scheduler: FromDishka[InMemoryVideoScheduler],
) -> None:
    job_id = uuid4()
    logger.info(f"Получено событие задачи на фрагментацию видео: {data}, сгенерированный job_id={job_id}")
    logger.info(
        "Задача поставлена в in-memory scheduler: job_id={}, file_id={}, queue_limit_K={}",
        job_id,
        data.file_id,
        settings.app.max_videos_in_progress,
    )
    await video_scheduler.submit(lambda: video_job_service.process_video_job(job_id=job_id, payload=data))
