from dishka.integrations.faststream import FromDishka
from faststream.kafka import KafkaRouter
from loguru import logger

from src.core.settings import settings
from src.infra.broker.schemas import KafkaVideoJobInSchema
from src.services.video_job import VideoJobService

router = KafkaRouter()


@router.subscriber(settings.kafka.topic_in, group_id=settings.kafka.group_id)
async def handle_video_job_event(
    data: KafkaVideoJobInSchema,
    video_job_service: FromDishka[VideoJobService],
) -> None:
    logger.info("Received video job event: {}", data)

    await video_job_service.validate_input_size(
        bucket=data.input.bucket,
        key=data.input.key,
        max_bytes=data.limits.max_bytes,
    )
    local_video_path = await video_job_service.download_input_video(
        job_id=data.job_id,
        input_bucket=data.input.bucket,
        input_key=data.input.key,
    )
    await video_job_service.publish_started_event(job_id=data.job_id, file_id=data.file_id)

    logger.info("Video file downloaded to temporary path: {}", local_video_path)
