from dishka.integrations.fastapi import DishkaRoute, FromDishka
from fastapi import APIRouter
from faststream.kafka import KafkaBroker

from src.api.schemas.health import Health
from src.infra.s3.repository import S3Repository

router = APIRouter(prefix="/health", tags=["health"], route_class=DishkaRoute)


@router.get("", response_model=Health)
async def health(
    broker: FromDishka[KafkaBroker],
    s3_repository: FromDishka[S3Repository],
):
    _timeout = 5

    status_broker = await broker.ping(_timeout)
    status_s3 = await s3_repository.is_bucket_available()

    return {
        "status": "available" if all((status_broker, status_s3)) else "unavailable",
        "extra": {
            "broker": status_broker,
            "s3": status_s3,
        },
    }
