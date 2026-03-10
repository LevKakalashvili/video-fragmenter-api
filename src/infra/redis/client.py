from redis.asyncio import Redis

from src.core.settings import settings

redis_client = Redis(
    host=settings.redis.host,
    port=settings.redis.port,
    db=settings.redis.db,
    password=settings.redis.password,
    decode_responses=True,
)
