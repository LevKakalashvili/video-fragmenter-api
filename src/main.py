import asyncio
import warnings
from collections.abc import Awaitable, Callable

from faststream import FastStream
from loguru import logger

from src.broker_subs import register_subs
from src.core.asgi_interceptors import ConsoleLifecycleInterceptor
from src.core.logger import init_logging
from src.core.settings import settings
from src.di import container
from src.infra.broker.kafka import kafka, kafka_consumer, kafka_producer
from src.infra.s3.repository import S3Repository

try:
    from dishka_faststream import setup_dishka as setup_faststream
except ImportError:
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="The integration has been moved to the dishka-faststream package*",
            category=DeprecationWarning,
        )
        from dishka.integrations.faststream import setup_dishka as setup_faststream

init_logging(is_debug=settings.app.debug)

interceptor = ConsoleLifecycleInterceptor()
stream_app = FastStream(
    kafka,
    after_startup=[interceptor.on_startup_complete],
)

register_subs(kafka_consumer)
setup_faststream(container=container, app=stream_app, auto_inject=True, finalize_container=True)


def _parse_kafka_bootstrap_servers(bootstrap_servers: str) -> list[tuple[str, int]]:
    endpoints: list[tuple[str, int]] = []
    for raw_item in bootstrap_servers.split(","):
        item = raw_item.strip()
        if not item:
            continue

        host, sep, port_str = item.rpartition(":")
        if sep and host and port_str.isdigit():
            endpoints.append((host, int(port_str)))
            continue

        endpoints.append((item, 9092))
    return endpoints


async def _check_tcp_resource(resource_name: str, host: str, port: int) -> None:
    timeout = settings.console.resource_check_timeout_seconds
    _, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=timeout)
    writer.close()
    await writer.wait_closed()
    logger.info(f"Проверка ресурса {resource_name}: УСПЕХ ({host}:{port})")


async def _check_kafka_available() -> None:
    endpoints = _parse_kafka_bootstrap_servers(settings.kafka.bootstrap_servers)
    if not endpoints:
        raise RuntimeError("Список bootstrap_servers пуст")

    errors: list[str] = []
    for host, port in endpoints:
        try:
            await _check_tcp_resource("kafka", host, port)
            return
        except Exception as exc:
            errors.append(f"{host}:{port} -> {exc!r}")

    raise RuntimeError("Не удалось подключиться ни к одному Kafka broker. " + "; ".join(errors))


async def _check_redis_available() -> None:
    await _check_tcp_resource("redis", settings.redis.host, settings.redis.port)


async def _check_minio_available() -> None:
    repository = S3Repository()
    bucket = settings.minio.bucket
    await repository.ensure_bucket_available(bucket=bucket)
    logger.info(f"Проверка ресурса minio: УСПЕХ (bucket={bucket})")


async def _run_startup_resource_checks() -> None:
    checks: tuple[tuple[str, Callable[[], Awaitable[None]]], ...] = (
        ("minio", _check_minio_available),
        ("kafka", _check_kafka_available),
        ("redis", _check_redis_available),
    )

    for resource_name, check in checks:
        try:
            await check()
        except Exception as exc:
            logger.error(f"Проверка ресурса {resource_name}: ОШИБКА. Exception: {exc!r}")


async def run() -> None:
    await _run_startup_resource_checks()
    await kafka_producer.start()
    try:
        await interceptor.run(lambda: stream_app.run(sleep_time=settings.console.sleep_time_seconds))
    finally:
        await kafka_producer.stop()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
