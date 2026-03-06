from src.core.logger import init_logging
from src.core.settings import settings
init_logging(is_debug=settings.app.debug)

from contextlib import asynccontextmanager

from dishka.integrations.fastapi import setup_dishka as setup_fastapi
from dishka.integrations.faststream import setup_dishka as setup_faststream
from faststream import FastStream

from src.di import container
from src.api.rest import create_app
from src.broker_subs import register_subs
from src.infra.broker.kafka import kafka, kafka_consumer, kafka_producer

@asynccontextmanager
async def lifespan(app_):
    register_subs(kafka_consumer)

    await kafka_producer.start()
    await kafka_consumer.start()
    try:
        yield
    finally:
        await kafka_consumer.stop()
        await kafka_producer.stop()


app = create_app(lifespan_=lifespan)
setup_fastapi(container=container, app=app)
setup_faststream(container, FastStream(kafka), auto_inject=True)

