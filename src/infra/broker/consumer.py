from faststream.kafka import KafkaBroker


class KafkaConsumer:
    def __init__(self, broker: KafkaBroker):
        self.broker = broker
        self._subscribers_registered = False

    def register_subscribers(self) -> None:
        if self._subscribers_registered:
            return

        from src.broker_subs import video_jobs

        self.broker.include_router(video_jobs.router)
        self._subscribers_registered = True

    async def start(self) -> None:
        self.register_subscribers()
        await self.broker.start()

    async def stop(self) -> None:
        await self.broker.stop()
