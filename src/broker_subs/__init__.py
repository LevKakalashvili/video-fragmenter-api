from src.infra.broker.consumer import KafkaConsumer


def register_subs(consumer: KafkaConsumer) -> None:
    consumer.register_subscribers()
