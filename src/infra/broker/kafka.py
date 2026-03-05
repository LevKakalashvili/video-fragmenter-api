from faststream.kafka import KafkaBroker

from src.core.settings import settings
from src.infra.broker.consumer import KafkaConsumer
from src.infra.broker.producer import KafkaProducer

kafka = KafkaBroker(settings.kafka.bootstrap_servers)
kafka_producer = KafkaProducer(kafka)
kafka_consumer = KafkaConsumer(kafka)
