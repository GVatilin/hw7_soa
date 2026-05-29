import json
import logging
import time
from pathlib import Path
from threading import Event

import requests
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from .config import settings

logger = logging.getLogger(__name__)


class KafkaEventProducer:
    def __init__(self) -> None:
        schema_path = Path(settings.event_schema_path)
        self.schema_str = schema_path.read_text(encoding="utf-8")
        self.topic = settings.kafka_topic
        self.bootstrap_servers = settings.kafka_bootstrap_servers
        self.schema_registry_url = settings.schema_registry_url
        self.admin = AdminClient({"bootstrap.servers": self.bootstrap_servers})
        self.schema_registry_client = SchemaRegistryClient({"url": self.schema_registry_url})
        self.avro_serializer = AvroSerializer(
            schema_registry_client=self.schema_registry_client,
            schema_str=self.schema_str,
            conf={"auto.register.schemas": True},
        )
        self.producer = Producer(
            {
                "bootstrap.servers": self.bootstrap_servers,
                "client.id": "movie-service",
                "acks": "all",
                "enable.idempotence": True,
                "retries": 10,
                "retry.backoff.ms": 500,
                "compression.type": "snappy",
                "linger.ms": 5,
            }
        )

    def bootstrap(self) -> None:
        self._ensure_topic()
        self._ensure_schema_registered()

    def _ensure_topic(self) -> None:
        metadata = self.admin.list_topics(timeout=15)
        if self.topic in metadata.topics:
            logger.info("topic already exists", extra={"topic": self.topic})
            return

        topic = NewTopic(
            topic=self.topic,
            num_partitions=settings.kafka_partitions,
            replication_factor=settings.kafka_replication_factor,
            config={"min.insync.replicas": settings.kafka_min_insync_replicas},
        )
        futures = self.admin.create_topics([topic])
        futures[self.topic].result(timeout=30)
        logger.info("topic created", extra={"topic": self.topic})

    def _ensure_schema_registered(self) -> None:
        subject = f"{self.topic}-value"
        payload = {"schema": json.dumps(json.loads(self.schema_str))}
        response = requests.post(
            f"{self.schema_registry_url}/subjects/{subject}/versions",
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            json=payload,
            timeout=10,
        )
        response.raise_for_status()
        body = response.json()
        logger.info("schema registered", extra={"subject": subject, "schema_id": body.get("id")})

    def publish(self, event: dict[str, object], key: str) -> None:
        context = SerializationContext(self.topic, MessageField.VALUE)
        payload = self.avro_serializer(event, context)
        if payload is None:
            raise RuntimeError("failed to serialize event")

        max_attempts = 5
        for attempt in range(1, max_attempts + 1):
            delivery_done = Event()
            delivery_error = [None]

            def _delivery_callback(err, _msg):
                if err is not None:
                    delivery_error[0] = RuntimeError(str(err))
                delivery_done.set()

            try:
                self.producer.produce(
                    topic=self.topic,
                    key=key.encode("utf-8"),
                    value=payload,
                    on_delivery=_delivery_callback,
                )
                deadline = time.time() + 10
                while not delivery_done.is_set() and time.time() < deadline:
                    self.producer.poll(0.1)
                if not delivery_done.is_set():
                    raise TimeoutError("delivery callback timeout")
                if delivery_error[0] is not None:
                    raise delivery_error[0]
                logger.info(
                    "event published",
                    extra={
                        "event_id": event["event_id"],
                        "event_type": event["event_type"],
                        "timestamp": event["timestamp"],
                    },
                )
                return
            except BufferError:
                self.producer.poll(0.1)
            except Exception:
                if attempt == max_attempts:
                    raise
                sleep_for = min(2 ** (attempt - 1), 8)
                logger.exception("publish retry", extra={"attempt": attempt, "sleep_for": sleep_for})
                time.sleep(sleep_for)

    def close(self) -> None:
        self.producer.flush(10)
