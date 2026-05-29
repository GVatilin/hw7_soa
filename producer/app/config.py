import os
from dataclasses import dataclass


@dataclass(slots=True)
class Settings:
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092")
    kafka_topic: str = os.getenv("KAFKA_TOPIC", "movie-events")
    kafka_partitions: int = int(os.getenv("KAFKA_PARTITIONS", "3"))
    kafka_replication_factor: int = int(os.getenv("KAFKA_REPLICATION_FACTOR", "2"))
    kafka_min_insync_replicas: str = os.getenv("KAFKA_MIN_INSYNC_REPLICAS", "1")
    schema_registry_url: str = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    event_schema_path: str = os.getenv("EVENT_SCHEMA_PATH", "/app/schemas/movie_event.avsc")


settings = Settings()
