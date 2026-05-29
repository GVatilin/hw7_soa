CREATE DATABASE IF NOT EXISTS movie_analytics;

CREATE TABLE IF NOT EXISTS movie_analytics.movie_events_raw
(
    event_date Date MATERIALIZED toDate(timestamp),
    event_id String,
    user_id String,
    movie_id String,
    event_type LowCardinality(String),
    timestamp DateTime64(3, 'UTC'),
    device_type LowCardinality(String),
    session_id String,
    progress_seconds UInt32,
    search_query Nullable(String),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, user_id, session_id, timestamp, event_id);

CREATE TABLE IF NOT EXISTS movie_analytics.movie_events_queue
(
    event_id String,
    user_id String,
    movie_id String,
    event_type String,
    timestamp DateTime64(3, 'UTC'),
    device_type String,
    session_id String,
    progress_seconds UInt32,
    search_query Nullable(String)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-1:9092,kafka-2:9092',
    kafka_topic_list = 'movie-events',
    kafka_group_name = 'clickhouse-movie-events',
    kafka_format = 'AvroConfluent',
    kafka_num_consumers = 1,
    kafka_thread_per_consumer = 0,
    kafka_handle_error_mode = 'stream',
    format_avro_schema_registry_url = 'http://schema-registry:8081';

CREATE MATERIALIZED VIEW IF NOT EXISTS movie_analytics.mv_movie_events_to_raw
TO movie_analytics.movie_events_raw
AS
SELECT
    event_id,
    user_id,
    movie_id,
    event_type,
    timestamp,
    device_type,
    session_id,
    progress_seconds,
    search_query
FROM movie_analytics.movie_events_queue;
