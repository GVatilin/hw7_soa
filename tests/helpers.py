import json
import time
from datetime import date, datetime, timezone
from uuid import uuid4

import boto3
import clickhouse_connect
import psycopg
import pytest
import requests
from botocore.config import Config


@pytest.fixture
def scenario() -> dict[str, object]:
    scenario_uuid = uuid4()
    target_date = date(
        2040 + (scenario_uuid.int % 20),
        ((scenario_uuid.int >> 8) % 12) + 1,
        ((scenario_uuid.int >> 16) % 28) + 1,
    )
    return {
        "id": scenario_uuid,
        "event_id": str(scenario_uuid),
        "user_id": f"test-user-{scenario_uuid}",
        "movie_id": f"movie-{scenario_uuid.hex[:8]}",
        "session_id": f"session-{scenario_uuid.hex}",
        "target_date": target_date,
    }


@pytest.fixture
def clickhouse_client():
    client = clickhouse_connect.get_client(host="clickhouse", port="8123", database="movie_analytics")
    try:
        yield client
    finally:
        client.close()


@pytest.fixture
def postgres_conn():
    with psycopg.connect("postgresql://movie:movie@postgres:5432/postgres") as conn:
        yield conn


@pytest.fixture
def s3_client():
    return boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
        config=Config(signature_version="s3v4"),
    )


def build_movie_event(scenario: dict[str, object], event_type: str = "VIEW_STARTED", progress_seconds: int = 0) -> dict[str, object]:
    target_date = scenario["target_date"]
    assert isinstance(target_date, date)
    event_id = scenario["event_id"] if event_type == "VIEW_STARTED" else str(uuid4())
    return {
        "event_id": event_id,
        "user_id": scenario["user_id"],
        "movie_id": scenario["movie_id"],
        "event_type": event_type,
        "timestamp": datetime(target_date.year, target_date.month, target_date.day, 12, 0, tzinfo=timezone.utc).isoformat(),
        "device_type": "DESKTOP",
        "session_id": scenario["session_id"],
        "progress_seconds": progress_seconds,
    }


def publish_event(payload: dict[str, object]) -> dict[str, object]:
    response = requests.post("http://producer:8000/events", json=payload, timeout=15)
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["event_id"] == payload["event_id"]
    assert body["status"] == "published"
    return body


def wait_for_clickhouse_event(client, event_id: str, timeout_seconds: int = 60) -> tuple | None:
    for _ in range(timeout_seconds):
        result = client.query(
            "SELECT event_id, user_id, movie_id, event_type, progress_seconds FROM movie_analytics.movie_events_raw WHERE event_id = %(event_id)s",
            parameters={"event_id": event_id},
        )
        if result.result_rows:
            return result.result_rows[0]
        time.sleep(1)
    return None


def cleanup_clickhouse(client, scenario: dict[str, object]) -> None:
    target_date = scenario["target_date"]
    assert isinstance(target_date, date)
    settings = {"mutations_sync": 2}
    client.command(
        "ALTER TABLE movie_analytics.movie_events_raw DELETE WHERE event_date = toDate(%(target_date)s) AND user_id = %(user_id)s",
        parameters={"target_date": target_date.isoformat(), "user_id": scenario["user_id"]},
        settings=settings,
    )
    client.command(
        "ALTER TABLE movie_analytics.agg_daily_metrics DELETE WHERE metric_date = toDate(%(target_date)s)",
        parameters={"target_date": target_date.isoformat()},
        settings=settings,
    )
    client.command(
        "ALTER TABLE movie_analytics.agg_top_movies_daily DELETE WHERE metric_date = toDate(%(target_date)s)",
        parameters={"target_date": target_date.isoformat()},
        settings=settings,
    )
    client.command(
        "ALTER TABLE movie_analytics.agg_retention_cohort_daily DELETE WHERE cohort_date = toDate(%(target_date)s)",
        parameters={"target_date": target_date.isoformat()},
        settings=settings,
    )


def cleanup_postgres(conn, target_date: date) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM metrics_daily WHERE metric_date = %s", (target_date,))
        cur.execute("DELETE FROM top_movies_daily WHERE metric_date = %s", (target_date,))
        cur.execute("DELETE FROM retention_cohort_daily WHERE cohort_date = %s", (target_date,))
    conn.commit()


def read_exported_json(s3_client, key: str) -> dict[str, object]:
    response = s3_client.get_object(Bucket="movie-analytics", Key=key)
    return json.loads(response["Body"].read().decode("utf-8"))
