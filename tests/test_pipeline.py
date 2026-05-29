import time
from datetime import date, datetime, timezone
from uuid import uuid4

import clickhouse_connect
import psycopg
import requests


def _get_client():
    return clickhouse_connect.get_client(host="clickhouse", port="8123", database="movie_analytics")


def _get_pg_conn():
    return psycopg.connect("postgresql://movie:movie@postgres:5432/postgres")


def test_event_flows_from_api_to_clickhouse_and_postgres_aggregate() -> None:
    event_uuid = uuid4()
    event_id = str(event_uuid)
    target_date = date(2040, 1, (event_uuid.int % 28) + 1)
    payload = {
        "event_id": event_id,
        "user_id": f"test-user-{uuid4()}",
        "movie_id": "movie-999",
        "event_type": "VIEW_STARTED",
        "timestamp": datetime(target_date.year, target_date.month, target_date.day, 12, 0, tzinfo=timezone.utc).isoformat(),
        "device_type": "DESKTOP",
        "session_id": f"session-{uuid4()}",
        "progress_seconds": 0,
    }

    response = requests.post("http://producer:8000/events", json=payload, timeout=15)
    assert response.status_code == 200, response.text

    client = _get_client()
    try:
        row = None
        for _ in range(60):
            result = client.query(
                "SELECT event_id, user_id, movie_id, event_type, progress_seconds FROM movie_analytics.movie_events_raw WHERE event_id = %(event_id)s",
                parameters={"event_id": event_id},
            )
            if result.result_rows:
                row = result.result_rows[0]
                break
            time.sleep(1)
        assert row is not None, "event did not arrive in ClickHouse"
        assert row[0] == event_id
        assert row[2] == "movie-999"
        assert row[3] == "VIEW_STARTED"
        assert row[4] == 0

        recompute = requests.post(f"http://aggregation-service:8001/recompute?date={target_date.isoformat()}", timeout=60)
        assert recompute.status_code == 200, recompute.text
        body = recompute.json()
        assert body["date"] == target_date.isoformat()
        assert body["processed_rows"] >= 1

        with _get_pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT value_numeric
                    FROM metrics_daily
                    WHERE metric_date = %s
                      AND metric_name = 'DAU'
                      AND dimension_key = 'scope'
                      AND dimension_value = 'all'
                    """,
                    (target_date,),
                )
                metric = cur.fetchone()
        assert metric is not None, "DAU aggregate was not written to PostgreSQL"
        assert metric[0] >= 1
    finally:
        client.command(
            "ALTER TABLE movie_analytics.movie_events_raw DELETE WHERE event_id = %(event_id)s",
            parameters={"event_id": event_id},
            settings={"mutations_sync": 2},
        )
        with _get_pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM metrics_daily WHERE metric_date = %s", (target_date,))
                cur.execute("DELETE FROM top_movies_daily WHERE metric_date = %s", (target_date,))
                cur.execute("DELETE FROM retention_cohort_daily WHERE cohort_date = %s", (target_date,))
        client.close()
