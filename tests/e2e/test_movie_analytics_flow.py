import requests

from tests.helpers import (
    build_movie_event,
    cleanup_clickhouse,
    cleanup_postgres,
    publish_event,
    read_exported_json,
    wait_for_clickhouse_event,
)


def test_movie_event_is_aggregated_and_exported(clickhouse_client, postgres_conn, s3_client, scenario) -> None:
    start_event = build_movie_event(scenario, event_type="VIEW_STARTED", progress_seconds=0)
    finish_event = build_movie_event(scenario, event_type="VIEW_FINISHED", progress_seconds=1200)
    target_date = scenario["target_date"]
    export_key = f"daily/{target_date.isoformat()}/aggregates.json"

    try:
        publish_event(start_event)
        publish_event(finish_event)
        assert wait_for_clickhouse_event(clickhouse_client, start_event["event_id"]) is not None
        assert wait_for_clickhouse_event(clickhouse_client, finish_event["event_id"]) is not None

        recompute = requests.post(f"http://aggregation-service:8001/recompute?date={target_date.isoformat()}", timeout=60)
        assert recompute.status_code == 200, recompute.text
        recompute_body = recompute.json()
        assert recompute_body["date"] == target_date.isoformat()
        assert recompute_body["processed_rows"] >= 2
        assert recompute_body["metrics_rows"] >= 1

        with postgres_conn.cursor() as cur:
            cur.execute(
                """
                SELECT value_numeric
                FROM metrics_daily
                WHERE metric_date = %s
                  AND metric_name = 'VIEW_CONVERSION'
                  AND dimension_key = 'scope'
                  AND dimension_value = 'all'
                """,
                (target_date,),
            )
            conversion = cur.fetchone()
        assert conversion is not None, "VIEW_CONVERSION aggregate was not written to PostgreSQL"
        assert conversion[0] >= 1

        export = requests.post(f"http://export-service:8002/export?date={target_date.isoformat()}", timeout=60)
        assert export.status_code == 200, export.text
        export_body = export.json()
        assert export_body["bucket"] == "movie-analytics"
        assert export_body["key"] == export_key
        assert export_body["bytes"] > 0

        exported = read_exported_json(s3_client, export_key)
        assert exported["date"] == target_date.isoformat()
        assert any(metric["metric_name"] == "VIEW_CONVERSION" for metric in exported["metrics"])
        assert any(movie["movie_id"] == scenario["movie_id"] for movie in exported["top_movies"])
    finally:
        s3_client.delete_object(Bucket="movie-analytics", Key=export_key)
        cleanup_postgres(postgres_conn, target_date)
        cleanup_clickhouse(clickhouse_client, scenario)
