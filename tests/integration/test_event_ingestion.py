from tests.helpers import build_movie_event, cleanup_clickhouse, publish_event, wait_for_clickhouse_event


def test_producer_publishes_event_to_kafka_and_clickhouse(clickhouse_client, scenario) -> None:
    payload = build_movie_event(scenario)

    try:
        publish_event(payload)
        row = wait_for_clickhouse_event(clickhouse_client, payload["event_id"])

        assert row is not None, "event did not arrive in ClickHouse"
        assert row[0] == payload["event_id"]
        assert row[1] == payload["user_id"]
        assert row[2] == payload["movie_id"]
        assert row[3] == "VIEW_STARTED"
        assert row[4] == 0
    finally:
        cleanup_clickhouse(clickhouse_client, scenario)
