from datetime import date, datetime, timezone

from app.service import AggregationService


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.queries = []

    def query(self, sql, parameters):
        self.queries.append((sql, parameters))
        return type(
            "Result",
            (),
            {"result_rows": [(date(2024, 1, 1), 1, "movie-1", 7, datetime(2024, 1, 1, tzinfo=timezone.utc))]},
        )()


def test_collect_top_movies_maps_clickhouse_rows() -> None:
    service = AggregationService(ch_client=FakeClickHouseClient())

    rows = service._collect_top_movies(date(2024, 1, 1), datetime(2024, 1, 1, tzinfo=timezone.utc))

    assert rows == [
        {
            "metric_date": date(2024, 1, 1),
            "rank": 1,
            "movie_id": "movie-1",
            "views": 8,
            "computed_at": datetime(2024, 1, 1, tzinfo=timezone.utc),
        }
    ]
