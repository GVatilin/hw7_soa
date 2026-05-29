from datetime import date, datetime, timezone

from app.service import _jsonable


def test_jsonable_converts_dates_and_datetimes() -> None:
    rows = [{"metric_date": date(2024, 1, 1), "computed_at": datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc), "value": 3}]

    assert _jsonable(rows) == [{"metric_date": "2024-01-01", "computed_at": "2024-01-01T12:00:00+00:00", "value": 3}]
