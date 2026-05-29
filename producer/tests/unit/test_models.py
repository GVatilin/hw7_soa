from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from app.models import MovieEventIn


def test_view_event_serializes_timestamp_for_kafka() -> None:
    event = MovieEventIn(
        user_id="user-1",
        movie_id="movie-1",
        event_type="VIEW_STARTED",
        timestamp=datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),
        device_type="DESKTOP",
        session_id="session-1",
    )

    payload = event.to_kafka_dict()

    assert payload["event_type"] == "VIEW_STARTED"
    assert payload["timestamp"] == 1704110400000
    assert payload["movie_id"] == "movie-1"


def test_search_event_requires_query() -> None:
    with pytest.raises(ValidationError):
        MovieEventIn(
            user_id="user-1",
            event_type="SEARCHED",
            device_type="MOBILE",
            session_id="session-1",
        )
