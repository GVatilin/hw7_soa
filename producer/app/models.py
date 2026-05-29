from datetime import datetime, timezone
from enum import Enum
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, model_validator


class EventType(str, Enum):
    VIEW_STARTED = "VIEW_STARTED"
    VIEW_FINISHED = "VIEW_FINISHED"
    VIEW_PAUSED = "VIEW_PAUSED"
    VIEW_RESUMED = "VIEW_RESUMED"
    LIKED = "LIKED"
    SEARCHED = "SEARCHED"


class DeviceType(str, Enum):
    MOBILE = "MOBILE"
    DESKTOP = "DESKTOP"
    TV = "TV"
    TABLET = "TABLET"


VIEW_EVENTS = {
    EventType.VIEW_STARTED,
    EventType.VIEW_FINISHED,
    EventType.VIEW_PAUSED,
    EventType.VIEW_RESUMED,
}


class MovieEventIn(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    user_id: str
    movie_id: str = ""
    event_type: EventType
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    device_type: DeviceType
    session_id: str
    progress_seconds: int = 0
    search_query: str | None = None

    @model_validator(mode="after")
    def validate_semantics(self) -> "MovieEventIn":
        if self.event_type == EventType.SEARCHED and not self.search_query:
            raise ValueError("search_query is required for SEARCHED events")
        if self.event_type in VIEW_EVENTS and not self.movie_id:
            raise ValueError("movie_id is required for view events")
        if self.progress_seconds < 0:
            raise ValueError("progress_seconds must be >= 0")
        return self

    def to_kafka_dict(self) -> dict[str, object]:
        ts = self.timestamp.astimezone(timezone.utc)
        return {
            "event_id": str(self.event_id),
            "user_id": self.user_id,
            "movie_id": self.movie_id,
            "event_type": self.event_type.value,
            "timestamp": int(ts.timestamp() * 1000),
            "device_type": self.device_type.value,
            "session_id": self.session_id,
            "progress_seconds": self.progress_seconds,
            "search_query": self.search_query,
        }
