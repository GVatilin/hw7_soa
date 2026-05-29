import logging
import random
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from .models import DeviceType, EventType, MovieEventIn

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class EventsGenerator:
    publisher: object
    running: bool = False
    _thread: threading.Thread | None = None

    def start(self) -> None:
        if self.running:
            return
        self.running = True
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        logger.info("generator started")

    def stop(self) -> None:
        self.running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3)

    def generate_batch(self, count: int) -> list[str]:
        event_ids: list[str] = []
        for _ in range(count):
            session_events = self._session_flow()
            for event in session_events:
                payload = event.to_kafka_dict()
                self.publisher.publish(payload, key=event.user_id)
                event_ids.append(str(event.event_id))
        return event_ids

    def _loop(self) -> None:
        while self.running:
            try:
                self.generate_batch(1)
            except Exception:
                logger.exception("generator failed")
            time.sleep(0.5)

    def _session_flow(self) -> list[MovieEventIn]:
        user_id = f"user-{random.randint(1, 200)}"
        movie_id = f"movie-{random.randint(1, 50)}"
        session_id = f"session-{uuid4()}"
        device_type = random.choice(list(DeviceType))
        base = datetime.now(timezone.utc)

        events = []
        progress = 0
        events.append(MovieEventIn(user_id=user_id, movie_id=movie_id, event_type=EventType.VIEW_STARTED, timestamp=base, device_type=device_type, session_id=session_id, progress_seconds=progress))

        if random.random() < 0.25:
            events.append(MovieEventIn(user_id=user_id, movie_id="", event_type=EventType.SEARCHED, timestamp=base + timedelta(seconds=1), device_type=device_type, session_id=f"search-{uuid4()}", progress_seconds=0, search_query=random.choice(["comedy", "drama", "thriller", "oscar winners"])))

        current_ts = base + timedelta(seconds=5)
        if random.random() < 0.6:
            progress += random.randint(60, 900)
            events.append(MovieEventIn(user_id=user_id, movie_id=movie_id, event_type=EventType.VIEW_PAUSED, timestamp=current_ts, device_type=device_type, session_id=session_id, progress_seconds=progress))
            current_ts += timedelta(seconds=random.randint(10, 60))
            progress += random.randint(20, 120)
            events.append(MovieEventIn(user_id=user_id, movie_id=movie_id, event_type=EventType.VIEW_RESUMED, timestamp=current_ts, device_type=device_type, session_id=session_id, progress_seconds=progress))
            current_ts += timedelta(seconds=random.randint(10, 60))

        if random.random() < 0.85:
            progress += random.randint(500, 7200)
            events.append(MovieEventIn(user_id=user_id, movie_id=movie_id, event_type=EventType.VIEW_FINISHED, timestamp=current_ts, device_type=device_type, session_id=session_id, progress_seconds=progress))
            current_ts += timedelta(seconds=1)

        if random.random() < 0.35:
            events.append(MovieEventIn(user_id=user_id, movie_id=movie_id, event_type=EventType.LIKED, timestamp=current_ts, device_type=device_type, session_id=session_id, progress_seconds=progress))
        return events
