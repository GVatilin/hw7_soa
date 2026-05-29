import logging
from fastapi import FastAPI, HTTPException, Query

from .generator import EventsGenerator
from .kafka_producer import KafkaEventProducer
from .logging_config import configure_logging
from .metrics import install_metrics
from .models import MovieEventIn

configure_logging()
logger = logging.getLogger(__name__)

app = FastAPI(title="movie-service")
install_metrics(app)
producer = KafkaEventProducer()
generator = EventsGenerator(producer)


@app.on_event("startup")
def startup_event() -> None:
    producer.bootstrap()


@app.on_event("shutdown")
def shutdown_event() -> None:
    generator.stop()
    producer.close()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/events")
def publish_event(event: MovieEventIn) -> dict[str, str]:
    try:
        payload = event.to_kafka_dict()
        producer.publish(payload, key=event.user_id)
        return {"event_id": str(event.event_id), "status": "published"}
    except Exception as exc:
        logger.exception("failed to publish event")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/generator/start")
def start_generator() -> dict[str, str]:
    generator.start()
    return {"status": "started" if generator.running else "already_running"}


@app.post("/generator/stop")
def stop_generator() -> dict[str, str]:
    generator.stop()
    return {"status": "stopped"}


@app.get("/generator/status")
def generator_status() -> dict[str, bool]:
    return {"running": generator.running}


@app.post("/generate")
def generate_events(count: int = Query(default=10, ge=1, le=1000)) -> dict[str, object]:
    try:
        event_ids = generator.generate_batch(count)
        return {"generated_sessions": count, "published_events": len(event_ids), "event_ids": event_ids}
    except Exception as exc:
        logger.exception("failed to generate events")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
