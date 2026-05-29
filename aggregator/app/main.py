import logging
from datetime import date, datetime, timezone

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, HTTPException, Query

from .config import settings
from .logging_config import configure_logging
from .metrics import install_metrics
from .service import AggregationService

configure_logging()
logger = logging.getLogger(__name__)

app = FastAPI(title="aggregation-service")
install_metrics(app)
service = AggregationService()
scheduler = BackgroundScheduler(timezone="UTC")


def _scheduled_run() -> None:
    target_date = datetime.now(timezone.utc).date()
    service.recompute(target_date)


@app.on_event("startup")
def startup_event() -> None:
    service.connect()
    scheduler.add_job(_scheduled_run, CronTrigger.from_crontab(settings.aggregation_cron), id="aggregation", replace_existing=True)
    scheduler.start()
    logger.info("aggregation scheduler started", extra={"cron": settings.aggregation_cron})


@app.on_event("shutdown")
def shutdown_event() -> None:
    scheduler.shutdown(wait=False)
    service.close()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/recompute")
def recompute(target_date: date = Query(alias="date")) -> dict[str, object]:
    try:
        return service.recompute(target_date)
    except Exception as exc:
        logger.exception("manual recompute failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
