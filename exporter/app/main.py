import logging
from datetime import date, datetime, timezone

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, HTTPException, Query

from .config import settings
from .logging_config import configure_logging
from .metrics import install_metrics
from .service import ExportService

configure_logging()
logger = logging.getLogger(__name__)

app = FastAPI(title="export-service")
install_metrics(app)
service = ExportService()
scheduler = BackgroundScheduler(timezone="UTC")


def _scheduled_export() -> None:
    target_date = datetime.now(timezone.utc).date()
    service.export(target_date)


@app.on_event("startup")
def startup_event() -> None:
    service.connect()
    scheduler.add_job(_scheduled_export, CronTrigger.from_crontab(settings.export_cron), id="export", replace_existing=True)
    scheduler.start()
    logger.info("export scheduler started", extra={"cron": settings.export_cron})


@app.on_event("shutdown")
def shutdown_event() -> None:
    scheduler.shutdown(wait=False)
    service.close()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/export")
def export_date(target_date: date = Query(alias="date")) -> dict[str, object]:
    try:
        return service.export(target_date)
    except Exception as exc:
        logger.exception("manual export failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
