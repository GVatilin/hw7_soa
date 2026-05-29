from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

import boto3
import psycopg
from botocore.config import Config
from psycopg.rows import dict_row

from .config import settings

logger = logging.getLogger(__name__)


def _jsonable(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = []
    for row in rows:
        item = {}
        for key, value in row.items():
            item[key] = value.isoformat() if hasattr(value, "isoformat") else value
        normalized.append(item)
    return normalized


@dataclass(slots=True)
class ExportService:
    pg_conn: psycopg.Connection | None = None
    s3_client: Any | None = None

    def connect(self) -> None:
        self.pg_conn = psycopg.connect(settings.postgres_dsn, row_factory=dict_row, autocommit=True)
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint_url,
            aws_access_key_id=settings.s3_access_key_id,
            aws_secret_access_key=settings.s3_secret_access_key,
            region_name=settings.s3_region,
            config=Config(signature_version="s3v4"),
        )

    def close(self) -> None:
        if self.pg_conn:
            self.pg_conn.close()

    def export(self, target_date: date) -> dict[str, Any]:
        assert self.pg_conn is not None
        assert self.s3_client is not None
        metrics = self._fetch_metrics(target_date)
        top_movies = self._fetch_top_movies(target_date)
        cohorts = self._fetch_cohorts(target_date)
        payload = {
            "date": target_date.isoformat(),
            "computed_at": datetime.now(timezone.utc).isoformat(),
            "metrics": _jsonable(metrics),
            "top_movies": _jsonable(top_movies),
            "retention_cohort": _jsonable(cohorts),
        }
        key = f"{settings.s3_prefix}/{target_date.isoformat()}/aggregates.json"
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.s3_client.put_object(Bucket=settings.s3_bucket, Key=key, Body=body, ContentType="application/json")
        logger.info("export finished", extra={"key": key, "date": target_date.isoformat()})
        return {"bucket": settings.s3_bucket, "key": key, "bytes": len(body)}

    def _fetch_metrics(self, target_date: date) -> list[dict[str, Any]]:
        assert self.pg_conn is not None
        with self.pg_conn.cursor() as cur:
            cur.execute("SELECT metric_date, metric_name, dimension_key, dimension_value, value_numeric, computed_at FROM metrics_daily WHERE metric_date = %s ORDER BY metric_name", (target_date,))
            return list(cur.fetchall())

    def _fetch_top_movies(self, target_date: date) -> list[dict[str, Any]]:
        assert self.pg_conn is not None
        with self.pg_conn.cursor() as cur:
            cur.execute("SELECT metric_date, rank, movie_id, views, computed_at FROM top_movies_daily WHERE metric_date = %s ORDER BY rank", (target_date,))
            return list(cur.fetchall())

    def _fetch_cohorts(self, target_date: date) -> list[dict[str, Any]]:
        assert self.pg_conn is not None
        with self.pg_conn.cursor() as cur:
            cur.execute("SELECT cohort_date, day_number, users_returned, cohort_size, retention_rate, computed_at FROM retention_cohort_daily WHERE cohort_date = %s ORDER BY day_number", (target_date,))
            return list(cur.fetchall())
