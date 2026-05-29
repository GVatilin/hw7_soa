import logging
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from threading import Lock
from typing import Any

import clickhouse_connect
import psycopg
from psycopg.rows import dict_row

from .config import settings

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class AggregationService:
    ch_client: Any | None = None
    pg_conn: psycopg.Connection | None = None
    lock: Lock = field(default_factory=Lock)

    def connect(self) -> None:
        self.ch_client = clickhouse_connect.get_client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
            database=settings.clickhouse_database,
        )
        self.pg_conn = psycopg.connect(settings.postgres_dsn, row_factory=dict_row)
        self.pg_conn.autocommit = False
        self.ensure_storage()

    def close(self) -> None:
        if self.pg_conn:
            self.pg_conn.close()
        if self.ch_client:
            self.ch_client.close()

    def ensure_storage(self) -> None:
        assert self.ch_client is not None
        assert self.pg_conn is not None
        self.ch_client.command(
            """
            CREATE TABLE IF NOT EXISTS movie_analytics.agg_daily_metrics
            (
                metric_date Date,
                metric_name LowCardinality(String),
                dimension_key LowCardinality(String),
                dimension_value String,
                value Float64,
                computed_at DateTime64(3, 'UTC')
            )
            ENGINE = ReplacingMergeTree(computed_at)
            PARTITION BY toYYYYMM(metric_date)
            ORDER BY (metric_date, metric_name, dimension_key, dimension_value)
            """
        )
        self.ch_client.command(
            """
            CREATE TABLE IF NOT EXISTS movie_analytics.agg_top_movies_daily
            (
                metric_date Date,
                rank UInt8,
                movie_id String,
                views UInt64,
                computed_at DateTime64(3, 'UTC')
            )
            ENGINE = ReplacingMergeTree(computed_at)
            PARTITION BY toYYYYMM(metric_date)
            ORDER BY (metric_date, rank, movie_id)
            """
        )
        self.ch_client.command(
            """
            CREATE TABLE IF NOT EXISTS movie_analytics.agg_retention_cohort_daily
            (
                cohort_date Date,
                day_number UInt8,
                users_returned UInt64,
                cohort_size UInt64,
                retention_rate Float64,
                computed_at DateTime64(3, 'UTC')
            )
            ENGINE = ReplacingMergeTree(computed_at)
            PARTITION BY toYYYYMM(cohort_date)
            ORDER BY (cohort_date, day_number)
            """
        )
        with self.pg_conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS metrics_daily
                (
                    metric_date date NOT NULL,
                    metric_name text NOT NULL,
                    dimension_key text NOT NULL,
                    dimension_value text NOT NULL,
                    value_numeric double precision NOT NULL,
                    computed_at timestamptz NOT NULL,
                    PRIMARY KEY (metric_date, metric_name, dimension_key, dimension_value)
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS top_movies_daily
                (
                    metric_date date NOT NULL,
                    rank int NOT NULL,
                    movie_id text NOT NULL,
                    views bigint NOT NULL,
                    computed_at timestamptz NOT NULL,
                    PRIMARY KEY (metric_date, rank)
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS retention_cohort_daily
                (
                    cohort_date date NOT NULL,
                    day_number int NOT NULL,
                    users_returned bigint NOT NULL,
                    cohort_size bigint NOT NULL,
                    retention_rate double precision NOT NULL,
                    computed_at timestamptz NOT NULL,
                    PRIMARY KEY (cohort_date, day_number)
                )
                """
            )
        self.pg_conn.commit()

    def recompute(self, target_date: date) -> dict[str, Any]:
        assert self.ch_client is not None
        assert self.pg_conn is not None
        with self.lock:
            started = time.perf_counter()
            logger.info("aggregation cycle started", extra={"date": target_date.isoformat()})
            processed_rows = self._count_source_rows(target_date)
            computed_at = datetime.now(timezone.utc)
            self._delete_clickhouse_rows(target_date)
            metrics = self._collect_metrics(target_date, computed_at)
            top_movies = self._collect_top_movies(target_date, computed_at)
            cohorts = self._collect_retention_heatmap(target_date, computed_at)
            self._insert_clickhouse_metrics(metrics)
            self._insert_clickhouse_top_movies(top_movies)
            self._insert_clickhouse_cohorts(cohorts)
            self._upsert_postgres_metrics(metrics)
            self._upsert_postgres_top_movies(top_movies)
            self._upsert_postgres_cohorts(cohorts)
            self.pg_conn.commit()
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            logger.info("aggregation cycle finished", extra={"date": target_date.isoformat(), "processed_rows": processed_rows, "elapsed_ms": elapsed_ms})
            return {
                "date": target_date.isoformat(),
                "processed_rows": processed_rows,
                "metrics_rows": len(metrics),
                "top_movies_rows": len(top_movies),
                "cohort_rows": len(cohorts),
                "elapsed_ms": elapsed_ms,
            }

    def _count_source_rows(self, target_date: date) -> int:
        result = self.ch_client.query(
            "SELECT count() AS cnt FROM movie_analytics.movie_events_raw WHERE event_date = %(target_date)s",
            parameters={"target_date": target_date.isoformat()},
        )
        return int(result.first_row[0]) if result.first_row else 0

    def _delete_clickhouse_rows(self, target_date: date) -> None:
        settings_sync = {"mutations_sync": 2}
        self.ch_client.command("ALTER TABLE movie_analytics.agg_daily_metrics DELETE WHERE metric_date = toDate(%(target_date)s)", parameters={"target_date": target_date.isoformat()}, settings=settings_sync)
        self.ch_client.command("ALTER TABLE movie_analytics.agg_top_movies_daily DELETE WHERE metric_date = toDate(%(target_date)s)", parameters={"target_date": target_date.isoformat()}, settings=settings_sync)
        self.ch_client.command("ALTER TABLE movie_analytics.agg_retention_cohort_daily DELETE WHERE cohort_date = toDate(%(target_date)s)", parameters={"target_date": target_date.isoformat()}, settings=settings_sync)

    def _collect_metrics(self, target_date: date, computed_at: datetime) -> list[dict[str, Any]]:
        sql = """
        WITH
            toDate(%(target_date)s) AS target_date,
            first_view AS (
                SELECT user_id, min(toDate(timestamp)) AS first_view_date
                FROM movie_analytics.movie_events_raw
                WHERE event_type = 'VIEW_STARTED'
                GROUP BY user_id
            ),
            next_day_users AS (
                SELECT DISTINCT fv.user_id
                FROM first_view fv
                INNER JOIN movie_analytics.movie_events_raw mer ON mer.user_id = fv.user_id
                WHERE fv.first_view_date = target_date
                  AND toDate(mer.timestamp) = addDays(fv.first_view_date, 1)
            ),
            day7_users AS (
                SELECT DISTINCT fv.user_id
                FROM first_view fv
                INNER JOIN movie_analytics.movie_events_raw mer ON mer.user_id = fv.user_id
                WHERE fv.first_view_date = target_date
                  AND toDate(mer.timestamp) = addDays(fv.first_view_date, 7)
            )
        SELECT * FROM
        (
            SELECT target_date AS metric_date, 'DAU' AS metric_name, 'scope' AS dimension_key, 'all' AS dimension_value,
                   toFloat64(uniqExact(user_id)) AS value, %(computed_at)s AS computed_at
            FROM movie_analytics.movie_events_raw
            WHERE event_date = target_date
            UNION ALL
            SELECT target_date, 'AVG_FINISH_PROGRESS', 'scope', 'all',
                   ifNull(avgIf(toFloat64(progress_seconds), event_type = 'VIEW_FINISHED'), 0), %(computed_at)s
            FROM movie_analytics.movie_events_raw
            WHERE event_date = target_date
            UNION ALL
            SELECT target_date, 'VIEW_CONVERSION', 'scope', 'all',
                   if(countIf(event_type = 'VIEW_STARTED') = 0, 0, toFloat64(countIf(event_type = 'VIEW_FINISHED')) / countIf(event_type = 'VIEW_STARTED')),
                   %(computed_at)s
            FROM movie_analytics.movie_events_raw
            WHERE event_date = target_date
            UNION ALL
            SELECT target_date, 'RETENTION_D1', 'scope', 'all',
                   if((SELECT count() FROM first_view WHERE first_view_date = target_date) = 0, 0,
                      toFloat64((SELECT count() FROM next_day_users)) / (SELECT count() FROM first_view WHERE first_view_date = target_date)),
                   %(computed_at)s
            UNION ALL
            SELECT target_date, 'RETENTION_D7', 'scope', 'all',
                   if((SELECT count() FROM first_view WHERE first_view_date = target_date) = 0, 0,
                      toFloat64((SELECT count() FROM day7_users)) / (SELECT count() FROM first_view WHERE first_view_date = target_date)),
                   %(computed_at)s
        )
        ORDER BY metric_name
        """
        result = self.ch_client.query(sql, parameters={"target_date": target_date.isoformat(), "computed_at": computed_at})
        return [{"metric_date": row[0], "metric_name": row[1], "dimension_key": row[2], "dimension_value": row[3], "value": float(row[4]), "computed_at": computed_at} for row in result.result_rows]

    def _collect_top_movies(self, target_date: date, computed_at: datetime) -> list[dict[str, Any]]:
        sql = """
        SELECT toDate(%(target_date)s) AS metric_date,
               row_number() OVER (ORDER BY views DESC, movie_id ASC) AS rank,
               movie_id,
               views,
               %(computed_at)s AS computed_at
        FROM (
            SELECT movie_id, countIf(event_type = 'VIEW_STARTED') AS views
            FROM movie_analytics.movie_events_raw
            WHERE event_date = toDate(%(target_date)s) AND movie_id != ''
            GROUP BY movie_id
            ORDER BY views DESC, movie_id ASC
            LIMIT 10
        )
        ORDER BY rank
        """
        result = self.ch_client.query(sql, parameters={"target_date": target_date.isoformat(), "computed_at": computed_at})
        return [{"metric_date": row[0], "rank": int(row[1]), "movie_id": row[2], "views": int(row[3]), "computed_at": computed_at} for row in result.result_rows]

    def _collect_retention_heatmap(self, target_date: date, computed_at: datetime) -> list[dict[str, Any]]:
        sql = """
        WITH
            first_view AS (
                SELECT user_id, min(toDate(timestamp)) AS cohort_date
                FROM movie_analytics.movie_events_raw
                WHERE event_type = 'VIEW_STARTED'
                GROUP BY user_id
            ),
            activity AS (
                SELECT DISTINCT user_id, toDate(timestamp) AS active_date
                FROM movie_analytics.movie_events_raw
            ),
            cohort_size AS (
                SELECT count() AS cohort_size
                FROM first_view
                WHERE cohort_date = toDate(%(target_date)s)
            )
        SELECT toDate(%(target_date)s) AS cohort_date,
               dateDiff('day', fv.cohort_date, a.active_date) AS day_number,
               uniqExact(a.user_id) AS users_returned,
               (SELECT cohort_size FROM cohort_size) AS cohort_size,
               if((SELECT cohort_size FROM cohort_size) = 0, 0, toFloat64(uniqExact(a.user_id)) / (SELECT cohort_size FROM cohort_size)) AS retention_rate,
               %(computed_at)s AS computed_at
        FROM first_view fv
        INNER JOIN activity a ON fv.user_id = a.user_id
        WHERE fv.cohort_date = toDate(%(target_date)s)
          AND dateDiff('day', fv.cohort_date, a.active_date) BETWEEN 0 AND 7
        GROUP BY cohort_date, day_number, computed_at
        ORDER BY day_number
        """
        result = self.ch_client.query(sql, parameters={"target_date": target_date.isoformat(), "computed_at": computed_at})
        return [{"cohort_date": row[0], "day_number": int(row[1]), "users_returned": int(row[2]), "cohort_size": int(row[3]), "retention_rate": float(row[4]), "computed_at": computed_at} for row in result.result_rows]

    def _insert_clickhouse_metrics(self, metrics: list[dict[str, Any]]) -> None:
        if not metrics:
            return
        self.ch_client.insert("movie_analytics.agg_daily_metrics", [[m["metric_date"], m["metric_name"], m["dimension_key"], m["dimension_value"], m["value"], m["computed_at"]] for m in metrics], column_names=["metric_date", "metric_name", "dimension_key", "dimension_value", "value", "computed_at"])

    def _insert_clickhouse_top_movies(self, top_movies: list[dict[str, Any]]) -> None:
        if not top_movies:
            return
        self.ch_client.insert("movie_analytics.agg_top_movies_daily", [[r["metric_date"], r["rank"], r["movie_id"], r["views"], r["computed_at"]] for r in top_movies], column_names=["metric_date", "rank", "movie_id", "views", "computed_at"])

    def _insert_clickhouse_cohorts(self, cohorts: list[dict[str, Any]]) -> None:
        if not cohorts:
            return
        self.ch_client.insert("movie_analytics.agg_retention_cohort_daily", [[r["cohort_date"], r["day_number"], r["users_returned"], r["cohort_size"], r["retention_rate"], r["computed_at"]] for r in cohorts], column_names=["cohort_date", "day_number", "users_returned", "cohort_size", "retention_rate", "computed_at"])

    def _upsert_postgres_metrics(self, metrics: list[dict[str, Any]]) -> None:
        assert self.pg_conn is not None
        with self.pg_conn.cursor() as cur:
            for row in metrics:
                cur.execute("""
                    INSERT INTO metrics_daily (metric_date, metric_name, dimension_key, dimension_value, value_numeric, computed_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (metric_date, metric_name, dimension_key, dimension_value)
                    DO UPDATE SET value_numeric = EXCLUDED.value_numeric, computed_at = EXCLUDED.computed_at
                """, (row["metric_date"], row["metric_name"], row["dimension_key"], row["dimension_value"], row["value"], row["computed_at"]))

    def _upsert_postgres_top_movies(self, top_movies: list[dict[str, Any]]) -> None:
        assert self.pg_conn is not None
        with self.pg_conn.cursor() as cur:
            for row in top_movies:
                cur.execute("""
                    INSERT INTO top_movies_daily (metric_date, rank, movie_id, views, computed_at)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (metric_date, rank)
                    DO UPDATE SET movie_id = EXCLUDED.movie_id, views = EXCLUDED.views, computed_at = EXCLUDED.computed_at
                """, (row["metric_date"], row["rank"], row["movie_id"], row["views"], row["computed_at"]))

    def _upsert_postgres_cohorts(self, cohorts: list[dict[str, Any]]) -> None:
        assert self.pg_conn is not None
        with self.pg_conn.cursor() as cur:
            for row in cohorts:
                cur.execute("""
                    INSERT INTO retention_cohort_daily (cohort_date, day_number, users_returned, cohort_size, retention_rate, computed_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (cohort_date, day_number)
                    DO UPDATE SET users_returned = EXCLUDED.users_returned,
                                  cohort_size = EXCLUDED.cohort_size,
                                  retention_rate = EXCLUDED.retention_rate,
                                  computed_at = EXCLUDED.computed_at
                """, (row["cohort_date"], row["day_number"], row["users_returned"], row["cohort_size"], row["retention_rate"], row["computed_at"]))
