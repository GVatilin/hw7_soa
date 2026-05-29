import os
from dataclasses import dataclass


@dataclass(slots=True)
class Settings:
    clickhouse_host: str = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    clickhouse_port: int = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    clickhouse_database: str = os.getenv("CLICKHOUSE_DATABASE", "movie_analytics")
    clickhouse_user: str = os.getenv("CLICKHOUSE_USER", "default")
    clickhouse_password: str = os.getenv("CLICKHOUSE_PASSWORD", "")
    postgres_dsn: str = os.getenv("POSTGRES_DSN", "postgresql://movie:movie@postgres:5432/movie_analytics")
    aggregation_cron: str = os.getenv("AGGREGATION_CRON", "*/2 * * * *")


settings = Settings()
