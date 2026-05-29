import os
from dataclasses import dataclass


@dataclass(slots=True)
class Settings:
    postgres_dsn: str = os.getenv("POSTGRES_DSN", "postgresql://movie:movie@postgres:5432/postgres")
    export_cron: str = os.getenv("EXPORT_CRON", "*/5 * * * *")
    s3_endpoint_url: str = os.getenv("S3_ENDPOINT_URL", "http://minio:9000")
    s3_access_key_id: str = os.getenv("S3_ACCESS_KEY_ID", "minioadmin")
    s3_secret_access_key: str = os.getenv("S3_SECRET_ACCESS_KEY", "minioadmin")
    s3_bucket: str = os.getenv("S3_BUCKET", "movie-analytics")
    s3_prefix: str = os.getenv("S3_PREFIX", "daily")
    s3_region: str = os.getenv("S3_REGION", "us-east-1")


settings = Settings()
