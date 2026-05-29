import time
from collections.abc import Awaitable, Callable

from fastapi import FastAPI, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from starlette.responses import Response as StarletteResponse


REQUESTS_TOTAL = Counter(
    "http_requests_total",
    "Total HTTP requests.",
    ["method", "endpoint", "status"],
)
REQUEST_ERRORS_TOTAL = Counter(
    "http_request_errors_total",
    "Total failed HTTP requests.",
    ["method", "endpoint", "error_type"],
)
REQUEST_DURATION_SECONDS = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds.",
    ["method", "endpoint"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
)


def _endpoint_for(request: Request) -> str:
    route = request.scope.get("route")
    return getattr(route, "path", request.url.path)


async def prometheus_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[StarletteResponse]],
) -> StarletteResponse:
    method = request.method
    endpoint = request.url.path
    status = "500"
    started = time.perf_counter()

    try:
        response = await call_next(request)
        status = str(response.status_code)
        endpoint = _endpoint_for(request)
        return response
    except Exception as exc:
        endpoint = _endpoint_for(request)
        REQUEST_ERRORS_TOTAL.labels(method, endpoint, type(exc).__name__).inc()
        raise
    finally:
        elapsed = time.perf_counter() - started
        REQUESTS_TOTAL.labels(method, endpoint, status).inc()
        REQUEST_DURATION_SECONDS.labels(method, endpoint).observe(elapsed)
        if status.startswith(("4", "5")):
            REQUEST_ERRORS_TOTAL.labels(method, endpoint, f"http_{status}").inc()


def install_metrics(app: FastAPI) -> None:
    app.middleware("http")(prometheus_middleware)

    @app.get("/metrics", include_in_schema=False)
    def metrics() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
