import time

import requests


SERVICES = {
    "producer": "http://producer:8000/metrics",
    "aggregation-service": "http://aggregation-service:8001/metrics",
    "export-service": "http://export-service:8002/metrics",
}


def test_services_export_required_prometheus_metrics() -> None:
    required = (
        "http_requests_total",
        "http_request_errors_total",
        "http_request_duration_seconds_bucket",
    )

    for name, url in SERVICES.items():
        response = requests.get(url, timeout=10)
        assert response.status_code == 200, f"{name} /metrics failed: {response.text}"
        body = response.text
        for metric in required:
            assert metric in body, f"{name} does not expose {metric}"


def test_prometheus_scrapes_application_targets() -> None:
    expected = {service: "up" for service in SERVICES}
    health_by_job = {}

    for _ in range(12):
        response = requests.get("http://prometheus:9090/api/v1/targets", timeout=10)
        assert response.status_code == 200, response.text

        targets = response.json()["data"]["activeTargets"]
        health_by_job = {
            label["job"]: target["health"]
            for target in targets
            for label in [target["labels"]]
            if label.get("job") in SERVICES
        }
        if health_by_job == expected:
            return
        time.sleep(5)

    assert health_by_job == expected
