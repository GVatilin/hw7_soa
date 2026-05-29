# Movie Analytics CI/CD, Testing and Observability

This repository contains a small movie analytics system:

- `producer`: FastAPI service that publishes movie events to Kafka.
- `aggregation-service`: FastAPI service that recomputes daily analytics from ClickHouse into PostgreSQL.
- `export-service`: FastAPI service that exports PostgreSQL aggregates to MinIO.
- Infrastructure: Kafka, Schema Registry, ClickHouse, PostgreSQL, MinIO, Prometheus, Alertmanager and Grafana.

## Run Locally

```bash
docker compose up -d
```

Useful URLs:

- Producer API: http://localhost:8000/docs
- Aggregation API: http://localhost:8001/docs
- Export API: http://localhost:8002/docs
- Prometheus: http://localhost:9090
- Alertmanager: http://localhost:9093
- Grafana: http://localhost:3000 (`admin` / `admin`)

Run integration and E2E tests:

```bash
docker compose run --rm tests
```

Run load test:

```bash
mkdir -p artifacts
docker compose run --rm k6
```

Check SLI thresholds from Prometheus:

```bash
python scripts/check_sli.py --prometheus http://localhost:9090 --output artifacts/sli-results.json
```

## CI Pipeline

The GitHub Actions pipeline is stored in `.github/workflows/ci.yml` and runs on every push and pull request.

Pipeline stages:

- Build Docker images for all application services and the test runner.
- Run unit tests for `producer`, `aggregation-service` and `export-service`.
- Start the full Docker Compose stack.
- Run integration and E2E tests through the real APIs, Kafka, ClickHouse and PostgreSQL.
- Run k6 load test with 10 VU for 30 seconds.
- Query Prometheus and fail the pipeline if system SLI failure thresholds are crossed.
- Upload load-test, SLI and dashboard artifacts.

## Metrics

Every FastAPI service exposes `/metrics` in Prometheus format.

Application metrics:

- `http_requests_total{method,endpoint,status}`: total API requests.
- `http_request_errors_total{method,endpoint,error_type}`: failed API requests.
- `http_request_duration_seconds{method,endpoint}`: request latency histogram.

Prometheus configuration is in `prometheus/prometheus.yml`.

## Grafana Dashboards

Dashboards are provisioned automatically from `grafana/dashboards`.

- `Service Observability`: throughput, p50/p95/p99 latency, error rate, availability and target health.
- `Infrastructure Observability`: Kafka brokers, Kafka consumer lag, PostgreSQL connections, transaction rate and cache hit ratio.
- `Movie Analytics Dashboard`: domain analytics from ClickHouse.

## Alerts

Prometheus alert rules are defined as code in `prometheus/alerts.yml`.

Configured alerts:

- `ServiceDown`: application target is not scrapeable for 30 seconds.
- `HighErrorRate`: more than 5% API errors for 1 minute.
- `HighLatency`: p95 API latency above 1 second for 1 minute.
- `KafkaConsumerLag`: ClickHouse Kafka consumer lag above 100 messages for 1 minute.

To demonstrate a firing alert:

```bash
docker compose stop producer
```

Wait about 30 seconds, then open Prometheus Alerts or Alertmanager. Start it again with:

```bash
docker compose start producer
```

## SLI and SLO

The system-level SLI checks are implemented in `scripts/check_sli.py` and use live Prometheus queries.

| SLI | PromQL | SLO | Failure threshold | Reasoning |
| --- | --- | --- | --- | --- |
| API availability | `sum(rate(http_requests_total{status!~"5.."}[2m])) / sum(rate(http_requests_total[2m]))` | `>= 99%` | `< 95%` | User-visible API should mostly accept requests; below 95% means the system is broken for demo traffic. |
| API error rate | `sum(rate(http_request_errors_total[2m])) / sum(rate(http_requests_total[2m]))` | `<= 1%` | `> 5%` | Occasional transient failures are tolerated, sustained 5% errors are not. |
| Producer p95 latency | `histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket{job="producer"}[2m])) by (le))` | `<= 2s` | `> 3s` | Producer waits for Kafka delivery, so the threshold allows local Docker overhead while still catching severe degradation. |

The CI job fails when any failure threshold is crossed.
