import argparse
import json
import time
import urllib.parse
import urllib.request
from pathlib import Path


QUERIES = {
    "api_availability": {
        "promql": 'sum(rate(http_requests_total{job=~"producer|aggregation-service|export-service",status!~"5.."}[2m])) / clamp_min(sum(rate(http_requests_total{job=~"producer|aggregation-service|export-service"}[2m])), 0.001)',
        "slo": 0.99,
        "failure_threshold": 0.95,
        "comparison": ">=",
    },
    "api_error_rate": {
        "promql": 'sum(rate(http_request_errors_total{job=~"producer|aggregation-service|export-service"}[2m])) / clamp_min(sum(rate(http_requests_total{job=~"producer|aggregation-service|export-service"}[2m])), 0.001)',
        "slo": 0.01,
        "failure_threshold": 0.05,
        "comparison": "<=",
    },
    "producer_p95_latency_seconds": {
        "promql": 'histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket{job="producer"}[2m])) by (le))',
        "slo": 2.0,
        "failure_threshold": 3.0,
        "comparison": "<=",
    },
}


def query(prometheus_url: str, promql: str) -> float:
    params = urllib.parse.urlencode({"query": promql})
    url = f"{prometheus_url.rstrip('/')}/api/v1/query?{params}"
    with urllib.request.urlopen(url, timeout=15) as response:
        body = json.loads(response.read().decode("utf-8"))

    if body["status"] != "success":
        raise RuntimeError(body)

    result = body["data"]["result"]
    if not result:
        return 0.0
    return float(result[0]["value"][1])


def passes(value: float, threshold: float, comparison: str) -> bool:
    if comparison == ">=":
        return value >= threshold
    if comparison == "<=":
        return value <= threshold
    raise ValueError(f"Unsupported comparison: {comparison}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--prometheus", default="http://localhost:9090")
    parser.add_argument("--output", default="artifacts/sli-results.json")
    parser.add_argument("--settle-seconds", type=int, default=15)
    args = parser.parse_args()

    time.sleep(args.settle_seconds)
    results = {}
    failures = []

    for name, spec in QUERIES.items():
        value = query(args.prometheus, spec["promql"])
        ok = passes(value, spec["failure_threshold"], spec["comparison"])
        results[name] = {
            "value": value,
            "slo": spec["slo"],
            "failure_threshold": spec["failure_threshold"],
            "comparison": spec["comparison"],
            "promql": spec["promql"],
            "passed": ok,
        }
        if not ok:
            failures.append(name)

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(json.dumps(results, indent=2))

    if failures:
        raise SystemExit(f"SLI failure thresholds crossed: {', '.join(failures)}")


if __name__ == "__main__":
    main()
