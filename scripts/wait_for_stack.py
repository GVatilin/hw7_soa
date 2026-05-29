import json
import time
import urllib.error
import urllib.request


CHECKS = {
    "producer": "http://localhost:8000/health",
    "aggregation-service": "http://localhost:8001/health",
    "export-service": "http://localhost:8002/health",
    "prometheus": "http://localhost:9090/-/ready",
    "grafana": "http://localhost:3000/api/health",
    "alertmanager": "http://localhost:9093/-/ready",
}


def _healthy(name: str, url: str) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            return 200 <= response.status < 300
    except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as exc:
        print(f"{name} not ready yet: {type(exc).__name__}: {exc}")
        return False


def main() -> None:
    deadline = time.time() + 240
    pending = dict(CHECKS)

    while pending and time.time() < deadline:
        for name, url in list(pending.items()):
            if _healthy(name, url):
                print(f"{name} is ready")
                pending.pop(name)

        if pending:
            print(f"waiting for: {', '.join(sorted(pending))}")
            time.sleep(5)

    if pending:
        raise SystemExit(
            f"stack did not become healthy: {json.dumps(pending, indent=2)}"
        )


if __name__ == "__main__":
    main()