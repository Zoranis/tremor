from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from urllib.request import urlopen


def _check_json(url: str) -> dict:
    with urlopen(url, timeout=5) as response:
        if response.status != 200:
            raise RuntimeError(f"unexpected status {response.status} for {url}")
        return json.loads(response.read().decode("utf-8"))


def _check_text(url: str) -> str:
    with urlopen(url, timeout=5) as response:
        if response.status != 200:
            raise RuntimeError(f"unexpected status {response.status} for {url}")
        return response.read().decode("utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate restart safety for the full Tremor stack.")
    parser.add_argument("--compose-service", default="dashboard")
    parser.add_argument("--vendor-url", default="http://localhost:18200/healthz")
    parser.add_argument("--dashboard-url", default="http://localhost:18600/metrics")
    args = parser.parse_args()

    baseline = _check_json(args.vendor_url)
    metrics_before = _check_json(args.dashboard_url)

    subprocess.run(["docker", "compose", "restart", args.compose_service], check=True)
    time.sleep(5)

    vendor_after = _check_json(args.vendor_url)
    metrics_after = _check_json(args.dashboard_url)

    report = {
        "vendor_before": baseline,
        "vendor_after": vendor_after,
        "metrics_before_present": bool(metrics_before),
        "metrics_after_present": bool(metrics_after),
        "dashboard_root": _check_text("http://localhost:18600/"),
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())