from __future__ import annotations

import argparse
import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import requests


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _sha1_text(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()


def _url(base_url: str, path: str) -> str:
    return f"{base_url.rstrip('/')}{path}"


def _safe_get_json(session: requests.Session, url: str, timeout: float) -> tuple[int | None, dict | None, str | None]:
    try:
        response = session.get(url, timeout=timeout)
    except requests.RequestException as exc:
        return None, None, str(exc)

    if response.status_code != 200:
        return response.status_code, None, response.text[:200]

    try:
        return response.status_code, response.json(), None
    except ValueError as exc:
        return response.status_code, None, f"json-parse-error: {exc}"


def _safe_get_text(session: requests.Session, url: str, timeout: float) -> tuple[int | None, str | None, str | None]:
    try:
        response = session.get(url, timeout=timeout)
    except requests.RequestException as exc:
        return None, None, str(exc)

    if response.status_code != 200:
        return response.status_code, None, response.text[:200]

    return response.status_code, response.text, None


def _capture_outage_timing(session: requests.Session, base_url: str, timeout: float, interval_seconds: float) -> dict:
    start = time.perf_counter()
    deadline = start + max(60.0, interval_seconds * 6)
    samples = []
    last_status = None

    while time.perf_counter() < deadline:
        status, _, error = _safe_get_text(session, _url(base_url, "/v2/lastupdate.txt"), timeout)
        samples.append({"status": status, "error": error, "at_seconds": round(time.perf_counter() - start, 3)})
        last_status = status
        if status == 503:
            break
        time.sleep(interval_seconds)

    return {
        "observed_503": last_status == 503,
        "elapsed_seconds": round(time.perf_counter() - start, 3),
        "samples": samples,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Capture vendor soak evidence to JSONL and summary files.")
    parser.add_argument("--base-url", default="http://localhost:18200")
    parser.add_argument("--minutes", type=int, default=30)
    parser.add_argument("--interval-seconds", type=float, default=5.0)
    parser.add_argument("--timeout-seconds", type=float, default=3.0)
    parser.add_argument("--output-dir", default="data/evidence")
    args = parser.parse_args()

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = Path(args.output_dir) / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    timeline_path = out_dir / "timeline.jsonl"
    summary_path = out_dir / "summary.json"
    notes_path = out_dir / "notes.md"

    total_samples = max(1, int((args.minutes * 60) / args.interval_seconds))
    manifest_last_sha = ""
    manifest_stale_count = 0
    health_errors = 0
    stats_errors = 0
    manifest_errors = 0
    dashboard_errors = 0
    outage_timing = None

    with requests.Session() as session, timeline_path.open("w", encoding="utf-8") as timeline:
        dashboard_status, dashboard_json, dashboard_error = _safe_get_json(
            session, _url(args.base_url, "/metrics"), args.timeout_seconds
        )
        if dashboard_status != 200:
            dashboard_errors += 1
        else:
            outage_timing = _capture_outage_timing(session, args.base_url, args.timeout_seconds, args.interval_seconds)

        for i in range(total_samples):
            at = _utc_now()
            health_status, health_json, health_error = _safe_get_json(
                session, _url(args.base_url, "/healthz"), args.timeout_seconds
            )
            stats_status, stats_json, stats_error = _safe_get_json(
                session, _url(args.base_url, "/stats"), args.timeout_seconds
            )
            manifest_status, manifest_text, manifest_error = _safe_get_text(
                session, _url(args.base_url, "/v2/lastupdate.txt"), args.timeout_seconds
            )

            if health_status != 200:
                health_errors += 1
            if stats_status != 200:
                stats_errors += 1
            if manifest_status != 200:
                manifest_errors += 1

            manifest_sha = _sha1_text(manifest_text) if manifest_text is not None else ""
            if manifest_sha and manifest_last_sha and manifest_sha == manifest_last_sha:
                manifest_stale_count += 1
            if manifest_sha:
                manifest_last_sha = manifest_sha

            record = {
                "at": at,
                "health": {
                    "status": health_status,
                    "error": health_error,
                    "ready": (health_json or {}).get("ready") if isinstance(health_json, dict) else None,
                    "simulated_now": (health_json or {}).get("simulated_now") if isinstance(health_json, dict) else None,
                },
                "stats": {
                    "status": stats_status,
                    "error": stats_error,
                    "payload": stats_json,
                },
                "dashboard": {
                    "status": dashboard_status,
                    "error": dashboard_error,
                    "payload": dashboard_json,
                },
                "manifest": {
                    "status": manifest_status,
                    "error": manifest_error,
                    "sha1": manifest_sha,
                    "line_count": len([line for line in (manifest_text or "").splitlines() if line.strip()]),
                },
            }
            timeline.write(json.dumps(record, sort_keys=True) + "\n")

            if i < total_samples - 1:
                time.sleep(args.interval_seconds)

    summary = {
        "run_id": run_id,
        "minutes": args.minutes,
        "interval_seconds": args.interval_seconds,
        "total_samples": total_samples,
        "health_errors": health_errors,
        "stats_errors": stats_errors,
        "manifest_errors": manifest_errors,
        "dashboard_errors": dashboard_errors,
        "manifest_stale_count": manifest_stale_count,
        "outage_timing": outage_timing,
        "artifacts": {
            "timeline": str(timeline_path),
            "summary": str(summary_path),
            "notes": str(notes_path),
        },
    }

    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    notes_path.write_text(
        "\n".join(
            [
                "# Soak Evidence Notes",
                "",
                f"- Run ID: {run_id}",
                f"- Duration minutes: {args.minutes}",
                f"- Interval seconds: {args.interval_seconds}",
                f"- Health errors: {health_errors}",
                f"- Stats errors: {stats_errors}",
                f"- Manifest errors: {manifest_errors}",
                f"- Dashboard errors: {dashboard_errors}",
                f"- Manifest stale samples: {manifest_stale_count}",
                f"- Outage timing sample: {json.dumps(outage_timing, sort_keys=True) if outage_timing is not None else 'n/a'}",
                "",
                "Add operator interpretation here: observed outage windows, lag trend behavior, and recovery details.",
            ]
        ),
        encoding="utf-8",
    )

    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
