"""Poll the vendor manifest, fetch+verify+parse slice files, and persist results.

This module owns orchestration only: deciding which manifest/backfill
entries to fetch and in what order, tracking dedup/alert state across polls,
and CLI wiring. The mechanics live in sibling modules:
  * `manifest.py`      — manifest line/timestamp parsing
  * `http_client.py`   — retrying HTTP fetch + payload verification
  * `alerts.py`         — poll-metric logging + alert fire/clear transitions
  * `poller_logging.py` — shared structured-logging helpers
"""
from __future__ import annotations

import argparse
import logging
import time
from dataclasses import dataclass
from typing import Callable, Iterable

import requests

from . import poller_logging as _log
from .alerts import _emit_alert_events, _log_poll_metrics
from .http_client import (
    TRANSIENT_HTTP_STATUSES,
    _download_and_verify,
    _fetch_simulated_now,
    _get_with_retry,
    _parse_zip_csv,
    _sha1_bytes,
)
from .manifest import (
    EXPECTED_FILE_TYPES,
    ManifestEntry,
    _expected_slice_sequence,
    _parse_timestamp,
    parse_manifest,
)
from .poller_logging import TelemetryHook, _call_hook


# Worst case under default settings (60s prod poll vs. 0.45s/slice replay) is
# ~133 slices between polls; this leaves headroom while still bounding a
# single poll's backfill burst if the gap is much larger (e.g. long downtime).
DEFAULT_MAX_BACKFILL_SLICES_PER_POLL = 300


@dataclass(frozen=True)
class PollResult:
    entry: ManifestEntry
    rows: list[dict[str, str]]


PersistCallback = Callable[[PollResult], None]


def _process_entry(
    session: requests.Session,
    entry: ManifestEntry,
    seen: set[tuple[str, str]],
    results: list[PollResult],
    logger: logging.Logger,
    *,
    timeout_seconds: float,
    max_retries: int,
    backoff_initial_seconds: float,
    backoff_max_seconds: float,
    telemetry_hooks: dict[str, TelemetryHook] | None = None,
) -> None:
    """Download, verify, and parse one manifest or backfill entry.

    Shared by the current-manifest loop and the gap-backfill loop in
    poll_once — both just need "fetch this (slice_ts, file_type), skip if
    already seen, append to results on success."
    """
    key = (entry.slice_ts, entry.file_type)
    if key in seen:
        _log.log_event(
            logger,
            action="file_process",
            result="skip_seen",
            slice_ts=entry.slice_ts,
            file_type=entry.file_type,
        )
        return

    zip_bytes = _download_and_verify(
        session,
        entry,
        logger,
        timeout_seconds,
        max_retries=max_retries,
        backoff_initial_seconds=backoff_initial_seconds,
        backoff_max_seconds=backoff_max_seconds,
        telemetry_hooks=telemetry_hooks,
    )
    if zip_bytes is None:
        return

    rows = _parse_zip_csv(
        zip_bytes,
        slice_ts=entry.slice_ts,
        file_type=entry.file_type,
        logger=logger,
    )
    if rows is None:
        return

    seen.add(key)
    results.append(PollResult(entry=entry, rows=rows))
    _log.log_event(
        logger,
        action="file_process",
        result="parsed_rows",
        slice_ts=entry.slice_ts,
        file_type=entry.file_type,
        row_count=len(rows),
    )


def _handle_manifest_poll_failure(
    logger: logging.Logger,
    *,
    result: str,
    manifest_url: str,
    manifest_latency_ms: float,
    status_code: int | None,
    alert_manifest_error_threshold: int,
    alert_lag_seconds_threshold: float,
    alert_state: dict[str, object] | None,
    telemetry_hooks: dict[str, TelemetryHook] | None,
) -> list[PollResult]:
    """Log, record telemetry for, and alert on a failed manifest poll.

    Covers all three ways `poll_once` can fail to get a usable manifest:
    no response, a 503 outage, and any other non-200 status. They all
    degrade the same way — zero results, one error tallied, vendor_outage
    marked active — differing only in the log line's `result` field.
    """
    if result != "request_error":
        _log.log_event(
            logger,
            action="manifest_poll",
            result=result,
            level=logging.WARNING,
            url=manifest_url,
            status=status_code,
        )
    _call_hook(
        telemetry_hooks,
        "record_manifest_poll",
        status_code=status_code,
        success=False,
        latency_ms=manifest_latency_ms,
        line_count=0,
        manifest_latest_slice=None,
    )
    _call_hook(telemetry_hooks, "set_degraded_state", degraded_type="vendor_outage", active=True)
    _log_poll_metrics(
        logger,
        manifest_poll_success_count=0,
        manifest_poll_error_count=1,
        processed_file_count=0,
        latest_processed_slice_by_file_type={},
        latest_lag_seconds_by_file_type={},
    )
    _emit_alert_events(
        logger,
        manifest_poll_success_count=0,
        manifest_poll_error_count=1,
        latest_lag_seconds_by_file_type={},
        alert_manifest_error_threshold=alert_manifest_error_threshold,
        alert_lag_seconds_threshold=alert_lag_seconds_threshold,
        alert_state=alert_state,
        telemetry_hooks=telemetry_hooks,
    )
    return []


def poll_once(
    session: requests.Session,
    *,
    base_url: str,
    seen: set[tuple[str, str]],
    logger: logging.Logger,
    timeout_seconds: float,
    max_retries: int = 2,
    backoff_initial_seconds: float = 0.2,
    backoff_max_seconds: float = 2.0,
    alert_manifest_error_threshold: int = 3,
    alert_lag_seconds_threshold: float = 1800.0,
    max_backfill_slices_per_poll: int = DEFAULT_MAX_BACKFILL_SLICES_PER_POLL,
    alert_state: dict[str, object] | None = None,
    telemetry_hooks: dict[str, TelemetryHook] | None = None,
) -> list[PollResult]:
    manifest_url = f"{base_url.rstrip('/')}/v2/lastupdate.txt"
    manifest_started = time.perf_counter()
    response = _get_with_retry(
        session,
        url=manifest_url,
        timeout_seconds=timeout_seconds,
        logger=logger,
        action="manifest_poll",
        max_retries=max_retries,
        backoff_initial_seconds=backoff_initial_seconds,
        backoff_max_seconds=backoff_max_seconds,
        retry_statuses=TRANSIENT_HTTP_STATUSES,
    )
    manifest_latency_ms = (time.perf_counter() - manifest_started) * 1000.0

    if response is None:
        return _handle_manifest_poll_failure(
            logger,
            result="request_error",
            manifest_url=manifest_url,
            manifest_latency_ms=manifest_latency_ms,
            status_code=None,
            alert_manifest_error_threshold=alert_manifest_error_threshold,
            alert_lag_seconds_threshold=alert_lag_seconds_threshold,
            alert_state=alert_state,
            telemetry_hooks=telemetry_hooks,
        )

    if response.status_code == 503:
        return _handle_manifest_poll_failure(
            logger,
            result="vendor_outage_503",
            manifest_url=manifest_url,
            manifest_latency_ms=manifest_latency_ms,
            status_code=response.status_code,
            alert_manifest_error_threshold=alert_manifest_error_threshold,
            alert_lag_seconds_threshold=alert_lag_seconds_threshold,
            alert_state=alert_state,
            telemetry_hooks=telemetry_hooks,
        )

    if response.status_code != 200:
        return _handle_manifest_poll_failure(
            logger,
            result="http_error",
            manifest_url=manifest_url,
            manifest_latency_ms=manifest_latency_ms,
            status_code=response.status_code,
            alert_manifest_error_threshold=alert_manifest_error_threshold,
            alert_lag_seconds_threshold=alert_lag_seconds_threshold,
            alert_state=alert_state,
            telemetry_hooks=telemetry_hooks,
        )

    _call_hook(telemetry_hooks, "set_degraded_state", degraded_type="vendor_outage", active=False)

    entries = parse_manifest(response.text, logger)
    manifest_latest_slice = max((entry.slice_ts for entry in entries), default=None)
    _call_hook(
        telemetry_hooks,
        "record_manifest_poll",
        status_code=response.status_code,
        success=True,
        latency_ms=manifest_latency_ms,
        line_count=len(entries),
        manifest_latest_slice=manifest_latest_slice,
    )
    _log.log_event(
        logger,
        action="manifest_poll",
        result="ok",
        line_count=len(entries),
        url=manifest_url,
    )

    by_slice: dict[str, set[str]] = {}
    for entry in entries:
        _call_hook(telemetry_hooks, "mark_manifest_slice_seen", slice_ts=entry.slice_ts)
        by_slice.setdefault(entry.slice_ts, set()).add(entry.file_type)

    for slice_ts, present_types in by_slice.items():
        for expected in EXPECTED_FILE_TYPES:
            if expected not in present_types:
                _log.log_event(
                    logger,
                    action="manifest_validate",
                    result="missing_file_type",
                    slice_ts=slice_ts,
                    file_type=expected,
                    level=logging.WARNING,
                )

    results: list[PollResult] = []

    # Backfill: if the durable checkpoint set's highest slice_ts is more than
    # one step behind what the manifest now shows as current, the manifest
    # advanced past one or more slices we never attempted — either because
    # this poll's cadence is slower than the vendor's replay speed, or because
    # the poller was down/restarted and missed manifest updates entirely. The
    # manifest never re-lists a slice once it's no longer current, but the
    # vendor still serves any curated slice directly by URL, so we can fetch
    # the gap ourselves instead of silently losing it.
    floor_slice_ts = max((slice_ts for slice_ts, _ in seen), default=None)
    if (
        floor_slice_ts is not None
        and manifest_latest_slice is not None
        and floor_slice_ts < manifest_latest_slice
    ):
        gap_slices = _expected_slice_sequence(floor_slice_ts, manifest_latest_slice)
        # The floor slice itself may be a partial slice: one or more of its
        # file types were never seen (e.g. chaos-hidden from every manifest)
        # before the manifest moved on to a later slice_ts. The manifest
        # never re-lists a slice once it's no longer current, so a type
        # missing at the floor would be lost forever if we only walked the
        # strictly-between range — the vendor still serves it directly by
        # URL, so fold it into the same backfill sweep.
        floor_has_gap = any((floor_slice_ts, ft) not in seen for ft in EXPECTED_FILE_TYPES)
        backfill_slices = ([floor_slice_ts] if floor_has_gap else []) + gap_slices
        if backfill_slices:
            if len(backfill_slices) > max_backfill_slices_per_poll:
                _log.log_event(
                    logger,
                    action="backfill",
                    result="capped",
                    level=logging.WARNING,
                    floor_slice_ts=floor_slice_ts,
                    manifest_latest_slice=manifest_latest_slice,
                    gap_slice_count=len(backfill_slices),
                    cap=max_backfill_slices_per_poll,
                )
                backfill_slices = backfill_slices[:max_backfill_slices_per_poll]
            else:
                _log.log_event(
                    logger,
                    action="backfill",
                    result="gap_detected",
                    level=logging.WARNING,
                    floor_slice_ts=floor_slice_ts,
                    manifest_latest_slice=manifest_latest_slice,
                    gap_slice_count=len(backfill_slices),
                )

            for gap_slice_ts in backfill_slices:
                for file_type in EXPECTED_FILE_TYPES:
                    if (gap_slice_ts, file_type) in seen:
                        continue
                    backfill_entry = ManifestEntry(
                        expected_bytes=None,
                        expected_sha1=None,
                        url=f"{base_url.rstrip('/')}/v2/{gap_slice_ts}.{file_type}.csv.zip",
                        slice_ts=gap_slice_ts,
                        file_type=file_type,
                    )
                    _process_entry(
                        session,
                        backfill_entry,
                        seen,
                        results,
                        logger,
                        timeout_seconds=timeout_seconds,
                        max_retries=max_retries,
                        backoff_initial_seconds=backoff_initial_seconds,
                        backoff_max_seconds=backoff_max_seconds,
                        telemetry_hooks=telemetry_hooks,
                    )

    for entry in entries:
        _process_entry(
            session,
            entry,
            seen,
            results,
            logger,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            backoff_initial_seconds=backoff_initial_seconds,
            backoff_max_seconds=backoff_max_seconds,
            telemetry_hooks=telemetry_hooks,
        )

    latest_lag_seconds_by_file_type: dict[str, float] = {}
    simulated_now = _fetch_simulated_now(
        session,
        base_url=base_url,
        logger=logger,
        timeout_seconds=timeout_seconds,
    )
    if simulated_now is not None:
        simulated_now_dt = _parse_timestamp(simulated_now)
        if simulated_now_dt is not None:
            for result in results:
                slice_dt = _parse_timestamp(result.entry.slice_ts)
                if slice_dt is None:
                    continue
                lag_seconds = (simulated_now_dt - slice_dt).total_seconds()
                _log.log_event(
                    logger,
                    action="lag_check",
                    result="ok",
                    slice_ts=result.entry.slice_ts,
                    simulated_now=simulated_now,
                    lag_seconds=lag_seconds,
                )
                latest_lag_seconds_by_file_type[result.entry.file_type] = lag_seconds

    latest_processed_slice_by_file_type: dict[str, str] = {}
    for result in results:
        file_type = result.entry.file_type
        slice_ts = result.entry.slice_ts
        current = latest_processed_slice_by_file_type.get(file_type)
        if current is None or slice_ts > current:
            latest_processed_slice_by_file_type[file_type] = slice_ts

    _log_poll_metrics(
        logger,
        manifest_poll_success_count=1,
        manifest_poll_error_count=0,
        processed_file_count=len(results),
        latest_processed_slice_by_file_type=latest_processed_slice_by_file_type,
        latest_lag_seconds_by_file_type=latest_lag_seconds_by_file_type,
    )
    _emit_alert_events(
        logger,
        manifest_poll_success_count=1,
        manifest_poll_error_count=0,
        latest_lag_seconds_by_file_type=latest_lag_seconds_by_file_type,
        alert_manifest_error_threshold=alert_manifest_error_threshold,
        alert_lag_seconds_threshold=alert_lag_seconds_threshold,
        alert_state=alert_state,
        telemetry_hooks=telemetry_hooks,
    )

    if alert_state is not None and manifest_latest_slice is not None:
        previous_latest = alert_state.get("last_manifest_latest_slice")
        previous_repeats = int(alert_state.get("manifest_repeat_count", 0))
        if previous_latest == manifest_latest_slice:
            repeat_count = previous_repeats + 1
        else:
            repeat_count = 0

        stale_active = bool(alert_state.get("stale_manifest_active", False))
        should_be_active = repeat_count >= 1

        if should_be_active and not stale_active:
            _log.log_event(
                logger,
                action="degraded",
                result="firing",
                degraded_type="stale_manifest",
                manifest_latest_slice=manifest_latest_slice,
                repeat_count=repeat_count,
            )
            _call_hook(
                telemetry_hooks,
                "set_degraded_state",
                degraded_type="stale_manifest",
                active=True,
            )
            alert_state["stale_manifest_active"] = True
        elif (not should_be_active) and stale_active:
            _log.log_event(
                logger,
                action="degraded",
                result="cleared",
                degraded_type="stale_manifest",
                manifest_latest_slice=manifest_latest_slice,
            )
            _call_hook(
                telemetry_hooks,
                "set_degraded_state",
                degraded_type="stale_manifest",
                active=False,
            )
            alert_state["stale_manifest_active"] = False

        alert_state["manifest_repeat_count"] = repeat_count
        alert_state["last_manifest_latest_slice"] = manifest_latest_slice

    return results


def poll_forever(
    *,
    base_url: str = "http://localhost:18200",
    interval_seconds: float = 0.45,
    timeout_seconds: float = 15.0,
    max_retries: int = 2,
    backoff_initial_seconds: float = 0.2,
    backoff_max_seconds: float = 2.0,
    alert_manifest_error_threshold: int = 3,
    alert_lag_seconds_threshold: float = 1800.0,
    max_backfill_slices_per_poll: int = DEFAULT_MAX_BACKFILL_SLICES_PER_POLL,
    seen: set[tuple[str, str]] | None = None,
    persist_callback: PersistCallback | None = None,
    telemetry_hooks: dict[str, TelemetryHook] | None = None,
) -> Iterable[PollResult]:
    logger = _log._setup_logger()
    seen_set: set[tuple[str, str]] = set() if seen is None else set(seen)
    alert_state: dict[str, object] = {
        "vendor_feed_down_active": False,
        "lag_high_active_by_file_type": {},
        "stale_manifest_active": False,
        "manifest_repeat_count": 0,
        "last_manifest_latest_slice": None,
    }

    with requests.Session() as session:
        while True:
            for result in poll_once(
                session,
                base_url=base_url,
                seen=seen_set,
                logger=logger,
                timeout_seconds=timeout_seconds,
                max_retries=max_retries,
                backoff_initial_seconds=backoff_initial_seconds,
                backoff_max_seconds=backoff_max_seconds,
                alert_manifest_error_threshold=alert_manifest_error_threshold,
                alert_lag_seconds_threshold=alert_lag_seconds_threshold,
                max_backfill_slices_per_poll=max_backfill_slices_per_poll,
                alert_state=alert_state,
                telemetry_hooks=telemetry_hooks,
            ):
                if persist_callback is not None:
                    persist_callback(result)
                    _log.log_event(
                        logger,
                        action="db_persist",
                        result="ok",
                        slice_ts=result.entry.slice_ts,
                        file_type=result.entry.file_type,
                        row_count=len(result.rows),
                    )
                yield result
            time.sleep(interval_seconds)


def _build_persist_callback(
    *,
    database_url: str,
    logger: logging.Logger,
    checkpoint_retain_per_file_type: int,
    checkpoint_compact_every: int,
) -> tuple[PersistCallback, set[tuple[str, str]], object]:
    from src.ingest.storage import PersistableResult, PostgresIngestStore

    store = PostgresIngestStore(dsn=database_url)
    store.open()
    seen = store.load_seen()
    _log.log_event(
        logger,
        action="db_checkpoint",
        result="loaded",
        checkpoint_count=len(seen),
    )
    persist_count = 0

    def _persist(result: PollResult) -> None:
        nonlocal persist_count
        store.persist(
            PersistableResult(
                slice_ts=result.entry.slice_ts,
                file_type=result.entry.file_type,
                rows=result.rows,
            )
        )
        persist_count += 1

        if checkpoint_compact_every > 0 and persist_count % checkpoint_compact_every == 0:
            deleted_count = store.compact_checkpoints(
                retain_per_file_type=checkpoint_retain_per_file_type
            )
            _log.log_event(
                logger,
                action="db_checkpoint",
                result="compacted",
                deleted_count=deleted_count,
                retain_per_file_type=checkpoint_retain_per_file_type,
                compact_every=checkpoint_compact_every,
            )

    return _persist, seen, store


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Poll vendor manifest, download and verify slice files, and parse CSV rows "
            "in memory."
        )
    )
    parser.add_argument("--base-url", default="http://localhost:18200")
    parser.add_argument(
        "--poll-profile",
        choices=("dev", "prod"),
        default="dev",
        help="Polling profile: dev keeps fast replay polling, prod uses one-minute cadence.",
    )
    parser.add_argument("--interval-seconds", type=float, default=None)
    parser.add_argument("--timeout-seconds", type=float, default=15.0)
    parser.add_argument("--max-retries", type=int, default=2)
    parser.add_argument("--backoff-initial-seconds", type=float, default=0.2)
    parser.add_argument("--backoff-max-seconds", type=float, default=2.0)
    parser.add_argument("--alert-manifest-error-threshold", type=int, default=3)
    parser.add_argument("--alert-lag-seconds-threshold", type=float, default=1800.0)
    parser.add_argument(
        "--max-backfill-slices-per-poll",
        type=int,
        default=DEFAULT_MAX_BACKFILL_SLICES_PER_POLL,
        help=(
            "Cap on how many missed slices to backfill (via direct per-slice GET) "
            "in a single poll when the manifest has advanced past slices we "
            "never attempted."
        ),
    )
    parser.add_argument(
        "--database-url",
        default="",
        help="Optional PostgreSQL DSN. If set, parsed rows are upserted and checkpoints are persisted.",
    )
    parser.add_argument(
        "--checkpoint-retain-per-file-type",
        type=int,
        default=192,
        help="Number of latest done checkpoints to keep per file type.",
    )
    parser.add_argument(
        "--checkpoint-compact-every",
        type=int,
        default=10,
        help="Run checkpoint compaction after this many persisted files (0 disables).",
    )
    args = parser.parse_args()

    logger = _log._setup_logger()
    persist_callback: PersistCallback | None = None
    telemetry_hooks: dict[str, TelemetryHook] | None = None
    seen: set[tuple[str, str]] | None = None
    store = None

    if args.database_url:
        try:
            persist_callback, seen, store = _build_persist_callback(
                database_url=args.database_url,
                logger=logger,
                checkpoint_retain_per_file_type=args.checkpoint_retain_per_file_type,
                checkpoint_compact_every=args.checkpoint_compact_every,
            )
        except Exception as exc:
            _log.log_event(
                logger,
                action="db_connect",
                result="error",
                level=logging.ERROR,
                error=str(exc),
            )
            return 1

    if store is not None:
        telemetry_hooks = {
            "record_manifest_poll": store.record_manifest_poll,
            "record_file_attempt": store.record_file_attempt,
            "record_alert_event": store.record_alert_event,
            "mark_manifest_slice_seen": store.mark_manifest_slice_seen,
            "set_degraded_state": store.set_degraded_state,
        }

    interval_seconds = args.interval_seconds
    if interval_seconds is None:
        interval_seconds = 60.0 if args.poll_profile == "prod" else 0.45

    alert_manifest_error_threshold = args.alert_manifest_error_threshold
    if args.poll_profile == "prod" and args.alert_manifest_error_threshold == 3:
        alert_manifest_error_threshold = 1

    try:
        for _ in poll_forever(
            base_url=args.base_url,
            interval_seconds=interval_seconds,
            timeout_seconds=args.timeout_seconds,
            max_retries=args.max_retries,
            backoff_initial_seconds=args.backoff_initial_seconds,
            backoff_max_seconds=args.backoff_max_seconds,
            alert_manifest_error_threshold=alert_manifest_error_threshold,
            alert_lag_seconds_threshold=args.alert_lag_seconds_threshold,
            max_backfill_slices_per_poll=args.max_backfill_slices_per_poll,
            seen=seen,
            persist_callback=persist_callback,
            telemetry_hooks=telemetry_hooks,
        ):
            pass
    finally:
        if store is not None:
            store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
