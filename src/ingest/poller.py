from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import logging
import re
import time
import zipfile
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from typing import Callable, Iterable

import requests


MANIFEST_LINE_RE = re.compile(r"^(\d+)\s+([0-9a-fA-F]{40})\s+(\S+)\s*$")
FILENAME_RE = re.compile(
    r"(?P<slice_ts>\d{14})\.(?P<file_type>events|mentions|articles)\.csv\.zip$"
)
EXPECTED_FILE_TYPES = ("events", "mentions", "articles")
FILE_TYPE_ORDER = {name: i for i, name in enumerate(EXPECTED_FILE_TYPES)}
TRANSIENT_HTTP_STATUSES = {429, 500, 502, 503, 504}
SLICE_STEP = timedelta(minutes=15)
# Worst case under default settings (60s prod poll vs. 0.45s/slice replay) is
# ~133 slices between polls; this leaves headroom while still bounding a
# single poll's backfill burst if the gap is much larger (e.g. long downtime).
DEFAULT_MAX_BACKFILL_SLICES_PER_POLL = 300


@dataclass(frozen=True)
class ManifestEntry:
    expected_bytes: int | None
    expected_sha1: str | None
    url: str
    slice_ts: str
    file_type: str


@dataclass(frozen=True)
class PollResult:
    entry: ManifestEntry
    rows: list[dict[str, str]]


PersistCallback = Callable[[PollResult], None]
TelemetryHook = Callable[..., None]


def _setup_logger() -> logging.Logger:
    logger = logging.getLogger("tremor.ingest.poller")
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    logger.propagate = False
    return logger


def log_event(
    logger: logging.Logger,
    *,
    action: str,
    result: str,
    slice_ts: str | None = None,
    file_type: str | None = None,
    level: int = logging.INFO,
    **fields: object,
) -> None:
    payload: dict[str, object] = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "action": action,
        "result": result,
        "slice_ts": slice_ts,
        "file_type": file_type,
    }
    payload.update(fields)
    logger.log(level, json.dumps(payload, sort_keys=True, default=str))


def _call_hook(hooks: dict[str, TelemetryHook] | None, name: str, **kwargs: object) -> None:
    if hooks is None:
        return
    hook = hooks.get(name)
    if hook is None:
        return
    try:
        hook(**kwargs)
    except Exception:
        # Telemetry must not break ingestion.
        return


def parse_manifest_line(line: str) -> ManifestEntry | None:
    m = MANIFEST_LINE_RE.match(line)
    if not m:
        return None

    expected_bytes = int(m.group(1))
    expected_sha1 = m.group(2).lower()
    url = m.group(3)

    filename = url.rsplit("/", 1)[-1]
    fm = FILENAME_RE.match(filename)
    if not fm:
        return None

    return ManifestEntry(
        expected_bytes=expected_bytes,
        expected_sha1=expected_sha1,
        url=url,
        slice_ts=fm.group("slice_ts"),
        file_type=fm.group("file_type"),
    )


def parse_manifest(text: str, logger: logging.Logger) -> list[ManifestEntry]:
    entries: list[ManifestEntry] = []
    raw_lines = [line.strip() for line in text.splitlines() if line.strip()]

    for line in raw_lines:
        entry = parse_manifest_line(line)
        if entry is None:
            log_event(
                logger,
                action="manifest_parse",
                result="invalid_line",
                level=logging.WARNING,
                line=line,
            )
            continue
        entries.append(entry)

    entries.sort(key=lambda e: (e.slice_ts, FILE_TYPE_ORDER.get(e.file_type, 99), e.url))

    return entries


def _sha1_bytes(data: bytes) -> str:
    return hashlib.sha1(data).hexdigest()


def _parse_ts_yyyymmddhhmmss(value: str) -> datetime | None:
    try:
        return datetime.strptime(value, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _parse_timestamp(value: str) -> datetime | None:
    compact = _parse_ts_yyyymmddhhmmss(value)
    if compact is not None:
        return compact

    try:
        # Vendor returns ISO timestamps like 2024-01-08T09:45:00Z.
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def _fetch_simulated_now(
    session: requests.Session,
    *,
    base_url: str,
    logger: logging.Logger,
    timeout_seconds: float,
) -> str | None:
    simulated_now_url = f"{base_url.rstrip('/')}/simulated_now"
    try:
        response = session.get(simulated_now_url, timeout=timeout_seconds)
    except requests.RequestException as exc:
        log_event(
            logger,
            action="lag_check",
            result="simulated_now_request_error",
            level=logging.WARNING,
            url=simulated_now_url,
            error=str(exc),
        )
        return None

    if response.status_code != 200:
        log_event(
            logger,
            action="lag_check",
            result="simulated_now_http_error",
            level=logging.WARNING,
            url=simulated_now_url,
            status=response.status_code,
        )
        return None

    body = response.text.strip()
    candidate = ""
    try:
        parsed = response.json()
        if isinstance(parsed, dict):
            raw = parsed.get("simulated_now")
            if isinstance(raw, str):
                candidate = raw.strip()
    except ValueError:
        pass

    if not candidate:
        m = re.search(r"(\d{14})", body)
        if m:
            candidate = m.group(1)

    if _parse_timestamp(candidate) is None:
        log_event(
            logger,
            action="lag_check",
            result="simulated_now_parse_error",
            level=logging.WARNING,
            url=simulated_now_url,
            body_preview=body[:120],
        )
        return None

    return candidate


def _expected_slice_sequence(after_ts: str, before_ts: str) -> list[str]:
    """15-minute-boundary slice_ts values strictly between after_ts and before_ts.

    Used to find slices the manifest skipped over between two observations —
    either because the poller polled slower than the vendor's replay advanced,
    or because the poller was down and missed one or more manifest updates.
    """
    start_dt = _parse_ts_yyyymmddhhmmss(after_ts)
    end_dt = _parse_ts_yyyymmddhhmmss(before_ts)
    if start_dt is None or end_dt is None or start_dt >= end_dt:
        return []

    out: list[str] = []
    cur = start_dt + SLICE_STEP
    while cur < end_dt:
        out.append(cur.strftime("%Y%m%d%H%M%S"))
        cur += SLICE_STEP
    return out


def _backoff_delay_seconds(*, attempt: int, initial: float, maximum: float) -> float:
    return min(maximum, initial * (2 ** (attempt - 1)))


def _get_with_retry(
    session: requests.Session,
    *,
    url: str,
    timeout_seconds: float,
    logger: logging.Logger,
    action: str,
    max_retries: int,
    backoff_initial_seconds: float,
    backoff_max_seconds: float,
    retry_statuses: set[int],
    slice_ts: str | None = None,
    file_type: str | None = None,
) -> requests.Response | None:
    attempts_total = max_retries + 1
    for attempt in range(1, attempts_total + 1):
        try:
            response = session.get(url, timeout=timeout_seconds)
        except requests.RequestException as exc:
            if attempt >= attempts_total:
                log_event(
                    logger,
                    action=action,
                    result="request_error",
                    slice_ts=slice_ts,
                    file_type=file_type,
                    level=logging.WARNING,
                    url=url,
                    error=str(exc),
                    attempt=attempt,
                    attempts_total=attempts_total,
                )
                return None

            delay_seconds = _backoff_delay_seconds(
                attempt=attempt,
                initial=backoff_initial_seconds,
                maximum=backoff_max_seconds,
            )
            log_event(
                logger,
                action=action,
                result="retry_request_error",
                slice_ts=slice_ts,
                file_type=file_type,
                level=logging.WARNING,
                url=url,
                error=str(exc),
                attempt=attempt,
                attempts_total=attempts_total,
                sleep_seconds=delay_seconds,
            )
            time.sleep(delay_seconds)
            continue

        if response.status_code in retry_statuses and attempt < attempts_total:
            delay_seconds = _backoff_delay_seconds(
                attempt=attempt,
                initial=backoff_initial_seconds,
                maximum=backoff_max_seconds,
            )
            log_event(
                logger,
                action=action,
                result="retry_http_status",
                slice_ts=slice_ts,
                file_type=file_type,
                level=logging.WARNING,
                url=url,
                status=response.status_code,
                attempt=attempt,
                attempts_total=attempts_total,
                sleep_seconds=delay_seconds,
            )
            time.sleep(delay_seconds)
            continue

        return response

    return None


def _parse_zip_csv(
    zip_bytes: bytes,
    *,
    slice_ts: str,
    file_type: str,
    logger: logging.Logger,
) -> list[dict[str, str]] | None:
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            members = [info for info in zf.infolist() if not info.is_dir()]
            if len(members) != 1:
                log_event(
                    logger,
                    action="zip_open",
                    result="invalid_member_count",
                    slice_ts=slice_ts,
                    file_type=file_type,
                    level=logging.WARNING,
                    member_count=len(members),
                )
                return None

            with zf.open(members[0], "r") as fh:
                text_stream = io.TextIOWrapper(fh, encoding="utf-8", newline="")
                reader = csv.DictReader(text_stream)
                rows = [dict(row) for row in reader]
                return rows
    except (zipfile.BadZipFile, OSError, UnicodeDecodeError, csv.Error) as exc:
        log_event(
            logger,
            action="zip_parse",
            result="error",
            slice_ts=slice_ts,
            file_type=file_type,
            level=logging.WARNING,
            error=str(exc),
        )
        return None


def _download_and_verify(
    session: requests.Session,
    entry: ManifestEntry,
    logger: logging.Logger,
    timeout_seconds: float,
    max_retries: int,
    backoff_initial_seconds: float,
    backoff_max_seconds: float,
    telemetry_hooks: dict[str, TelemetryHook] | None = None,
) -> bytes | None:
    started = time.perf_counter()

    response = _get_with_retry(
        session,
        url=entry.url,
        timeout_seconds=timeout_seconds,
        logger=logger,
        action="file_download",
        max_retries=max_retries,
        backoff_initial_seconds=backoff_initial_seconds,
        backoff_max_seconds=backoff_max_seconds,
        retry_statuses=TRANSIENT_HTTP_STATUSES,
        slice_ts=entry.slice_ts,
        file_type=entry.file_type,
    )
    if response is None:
        _call_hook(
            telemetry_hooks,
            "record_file_attempt",
            slice_ts=entry.slice_ts,
            file_type=entry.file_type,
            url=entry.url,
            status_code=None,
            success=False,
            outcome="request_error",
            latency_ms=(time.perf_counter() - started) * 1000.0,
        )
        return None

    if response.status_code == 404:
        log_event(
            logger,
            action="file_download",
            result="late_slice_404",
            slice_ts=entry.slice_ts,
            file_type=entry.file_type,
            level=logging.WARNING,
            url=entry.url,
            status=response.status_code,
        )
        _call_hook(
            telemetry_hooks,
            "record_file_attempt",
            slice_ts=entry.slice_ts,
            file_type=entry.file_type,
            url=entry.url,
            status_code=response.status_code,
            success=False,
            outcome="late_slice_404",
            latency_ms=(time.perf_counter() - started) * 1000.0,
        )
        return None

    if response.status_code != 200:
        log_event(
            logger,
            action="file_download",
            result="http_error",
            slice_ts=entry.slice_ts,
            file_type=entry.file_type,
            level=logging.WARNING,
            url=entry.url,
            status=response.status_code,
        )
        _call_hook(
            telemetry_hooks,
            "record_file_attempt",
            slice_ts=entry.slice_ts,
            file_type=entry.file_type,
            url=entry.url,
            status_code=response.status_code,
            success=False,
            outcome="http_error",
            latency_ms=(time.perf_counter() - started) * 1000.0,
        )
        return None

    zip_bytes = response.content
    actual_bytes = len(zip_bytes)
    actual_sha1 = _sha1_bytes(zip_bytes)

    # Backfilled entries (see poll_once) have no manifest-provided hash to
    # check against — the manifest never re-lists a slice once it's no longer
    # current, so we fetch those directly by URL and settle for the zip/CSV
    # structural check in _parse_zip_csv instead of byte-perfect verification.
    if entry.expected_bytes is not None and actual_bytes != entry.expected_bytes:
        log_event(
            logger,
            action="file_verify",
            result="bytes_mismatch",
            slice_ts=entry.slice_ts,
            file_type=entry.file_type,
            level=logging.WARNING,
            expected_bytes=entry.expected_bytes,
            actual_bytes=actual_bytes,
        )
        _call_hook(
            telemetry_hooks,
            "record_file_attempt",
            slice_ts=entry.slice_ts,
            file_type=entry.file_type,
            url=entry.url,
            status_code=response.status_code,
            success=False,
            outcome="bytes_mismatch",
            latency_ms=(time.perf_counter() - started) * 1000.0,
        )
        return None

    if entry.expected_sha1 is not None and actual_sha1 != entry.expected_sha1:
        log_event(
            logger,
            action="file_verify",
            result="sha1_mismatch",
            slice_ts=entry.slice_ts,
            file_type=entry.file_type,
            level=logging.WARNING,
            expected_sha1=entry.expected_sha1,
            actual_sha1=actual_sha1,
        )
        _call_hook(
            telemetry_hooks,
            "record_file_attempt",
            slice_ts=entry.slice_ts,
            file_type=entry.file_type,
            url=entry.url,
            status_code=response.status_code,
            success=False,
            outcome="sha1_mismatch",
            latency_ms=(time.perf_counter() - started) * 1000.0,
        )
        return None

    log_event(
        logger,
        action="file_verify",
        result="ok",
        slice_ts=entry.slice_ts,
        file_type=entry.file_type,
        bytes=actual_bytes,
        sha1=actual_sha1,
        verified=entry.expected_sha1 is not None,
    )
    _call_hook(
        telemetry_hooks,
        "record_file_attempt",
        slice_ts=entry.slice_ts,
        file_type=entry.file_type,
        url=entry.url,
        status_code=response.status_code,
        success=True,
        outcome="ok",
        latency_ms=(time.perf_counter() - started) * 1000.0,
    )
    return zip_bytes


def _log_poll_metrics(
    logger: logging.Logger,
    *,
    manifest_poll_success_count: int,
    manifest_poll_error_count: int,
    processed_file_count: int,
    latest_processed_slice_by_file_type: dict[str, str],
    latest_lag_seconds_by_file_type: dict[str, float],
) -> None:
    log_event(
        logger,
        action="poll_metrics",
        result="summary",
        manifest_poll_success_count=manifest_poll_success_count,
        manifest_poll_error_count=manifest_poll_error_count,
        processed_file_count=processed_file_count,
        latest_processed_slice_by_file_type=latest_processed_slice_by_file_type,
        latest_lag_seconds_by_file_type=latest_lag_seconds_by_file_type,
    )


def _emit_alert_events(
    logger: logging.Logger,
    *,
    manifest_poll_success_count: int,
    manifest_poll_error_count: int,
    latest_lag_seconds_by_file_type: dict[str, float],
    alert_manifest_error_threshold: int,
    alert_lag_seconds_threshold: float,
    alert_state: dict[str, object] | None = None,
    telemetry_hooks: dict[str, TelemetryHook] | None = None,
) -> None:
    vendor_feed_down_active = False
    lag_high_active_by_file_type: dict[str, bool] = {}
    if alert_state is not None:
        vendor_feed_down_active = bool(alert_state.get("vendor_feed_down_active", False))
        lag_state = alert_state.get("lag_high_active_by_file_type")
        if isinstance(lag_state, dict):
            lag_high_active_by_file_type = {
                str(k): bool(v) for k, v in lag_state.items()
            }

    if manifest_poll_error_count >= alert_manifest_error_threshold:
        if not vendor_feed_down_active:
            log_event(
                logger,
                action="alert",
                result="firing",
                alert_name="vendor_feed_down",
                manifest_poll_error_count=manifest_poll_error_count,
                threshold=alert_manifest_error_threshold,
            )
            _call_hook(
                telemetry_hooks,
                "record_alert_event",
                alert_name="vendor_feed_down",
                file_type=None,
                state="firing",
                value=float(manifest_poll_error_count),
                threshold=float(alert_manifest_error_threshold),
            )
            vendor_feed_down_active = True
    elif manifest_poll_success_count > 0:
        if vendor_feed_down_active:
            log_event(
                logger,
                action="alert",
                result="cleared",
                alert_name="vendor_feed_down",
                manifest_poll_error_count=manifest_poll_error_count,
                threshold=alert_manifest_error_threshold,
            )
            _call_hook(
                telemetry_hooks,
                "record_alert_event",
                alert_name="vendor_feed_down",
                file_type=None,
                state="cleared",
                value=float(manifest_poll_error_count),
                threshold=float(alert_manifest_error_threshold),
            )
            vendor_feed_down_active = False

    for file_type, lag_seconds in latest_lag_seconds_by_file_type.items():
        lag_alert_active = lag_high_active_by_file_type.get(file_type, False)
        if lag_seconds >= alert_lag_seconds_threshold:
            if not lag_alert_active:
                log_event(
                    logger,
                    action="alert",
                    result="firing",
                    file_type=file_type,
                    alert_name="ingest_lag_high",
                    lag_seconds=lag_seconds,
                    threshold=alert_lag_seconds_threshold,
                )
                _call_hook(
                    telemetry_hooks,
                    "record_alert_event",
                    alert_name="ingest_lag_high",
                    file_type=file_type,
                    state="firing",
                    value=float(lag_seconds),
                    threshold=float(alert_lag_seconds_threshold),
                )
                lag_high_active_by_file_type[file_type] = True
        elif lag_alert_active:
            log_event(
                logger,
                action="alert",
                result="cleared",
                file_type=file_type,
                alert_name="ingest_lag_high",
                lag_seconds=lag_seconds,
                threshold=alert_lag_seconds_threshold,
            )
            _call_hook(
                telemetry_hooks,
                "record_alert_event",
                alert_name="ingest_lag_high",
                file_type=file_type,
                state="cleared",
                value=float(lag_seconds),
                threshold=float(alert_lag_seconds_threshold),
            )
            lag_high_active_by_file_type[file_type] = False

    if alert_state is not None:
        alert_state["vendor_feed_down_active"] = vendor_feed_down_active
        alert_state["lag_high_active_by_file_type"] = lag_high_active_by_file_type


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
        log_event(
            logger,
            action="file_process",
            result="skip_seen",
            slice_ts=entry.slice_ts,
            file_type=entry.file_type,
        )
        return

    if telemetry_hooks is None:
        zip_bytes = _download_and_verify(
            session,
            entry,
            logger,
            timeout_seconds,
            max_retries=max_retries,
            backoff_initial_seconds=backoff_initial_seconds,
            backoff_max_seconds=backoff_max_seconds,
        )
    else:
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
    log_event(
        logger,
        action="file_process",
        result="parsed_rows",
        slice_ts=entry.slice_ts,
        file_type=entry.file_type,
        row_count=len(rows),
    )


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
        _call_hook(
            telemetry_hooks,
            "record_manifest_poll",
            status_code=None,
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

    if response.status_code == 503:
        log_event(
            logger,
            action="manifest_poll",
            result="vendor_outage_503",
            level=logging.WARNING,
            url=manifest_url,
            status=response.status_code,
        )
        _call_hook(
            telemetry_hooks,
            "record_manifest_poll",
            status_code=response.status_code,
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

    if response.status_code != 200:
        log_event(
            logger,
            action="manifest_poll",
            result="http_error",
            level=logging.WARNING,
            url=manifest_url,
            status=response.status_code,
        )
        _call_hook(
            telemetry_hooks,
            "record_manifest_poll",
            status_code=response.status_code,
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
    log_event(
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
                log_event(
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
                log_event(
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
                log_event(
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
                log_event(
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
            log_event(
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
            log_event(
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
    logger = _setup_logger()
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
                    log_event(
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
    log_event(
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
            log_event(
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

    logger = _setup_logger()
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
            log_event(
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
