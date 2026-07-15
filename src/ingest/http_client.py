"""HTTP fetch, retry, and payload-verification helpers used while polling.

Downloading and verifying one manifest entry is the unit of work here;
deciding *which* entries to fetch (ordering, backfill, dedup) belongs to
poller.py.
"""
from __future__ import annotations

import csv
import hashlib
import io
import logging
import re
import time
import zipfile

import requests

from . import poller_logging as _log
from .manifest import ManifestEntry, _parse_timestamp
from .poller_logging import TelemetryHook, _call_hook


TRANSIENT_HTTP_STATUSES = {429, 500, 502, 503, 504}


def _sha1_bytes(data: bytes) -> str:
    return hashlib.sha1(data).hexdigest()


def _backoff_delay_seconds(*, attempt: int, initial: float, maximum: float) -> float:
    return min(maximum, initial * (2 ** (attempt - 1)))


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
        _log.log_event(
            logger,
            action="lag_check",
            result="simulated_now_request_error",
            level=logging.WARNING,
            url=simulated_now_url,
            error=str(exc),
        )
        return None

    if response.status_code != 200:
        _log.log_event(
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
        _log.log_event(
            logger,
            action="lag_check",
            result="simulated_now_parse_error",
            level=logging.WARNING,
            url=simulated_now_url,
            body_preview=body[:120],
        )
        return None

    return candidate


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
                _log.log_event(
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
            _log.log_event(
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
            _log.log_event(
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
                _log.log_event(
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
        _log.log_event(
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
        _log.log_event(
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
        _log.log_event(
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
        _log.log_event(
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
        _log.log_event(
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

    _log.log_event(
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
