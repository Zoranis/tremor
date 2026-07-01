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
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Iterable

import requests


MANIFEST_LINE_RE = re.compile(r"^(\d+)\s+([0-9a-fA-F]{40})\s+(\S+)\s*$")
FILENAME_RE = re.compile(
    r"(?P<slice_ts>\d{14})\.(?P<file_type>events|mentions|articles)\.csv\.zip$"
)
EXPECTED_FILE_TYPES = ("events", "mentions", "articles")
FILE_TYPE_ORDER = {name: i for i, name in enumerate(EXPECTED_FILE_TYPES)}
TRANSIENT_HTTP_STATUSES = {429, 500, 502, 503, 504}


@dataclass(frozen=True)
class ManifestEntry:
    expected_bytes: int
    expected_sha1: str
    url: str
    slice_ts: str
    file_type: str


@dataclass(frozen=True)
class PollResult:
    entry: ManifestEntry
    rows: list[dict[str, str]]


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
) -> bytes | None:
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
        return None

    zip_bytes = response.content
    actual_bytes = len(zip_bytes)
    if actual_bytes != entry.expected_bytes:
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
        return None

    actual_sha1 = _sha1_bytes(zip_bytes)
    if actual_sha1 != entry.expected_sha1:
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
        return None

    log_event(
        logger,
        action="file_verify",
        result="ok",
        slice_ts=entry.slice_ts,
        file_type=entry.file_type,
        bytes=actual_bytes,
        sha1=actual_sha1,
    )
    return zip_bytes


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
) -> list[PollResult]:
    manifest_url = f"{base_url.rstrip('/')}/v2/lastupdate.txt"
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
    if response is None:
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
        return []

    entries = parse_manifest(response.text, logger)
    log_event(
        logger,
        action="manifest_poll",
        result="ok",
        line_count=len(entries),
        url=manifest_url,
    )

    by_slice: dict[str, set[str]] = {}
    for entry in entries:
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
    for entry in entries:
        key = (entry.slice_ts, entry.file_type)
        if key in seen:
            log_event(
                logger,
                action="file_process",
                result="skip_seen",
                slice_ts=entry.slice_ts,
                file_type=entry.file_type,
            )
            continue

        zip_bytes = _download_and_verify(
            session,
            entry,
            logger,
            timeout_seconds,
            max_retries=max_retries,
            backoff_initial_seconds=backoff_initial_seconds,
            backoff_max_seconds=backoff_max_seconds,
        )
        if zip_bytes is None:
            continue

        rows = _parse_zip_csv(
            zip_bytes,
            slice_ts=entry.slice_ts,
            file_type=entry.file_type,
            logger=logger,
        )
        if rows is None:
            continue

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

    return results


def poll_forever(
    *,
    base_url: str = "http://localhost:18200",
    interval_seconds: float = 0.45,
    timeout_seconds: float = 15.0,
    max_retries: int = 2,
    backoff_initial_seconds: float = 0.2,
    backoff_max_seconds: float = 2.0,
) -> Iterable[PollResult]:
    logger = _setup_logger()
    seen: set[tuple[str, str]] = set()

    with requests.Session() as session:
        while True:
            for result in poll_once(
                session,
                base_url=base_url,
                seen=seen,
                logger=logger,
                timeout_seconds=timeout_seconds,
                max_retries=max_retries,
                backoff_initial_seconds=backoff_initial_seconds,
                backoff_max_seconds=backoff_max_seconds,
            ):
                yield result
            time.sleep(interval_seconds)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Poll vendor manifest, download and verify slice files, and parse CSV rows "
            "in memory."
        )
    )
    parser.add_argument("--base-url", default="http://localhost:18200")
    parser.add_argument("--interval-seconds", type=float, default=0.45)
    parser.add_argument("--timeout-seconds", type=float, default=15.0)
    parser.add_argument("--max-retries", type=int, default=2)
    parser.add_argument("--backoff-initial-seconds", type=float, default=0.2)
    parser.add_argument("--backoff-max-seconds", type=float, default=2.0)
    args = parser.parse_args()

    for _ in poll_forever(
        base_url=args.base_url,
        interval_seconds=args.interval_seconds,
        timeout_seconds=args.timeout_seconds,
        max_retries=args.max_retries,
        backoff_initial_seconds=args.backoff_initial_seconds,
        backoff_max_seconds=args.backoff_max_seconds,
    ):
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
