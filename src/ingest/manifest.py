"""Manifest line parsing and slice-timestamp helpers.

Pure/stateless: no HTTP, no persistence. Anything that turns manifest text
or a slice_ts string into a typed value lives here.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from . import poller_logging as _log


MANIFEST_LINE_RE = re.compile(r"^(\d+)\s+([0-9a-fA-F]{40})\s+(\S+)\s*$")
FILENAME_RE = re.compile(
    r"(?P<slice_ts>\d{14})\.(?P<file_type>events|mentions|articles)\.csv\.zip$"
)
EXPECTED_FILE_TYPES = ("events", "mentions", "articles")
FILE_TYPE_ORDER = {name: i for i, name in enumerate(EXPECTED_FILE_TYPES)}
SLICE_STEP = timedelta(minutes=15)


@dataclass(frozen=True)
class ManifestEntry:
    expected_bytes: int | None
    expected_sha1: str | None
    url: str
    slice_ts: str
    file_type: str


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
            _log.log_event(
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
