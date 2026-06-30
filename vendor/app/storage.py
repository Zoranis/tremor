"""Curated-CSV storage layer for the gdelt-vendor.

Two responsibilities:

  * On startup, read `<CACHE_DIR>/curated/_manifests.json` once into memory.
    The map shape is `{ "<YYYYMMDDHHMMSS>": { "events": {"bytes": N, "sha1": "..."},
                                              "mentions": {...},
                                              "articles": {...} } }`.
    Built by `download_and_curate.py` so the FastAPI manifest endpoint is O(1).
  * Resolve `/v2/{filename}` to a concrete path under `<CACHE_DIR>/curated/`,
    returning a `FileResponse` (zip bytes, served verbatim).
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi.responses import FileResponse


CACHE_DIR = Path(os.environ.get("CACHE_DIR", "/cache"))
CURATED_DIR = CACHE_DIR / "curated"
MANIFEST_PATH = CURATED_DIR / "_manifests.json"


# ───────────────────────── Manifest loading ─────────────────────────────────

_MANIFEST: dict[str, dict[str, dict[str, Any]]] = {}
_SLICE_TS_SORTED: list[str] = []


def load_manifest() -> None:
    """Read `_manifests.json` into module-level state. Idempotent."""
    global _MANIFEST, _SLICE_TS_SORTED
    if not MANIFEST_PATH.exists():
        _MANIFEST = {}
        _SLICE_TS_SORTED = []
        return
    raw = json.loads(MANIFEST_PATH.read_text())
    _MANIFEST = raw
    _SLICE_TS_SORTED = sorted(raw.keys())


def manifest_for_slice(slice_ts: str) -> dict[str, dict[str, Any]] | None:
    """Return `{file_type: {bytes, sha1}}` for a slice, or None if absent."""
    return _MANIFEST.get(slice_ts)


def all_slice_ts() -> list[str]:
    return _SLICE_TS_SORTED


def manifest_size() -> int:
    return len(_MANIFEST)


# ───────────────────────── Slice-ts helpers ─────────────────────────────────

def datetime_to_slice_ts(t: datetime) -> str:
    """`datetime` → GDELT slice timestamp string (`YYYYMMDDHHMMSS`)."""
    if t.tzinfo is None:
        t = t.replace(tzinfo=timezone.utc)
    return t.strftime("%Y%m%d%H%M%S")


def slice_ts_to_datetime(ts: str) -> datetime:
    return datetime.strptime(ts, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)


def nearest_available_slice(target_ts: str) -> str | None:
    """Return the largest slice_ts in the manifest ≤ target_ts.

    The clock may briefly point at a slice before it lands in the manifest
    (shouldn't happen because data-init runs to completion first, but be
    defensive). Returns None if the manifest is empty.
    """
    if not _SLICE_TS_SORTED:
        return None
    # Linear scan from the end is fine: manifests are <1k entries.
    for ts in reversed(_SLICE_TS_SORTED):
        if ts <= target_ts:
            return ts
    return None


def prior_slice(slice_ts: str) -> str | None:
    """Return the manifest slice immediately before `slice_ts`, or None."""
    if not _SLICE_TS_SORTED:
        return None
    prev: str | None = None
    for ts in _SLICE_TS_SORTED:
        if ts >= slice_ts:
            return prev
        prev = ts
    return prev


# ───────────────────────── File serving ─────────────────────────────────────

def curated_filename(slice_ts: str, file_type: str) -> str:
    return f"{slice_ts}.{file_type}.csv.zip"


def parse_filename(filename: str) -> tuple[str, str] | None:
    """`20240101001500.events.csv.zip` → (`20240101001500`, `events`)."""
    if not filename.endswith(".csv.zip"):
        return None
    stem = filename[: -len(".csv.zip")]
    parts = stem.split(".")
    if len(parts) != 2:
        return None
    slice_ts, ftype = parts
    if len(slice_ts) != 14 or not slice_ts.isdigit():
        return None
    if ftype not in ("events", "mentions", "articles"):
        return None
    return slice_ts, ftype


def curated_file_response(filename: str) -> FileResponse | None:
    """Return a `FileResponse` for the curated zip, or None if absent."""
    path = CURATED_DIR / filename
    if not path.exists():
        return None
    return FileResponse(
        path,
        media_type="application/zip",
        filename=filename,
    )
