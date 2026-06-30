"""FastAPI app for the gdelt-vendor mock.

Routes:
  GET  /healthz             liveness + simulated_now + manifest size
  GET  /stats               per-file-type GET counts, chaos rates, outage state
  GET  /v2/lastupdate.txt   GDELT manifest: 3 lines `<bytes> <sha1> <url>`
  GET  /v2/{filename}       curated zip bytes (or 404 chaos-late / 503 outage)
  GET  /simulated_now       `{simulated_now: "..."}` debug helper
"""
from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse, Response

from . import chaos
from . import clock as clock_mod
from . import storage


VENDOR_PUBLIC_BASE = os.environ.get("VENDOR_PUBLIC_BASE", "http://localhost:18200").rstrip("/")
FILE_TYPES = ("events", "mentions", "articles")


# ───────────────────────── App state ────────────────────────────────────────

state: clock_mod.ReplayState = clock_mod.ReplayState()
chaos_state = chaos.ChaosState()
outage_windows = chaos.parse_outage_schedule(os.environ.get("VENDOR_OUTAGE_SCHEDULE"))

# Per-file-type GET counters.
get_counts: dict[str, int] = {"events": 0, "mentions": 0, "articles": 0}
# Tracks the last slice_ts the manifest published, so stale-chaos can pin
# the prior one.
last_published_ts: str | None = None


@asynccontextmanager
async def lifespan(_app: FastAPI):
    storage.load_manifest()

    persisted = clock_mod.load_persisted_clock()
    if persisted:
        state.restore(persisted)

    replay_task = asyncio.create_task(clock_mod.replay_loop(state))
    persist_task = asyncio.create_task(clock_mod._persist_loop(state))
    try:
        yield
    finally:
        replay_task.cancel()
        persist_task.cancel()


app = FastAPI(title="Tremor gdelt-vendor mock", version="0.1.0", lifespan=lifespan)


# ───────────────────────── Helpers ──────────────────────────────────────────

def _is_outage_now() -> bool:
    return chaos.in_outage(state.simulated_now, outage_windows)


def _current_slice_ts() -> str | None:
    """The slice_ts the manifest should currently advertise.

    Logic:
      * Round simulated_now down to the nearest manifest entry.
      * If stale-chaos is armed, pin the prior slice for 1-2 ticks.
    """
    global last_published_ts
    target = storage.datetime_to_slice_ts(state.simulated_now)
    available = storage.nearest_available_slice(target)
    if available is None:
        return None

    # Arm chaos for this slice (idempotent — only fires once per slice_ts).
    chaos_state.maybe_arm_late(available)
    chaos_state.maybe_arm_partial(available)
    chaos_state.mark_evaluated(available)

    # Stale-manifest: when the clock advances past last_published_ts, decide
    # whether to keep pinning the prior slice.
    if last_published_ts is not None and available != last_published_ts:
        served = chaos_state.maybe_pin_stale(available, last_published_ts)
        if served != available:
            return served
    last_published_ts = available
    return available


# ───────────────────────── Routes ───────────────────────────────────────────

@app.get("/healthz")
def healthz() -> dict[str, Any]:
    return {
        "ready": storage.manifest_size() > 0,
        "simulated_now": state.simulated_now.isoformat().replace("+00:00", "Z"),
        "slices_total": storage.manifest_size(),
        "slices_served": state.slices_served,
    }


@app.get("/stats")
def stats() -> dict[str, Any]:
    return {
        "simulated_now": state.simulated_now.isoformat().replace("+00:00", "Z"),
        "slices_total": storage.manifest_size(),
        "slices_served": state.slices_served,
        "get_counts": dict(get_counts),
        "chaos": {
            "late_slice_rate": chaos.LATE_SLICE_RATE,
            "partial_slice_rate": chaos.PARTIAL_SLICE_RATE,
            "stale_manifest_rate": chaos.STALE_MANIFEST_RATE,
            "outage_windows": [
                f"{a[0]:02d}:{a[1]:02d}-{b[0]:02d}:{b[1]:02d}"
                for a, b in outage_windows
            ],
            "in_outage": _is_outage_now(),
            "late_fired": chaos_state.late_fired,
            "partial_fired": chaos_state.partial_fired,
            "stale_fired": chaos_state.stale_fired,
        },
    }


@app.get("/simulated_now")
def simulated_now() -> dict[str, Any]:
    return {"simulated_now": state.simulated_now.isoformat().replace("+00:00", "Z")}


@app.get("/v2/lastupdate.txt", response_class=PlainTextResponse)
def lastupdate() -> PlainTextResponse:
    if _is_outage_now():
        raise HTTPException(status_code=503, detail="vendor outage")

    slice_ts = _current_slice_ts()
    if slice_ts is None:
        # No manifest yet — surface as 503 so pollers retry.
        raise HTTPException(status_code=503, detail="manifest not ready")

    entry = storage.manifest_for_slice(slice_ts)
    if entry is None:
        raise HTTPException(status_code=503, detail="manifest entry missing")

    lines: list[str] = []
    for ftype in FILE_TYPES:
        # Partial-slice chaos: drop this file type from the manifest.
        if chaos_state.is_partial_hidden(slice_ts, ftype):
            continue
        meta = entry.get(ftype)
        if not meta:
            continue
        filename = storage.curated_filename(slice_ts, ftype)
        url = f"{VENDOR_PUBLIC_BASE}/v2/{filename}"
        lines.append(f"{meta['bytes']} {meta['sha1']} {url}")

    body = "\n".join(lines) + "\n"
    return PlainTextResponse(body, media_type="text/plain")


@app.get("/v2/{filename}")
def get_curated(filename: str) -> Response:
    if _is_outage_now():
        raise HTTPException(status_code=503, detail="vendor outage")

    parsed = storage.parse_filename(filename)
    if parsed is None:
        raise HTTPException(status_code=400, detail="bad filename shape")
    slice_ts, ftype = parsed

    # Late-slice chaos: 404 the file while its deadline hasn't expired.
    if chaos_state.is_late_hidden(slice_ts, ftype):
        raise HTTPException(status_code=404, detail="not yet available")

    resp = storage.curated_file_response(filename)
    if resp is None:
        raise HTTPException(status_code=404, detail="unknown slice/file")

    get_counts[ftype] = get_counts.get(ftype, 0) + 1
    return resp
