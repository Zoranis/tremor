"""Background replay clock for the gdelt-vendor.

Holds `simulated_now` (rounded down to a 15-minute slice boundary) and
advances it at REPLAY_SECONDS_PER_SLICE pace. Persists state to
<CACHE_DIR>/_clock_state.json every 5 wall-seconds so a `docker compose
restart gdelt-vendor` resumes mid-window.

Unlike Lume, there is no fan-out queue here — discovery is by manifest poll,
not by push. The clock only has to expose `simulated_now`; storage.py and
main.py compute the manifest contents on each request.
"""
from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


SECONDS_PER_SLICE = float(os.environ.get("REPLAY_SECONDS_PER_SLICE", "0.45"))
WINDOW_START = datetime.fromisoformat(os.environ.get("REPLAY_WINDOW_START", "2024-01-01")).replace(tzinfo=timezone.utc)
WINDOW_END = datetime.fromisoformat(os.environ.get("REPLAY_WINDOW_END", "2024-01-08")).replace(tzinfo=timezone.utc)
LOOP_AT_END = os.environ.get("REPLAY_LOOP", "true").lower() in ("1", "true", "yes")
SLICE_MINUTES = 15

STATE_PATH = Path(os.environ.get("CACHE_DIR", "/cache")) / "_clock_state.json"


def floor_to_slice(t: datetime) -> datetime:
    """Round `t` down to the nearest :00/:15/:30/:45 boundary."""
    minute = (t.minute // SLICE_MINUTES) * SLICE_MINUTES
    return t.replace(minute=minute, second=0, microsecond=0)


@dataclass
class ReplayState:
    # `simulated_now` is the *latest published slice timestamp* — i.e. the
    # boundary that students should see at `/v2/lastupdate.txt`.
    simulated_now: datetime = field(default_factory=lambda: floor_to_slice(WINDOW_START))
    slices_served: int = 0

    def snapshot(self) -> dict[str, Any]:
        return {"simulated_now": self.simulated_now.isoformat()}

    def restore(self, payload: dict[str, Any]) -> None:
        try:
            t = datetime.fromisoformat(payload["simulated_now"])
            if t.tzinfo is None:
                t = t.replace(tzinfo=timezone.utc)
            self.simulated_now = floor_to_slice(t)
        except Exception:
            pass


def load_persisted_clock() -> dict[str, Any] | None:
    if not STATE_PATH.exists():
        return None
    try:
        return json.loads(STATE_PATH.read_text())
    except Exception:
        return None


async def _persist_loop(state: ReplayState) -> None:
    while True:
        await asyncio.sleep(5)
        try:
            # Persist outside the read-only cache mount: write next to the
            # mount root if writable; if not, just no-op (the vendor still
            # works without resume).
            try:
                STATE_PATH.write_text(json.dumps(state.snapshot(), indent=2))
            except OSError:
                pass
        except Exception:
            pass


async def replay_loop(state: ReplayState) -> None:
    """Advance `simulated_now` one slice every SECONDS_PER_SLICE wall-seconds.

    Wall-time anchored: every tick we compute how many slices should have
    elapsed and snap there, so a slow tick doesn't slip the clock behind.
    """
    SLICE_TIMEDELTA = timedelta(minutes=SLICE_MINUTES)
    TICK_S = max(SECONDS_PER_SLICE * 0.5, 0.05)

    # Clamp restored clock into the window.
    if state.simulated_now < WINDOW_START:
        state.simulated_now = floor_to_slice(WINDOW_START)

    wall_origin = time.monotonic()
    sim_origin = state.simulated_now

    while True:
        wall_elapsed = time.monotonic() - wall_origin
        slices_due = int(wall_elapsed / SECONDS_PER_SLICE)
        target = sim_origin + slices_due * SLICE_TIMEDELTA

        if target > state.simulated_now:
            # End-of-window handling.
            window_end_slice = floor_to_slice(WINDOW_END) + timedelta(days=1)
            if target >= window_end_slice:
                if LOOP_AT_END:
                    state.simulated_now = floor_to_slice(WINDOW_START)
                    wall_origin = time.monotonic()
                    sim_origin = state.simulated_now
                    state.slices_served += 1
                    await asyncio.sleep(TICK_S)
                    continue
                else:
                    state.simulated_now = window_end_slice - SLICE_TIMEDELTA
                    await asyncio.sleep(1.0)
                    continue
            state.simulated_now = target
            state.slices_served += 1

        await asyncio.sleep(TICK_S)
