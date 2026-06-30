"""Chaos toggles for the gdelt-vendor.

All knobs read from the env at module import; restarting the container (e.g.
via `make vendor-chaos` / `make vendor-calm`) re-imports the module and picks
up the new values.

Four modes:

  * LATE_SLICE_RATE     — when a slice first becomes current, decide whether
                          any of its files should 404 for 30–90 wall-seconds
                          before serving cleanly. State lives in a per-key
                          deadline map; main.py consults it on every GET.
  * PARTIAL_SLICE_RATE  — when a slice first becomes current, decide whether
                          to omit one file type from the manifest until a
                          random delay has elapsed. The file IS available at
                          /v2/<filename> directly — it just isn't advertised
                          for a while.
  * STALE_MANIFEST_RATE — when the clock advances, with this probability hold
                          the prior slice on the manifest for 1–2 ticks before
                          publishing the new one.
  * OUTAGE_SCHEDULE     — sim-time windows during which the manifest endpoint
                          and /v2/{file} both return 503.
"""
from __future__ import annotations

import os
import random
import time
from datetime import datetime
from typing import Iterable


LATE_SLICE_RATE     = float(os.environ.get("VENDOR_LATE_SLICE_RATE", "0.0"))
PARTIAL_SLICE_RATE  = float(os.environ.get("VENDOR_PARTIAL_SLICE_RATE", "0.0"))
STALE_MANIFEST_RATE = float(os.environ.get("VENDOR_STALE_MANIFEST_RATE", "0.0"))

LATE_MIN_S = 30.0
LATE_MAX_S = 90.0
PARTIAL_MIN_S = 30.0
PARTIAL_MAX_S = 120.0

FILE_TYPES = ("events", "mentions", "articles")


class ChaosState:
    """Per-process chaos memory. Held by main.py as a singleton."""

    def __init__(self, seed: int = 0xC0FFEE) -> None:
        self.rng = random.Random(seed)
        # (slice_ts, file_type) → unix_ts when the file should be visible.
        self.late_until: dict[tuple[str, str], float] = {}
        # (slice_ts, file_type) → unix_ts when the file should appear in the manifest.
        self.partial_hidden_until: dict[tuple[str, str], float] = {}
        # Slices we've already evaluated chaos for; prevents flipping decisions
        # on every poll.
        self.evaluated: set[str] = set()
        # For stale-manifest: how many more polls to pin to a prior slice.
        self.stale_holds_remaining: int = 0
        # Track the slice_ts we should pin while stale.
        self.stale_pinned_ts: str | None = None
        # Track total chaos events fired (telemetry).
        self.late_fired: int = 0
        self.partial_fired: int = 0
        self.stale_fired: int = 0

    # ── Late-slice ─────────────────────────────────────────────────────────
    def maybe_arm_late(self, slice_ts: str) -> None:
        if slice_ts in self.evaluated:
            return
        if self.rng.random() < LATE_SLICE_RATE:
            ftype = self.rng.choice(FILE_TYPES)
            delay = self.rng.uniform(LATE_MIN_S, LATE_MAX_S)
            self.late_until[(slice_ts, ftype)] = time.time() + delay
            self.late_fired += 1

    def is_late_hidden(self, slice_ts: str, ftype: str) -> bool:
        deadline = self.late_until.get((slice_ts, ftype))
        if deadline is None:
            return False
        if time.time() >= deadline:
            return False
        return True

    # ── Partial-slice ──────────────────────────────────────────────────────
    def maybe_arm_partial(self, slice_ts: str) -> None:
        if slice_ts in self.evaluated:
            return
        if self.rng.random() < PARTIAL_SLICE_RATE:
            ftype = self.rng.choice(FILE_TYPES)
            delay = self.rng.uniform(PARTIAL_MIN_S, PARTIAL_MAX_S)
            self.partial_hidden_until[(slice_ts, ftype)] = time.time() + delay
            self.partial_fired += 1

    def is_partial_hidden(self, slice_ts: str, ftype: str) -> bool:
        deadline = self.partial_hidden_until.get((slice_ts, ftype))
        if deadline is None:
            return False
        if time.time() >= deadline:
            return False
        return True

    def mark_evaluated(self, slice_ts: str) -> None:
        self.evaluated.add(slice_ts)
        # Don't let memory grow without bound.
        if len(self.evaluated) > 4000:
            for _ in range(1000):
                self.evaluated.pop()

    # ── Stale-manifest ─────────────────────────────────────────────────────
    def maybe_pin_stale(self, current_ts: str, prior_ts: str) -> str:
        """Called when the clock has advanced to `current_ts` and the prior
        published slice was `prior_ts`. Returns the slice_ts that should be
        served on the manifest (either `current_ts` or `prior_ts`)."""
        if self.stale_holds_remaining > 0 and self.stale_pinned_ts is not None:
            self.stale_holds_remaining -= 1
            return self.stale_pinned_ts
        if self.rng.random() < STALE_MANIFEST_RATE:
            self.stale_holds_remaining = self.rng.choice([1, 2])
            self.stale_pinned_ts = prior_ts
            self.stale_fired += 1
            return prior_ts
        return current_ts


# ───────────────────────── Outage windows ───────────────────────────────────

def parse_outage_schedule(raw: str | None) -> list[tuple[tuple[int, int], tuple[int, int]]]:
    """Parse 'HH:MM-HH:MM,HH:MM-HH:MM' into [( (h1,m1),(h2,m2) ), ...]."""
    if not raw:
        return []
    out: list[tuple[tuple[int, int], tuple[int, int]]] = []
    for piece in raw.split(","):
        piece = piece.strip()
        if not piece:
            continue
        try:
            a, b = piece.split("-", 1)
            ah, am = (int(x) for x in a.split(":", 1))
            bh, bm = (int(x) for x in b.split(":", 1))
            out.append(((ah, am), (bh, bm)))
        except Exception:
            continue
    return out


def in_outage(now: datetime, windows: Iterable[tuple[tuple[int, int], tuple[int, int]]]) -> bool:
    cur = now.hour * 60 + now.minute
    for (h1, m1), (h2, m2) in windows:
        start = h1 * 60 + m1
        end = h2 * 60 + m2
        if start <= cur < end:
            return True
    return False
