# Tremor — Solution Explanation

> Companion reading: [`BRIEF.md`](BRIEF.md) is the assignment. [`RUNBOOK.md`](RUNBOOK.md) is
> the operator's cheat sheet. This file explains **how the system that answers the brief is
> actually built**, part by part, as of the current codebase (post `7719f87`, the
> poller/dashboard decomposition).

## Brief description

Tremor turns a noisy, poll-only GDELT news feed into two things a newsroom can rely on: an
always-on **operational dashboard** an overnight producer can glance at, and a clean **SQL
surface** an editor can query live at 06:00. A simulated vendor (`gdelt-vendor`) replays seven
days of real GDELT data on a fast clock and injects realistic failure modes — late files,
partial slices, stale manifests, outages. An **ingestion poller** polls that vendor once a
minute, verifies and parses each slice's three CSVs, and durably persists them to Postgres with
restart-safe checkpointing. Every poll, download, and alert transition is also written to
observability tables, which feed both a lightweight **dashboard** (FastAPI + a hand-rolled
JS/SVG frontend, no Grafana/Prometheus) and a pack of six **analyst SQL queries** that answer
the newsroom's editorial questions directly against the same database. The whole stack comes up
with `make run` and is designed to survive `docker compose restart` of any one service without
losing or duplicating a slice.

The rest of this document walks through each piece: the vendor, the poller (and its five
submodules), the persistence/observability schema, the dashboard, the SQL pack, and the
operational tooling that ties it together — then traces one slice's journey end to end and
explains the resilience decisions that make restart-safety and chaos-tolerance hold up.

## System at a glance

| Component | Code | Role |
|---|---|---|
| `data-init` | `vendor/download_and_curate.py` | One-shot: downloads + curates 7 sim-days of GDELT into the `gdelt-cache` volume, then exits. |
| `gdelt-vendor` | `vendor/app/` | FastAPI mock of the real GDELT manifest contract, replaying the cached window on a simulated clock, with four chaos modes. |
| `postgres` | — | Durable store for curated rows (`events`/`mentions`/`articles`) and pipeline observability tables. |
| `ingest-poller` | `src/ingest/` | Polls the manifest, verifies + parses each file, upserts rows, checkpoints progress, emits structured logs and alerts. |
| `dashboard` | `dashboard/` | FastAPI metrics API (`/metrics`) plus a static HTML/CSS/JS single-screen UI that polls it. |
| SQL pack | `sql/queries/` | Six hand-written queries answering the brief's editorial questions directly against Postgres. |
| Tooling | `Makefile`, `tools/`, `RUNBOOK.md` | One-command bring-up, chaos toggles, soak-evidence capture, restart validation. |

```
 data-init ──(populates)──> gdelt-cache volume
                                   │
                                   ▼
                            gdelt-vendor  <───polls (1/min)─── ingest-poller ───upserts───> postgres
                          (manifest+files,                          │                        │  ▲
                           chaos modes)                    structured JSON logs         observability
                                                                                          tables/views
                                                                                               │
                                                                                dashboard <────┘
                                                                              (/metrics + UI)
                                                                                               │
                                                                                    analyst SQL pack
                                                                                   (sql/queries/*.sql)
```

## Part 1 — The vendor: a faithful, misbehaving GDELT

**Files:** `vendor/app/main.py`, `clock.py`, `chaos.py`, `storage.py`, `download_and_curate.py`

The vendor exists so the pipeline can be built and defended without depending on the real
GDELT service. `data-init` downloads and curates a 7-day historical window into a shared
Docker volume once; `gdelt-vendor` then serves that cached window over the exact contract
described in `BRIEF.md`:

- `GET /v2/lastupdate.txt` → three lines of `<bytes> <sha1> <url>`, one per file type.
- `GET /v2/{slice_ts}.{type}.csv.zip` → the curated zip.

**`clock.py`** drives a simulated clock (`ReplayState`) that advances through the cached window
at `REPLAY_SECONDS_PER_SLICE` wall-seconds per 15-minute slice (default `0.45s`, i.e. ~8
simulated minutes per wall-second), loops when it reaches the end of the window, and
periodically persists its position so a vendor restart resumes mid-replay instead of rewinding.

**`chaos.py`** implements the four failure modes the brief requires the pipeline to survive,
each gated by an env var and armed *once per slice* (not re-rolled every poll, so behavior is
stable within a slice):

- **Late slice** (`VENDOR_LATE_SLICE_RATE`) — a random file type 404s for 30–90s after the slice
  becomes current, then serves cleanly.
- **Partial slice** (`VENDOR_PARTIAL_SLICE_RATE`) — a random file type is omitted from the
  manifest for 30–120s, even though `GET /v2/<file>` already serves it directly — this is the
  "manifest is not atomic across file types" case the brief calls out.
- **Stale manifest** (`VENDOR_STALE_MANIFEST_RATE`) — when the clock advances, the manifest
  keeps advertising the *prior* slice for 1–2 polls before catching up.
- **Outage** (`VENDOR_OUTAGE_SCHEDULE`) — a wall-clock `HH:MM-HH:MM` window during which both
  the manifest and file endpoints return 503.

`make vendor-chaos` / `make vendor-calm` flip all four at once by recreating the `gdelt-vendor`
container with new env vars (chaos knobs are read at import time, so a restart is required to
change them).

## Part 2 — The ingestion poller

**Files:** `src/ingest/poller.py`, `manifest.py`, `http_client.py`, `alerts.py`,
`poller_logging.py`

This is the core of the pipeline, and it was recently split from one monolithic file into five
single-purpose modules (see the module docstring in `poller.py`) so each concern can be tested
and reasoned about independently:

- **`manifest.py`** — pure/stateless. Parses manifest lines and filenames into `ManifestEntry`
  objects, normalizes both compact (`YYYYMMDDHHMMSS`) and ISO timestamp formats to UTC, and
  computes the expected sequence of 15-minute slice boundaries between two timestamps (used for
  backfill, below). No HTTP, no I/O — this is why it's trivial to unit test.
- **`http_client.py`** — owns "fetch one URL reliably and verify what came back": retry-with-
  backoff on transient statuses (`429/500/502/503/504`) and connection errors, SHA1 + byte-length
  verification against the manifest's declared values, and zip/CSV parsing into row dicts.
- **`alerts.py`** — turns raw poll counts into two operator-facing alerts (`vendor_feed_down`,
  `ingest_lag_high`) using **stateful fire/clear transitions**: an alert only logs (and records a
  telemetry event) when its active/inactive state *changes*, not on every poll it remains in that
  state. This is a deliberate anti-noise decision — see Part 6.
- **`poller_logging.py`** — one structured-JSON `log_event()` helper shared by every module, plus
  a `telemetry_hooks` dispatch mechanism so the poller can stay ignorant of *how* metrics get
  persisted (see Part 3).
- **`poller.py`** — orchestration only. `poll_once()` does, in order:
  1. Fetch the manifest (with retry); a failure here (`request_error`, `503`, or any non-200)
     short-circuits to alert/telemetry bookkeeping and returns no results.
  2. Parse and sort entries deterministically — `(slice_ts, fixed file-type order, url)` — so
     processing order is reproducible run-to-run regardless of manifest line order.
  3. Validate that all three file types are present per slice; log a warning for any that
     aren't (this is what a partial-slice chaos event looks like from the poller's side).
  4. **Backfill gap detection** — if the highest slice_ts we've durably finished is more than
     one step behind the manifest's current slice, the manifest advanced past slices we never
     attempted (slow poll cadence, or the poller was down). Since the manifest never re-lists a
     slice once it's no longer current but the vendor still serves any curated slice directly by
     URL, the poller reconstructs the missing `(slice_ts, file_type)` keys itself
     (`_expected_slice_sequence`) and fetches them directly, capped at
     `--max-backfill-slices-per-poll` (default 300) to bound a single poll's burst after a long
     outage.
  5. Process every current-manifest entry through `_process_entry` — skip if already in the
     in-memory `seen` set, otherwise download+verify+parse and append to results.
  6. Fetch `/simulated_now` and compute per-file-type lag for anything just processed.
  7. Log a per-poll metrics summary and evaluate alert transitions.
  8. Track stale-manifest as its own degraded-window state: if the manifest's latest slice
     repeats across polls, fire a `stale_manifest` degraded event; clear it once the manifest
     advances again.

  `poll_forever()` wraps this in a `while True` loop with `time.sleep(interval_seconds)`, and
  optionally calls a `persist_callback` per result (wired to Postgres — see Part 3) before
  yielding.

  CLI (`main()`) exposes a `--poll-profile {dev,prod}` switch: `dev` keeps the fast 0.45s replay
  cadence for local iteration; `prod` polls once per minute as the brief specifies, and also
  tightens the manifest-error alert threshold to 1 (a single failed poll matters more when polls
  are 60s apart).

## Part 3 — Persistence and observability

**Files:** `src/schema.sql`, `src/ingest/storage.py`

Two distinct concerns live in the same Postgres database:

**Business data** — `events`, `mentions`, `articles`, matching the brief's column contracts
exactly. All three tables are written via `ON CONFLICT ... DO UPDATE` (upsert), which is what
makes retries and late-arriving replays harmless: reprocessing a slice overwrites the same row
rather than duplicating it. `mentions` uses a composite primary key
`(event_id, mention_time, source_domain, slice_ts)` with **no foreign key** to `events` — a
deliberate choice, because a partial-slice chaos event can legitimately deliver a mentions row
before its corresponding events row exists, and a strict FK would turn expected non-atomic
publishing into ingest failures.

**Restart-safety** — `ingest_checkpoints (slice_ts, file_type, status)`. On startup with
`--database-url` set, the poller calls `store.load_seen()` to preload every `status = 'done'`
checkpoint into the in-memory `seen` set, so already-completed `(slice_ts, file_type)` work is
skipped immediately after a restart — this is the mechanism behind `RUNBOOK.md`'s "restart and
idempotency check". Checkpoints are compacted periodically (`compact_checkpoints`, default: keep
the latest 192 `done` rows per file type, run every 10 persisted files) so this table doesn't
grow unbounded over a long-running deployment; it's operational metadata, not business history.

**Pipeline observability** — four more tables, all fed via the `telemetry_hooks` dict that
`poller.py` calls into without knowing it's talking to Postgres:

- `ingest_manifest_polls` — every manifest poll: status code, success, latency, line count.
- `ingest_file_attempts` — every file download attempt: outcome (`ok`, `late_slice_404`,
  `bytes_mismatch`, `sha1_mismatch`, `http_error`, `request_error`), latency.
- `ingest_alert_events` — every alert fire/clear transition, with the value and threshold that
  triggered it.
- `ingest_degraded_windows` — start/end timestamps for `vendor_outage` and `stale_manifest`
  degraded periods (an open row has `active = TRUE` and `ended_at IS NULL`).
- `ingest_slice_status` — one row per `slice_ts` with `events_done`/`mentions_done`/
  `articles_done` booleans and a `fully_processed_at` timestamp set only once all three flip
  true. This table is what makes "slice lag" precisely computable: it's the durable answer to
  "which slice has this pipeline *fully* processed," not just "which file did we last see."

Five views turn that raw telemetry into the exact metrics the brief's dashboard section asks
for, so all four required signals are one `SELECT` away:

| View | Answers |
|---|---|
| `v_slice_lag` | manifest's latest slice minus the latest **fully-processed** slice, in seconds and in slice-count |
| `v_ingestion_rate_5m_by_type` | rolling 5-minute successful-slice count per file type |
| `v_manifest_poll_health` | success rate, avg/p95 latency, and 503 count over the last 120 polls |
| `v_pipeline_self_audit` | fraction of the observed window spent in a `stale_manifest` degraded state |
| `v_slice_completion` | per-slice completion flags, for ad hoc auditing |

`PostgresIngestStore._ensure_schema()` runs `schema.sql` (all `CREATE TABLE IF NOT EXISTS` /
`CREATE OR REPLACE VIEW`, so it's idempotent) plus one explicit migration — dropping a legacy
`mentions_event_id_fkey` if a prior schema version created it — every time the poller opens a
connection, so the schema self-heals on startup without a separate migration step.

## Part 4 — The operational dashboard

**Files:** `dashboard/app.py`, `dashboard/templates/index.html`, `dashboard/static/app.{css,js}`

A small FastAPI service, split into an API half and a UI half:

- **`app.py`**: `/healthz` for the container healthcheck, and one aggregate `/metrics` endpoint
  that queries the five views/tables above and returns them as one JSON payload — slice lag,
  5-minute ingestion rate by type, manifest poll health, currently-firing alerts (last 60
  minutes), and recent degraded windows. `/` serves the static `index.html` shell.
- **`static/app.js`** (not read line-by-line here, but its job per `index.html`'s structure)
  polls `/metrics` on an interval and renders four KPI tiles (slice lag, manifest poll success,
  p95 poll latency, outage alert state) plus an SVG sparkline chart of per-file-type ingestion
  rate, an active-alerts list, and a degraded-windows table — with a visible "DASHBOARD
  CONNECTION LOST" banner if polling itself starts failing, so a stale screen doesn't get
  mistaken for a healthy one.

This directly satisfies the brief's four required overnight-producer signals (slice lag,
per-file-type ingestion rate, manifest poll health, outage alert) on one auto-refreshing screen,
without pulling in Grafana/Prometheus — the observability schema in Part 3 was designed to make
that unnecessary.

## Part 5 — The analyst SQL surface

**Files:** `sql/queries/*.sql`, `sql/queries/README.md`

Six queries, one per editorial question in the brief, run directly against the same
`events`/`mentions`/`articles` tables the poller writes:

1. `protest_hotspots.sql` — top-10 countries by `protest`/`clash` volume, anchored to the
   **latest ingested `event_time`** (not wall-clock `NOW()`) since the replay window tracks its
   own fixed historical dates.
2. `bilateral_trend.sql` — daily mean `intensity` for a chosen actor↔target pair.
3. `theme_surge.sql` — top themes by mention growth (last 24 sim-hours vs. trailing 7-day
   baseline); joins mentions to `articles.primary_theme` via `slice_ts`, since mentions rows
   don't carry theme directly.
4. `outlet_amplification.sql` — mention-to-event ratio per `source_domain`.
5. `geographic_overlay.sql` — event volume vs. protest-article volume per 1°lat/lon bucket, via
   a `FULL OUTER JOIN` on bucket coordinates so a region with articles but no events (the "story
   is breaking now" case) still shows up.
6. `pipeline_self_audit.sql` — degraded-window history plus the `v_pipeline_self_audit` fraction,
   i.e. "what fraction of the window did we run degraded" — required reading before trusting the
   other five.

Because these run against the exact tables the poller upserts into (not a separate warehouse or
batch export), a producer's answer is only as stale as the poller's last successful poll —
consistent with "iterate during the editorial meeting."

## Part 6 — Operational tooling

- **`Makefile`** — `make run` builds and starts the full stack (vendor + Postgres + poller +
  dashboard) with the production 60s poll profile; `make run-dev` uses the fast replay-speed
  profile for local iteration; `make vendor-chaos`/`make vendor-calm` toggle all four chaos modes
  by recreating just the vendor container; `make soak-capture` and `make validate-restart` back
  the evidence-gathering workflow below.
- **`tools/capture_soak_evidence.py`** — samples the vendor's `/healthz`, `/stats`, and manifest
  endpoints on an interval and writes `timeline.jsonl` + `summary.json` per run under
  `data/evidence/<run_id>/`, used to validate alert thresholds against real calm/chaos behavior
  rather than guessing them (see `RUNBOOK.md`'s tuned defaults:
  `--alert-manifest-error-threshold 3`, `--alert-lag-seconds-threshold 1800`).
- **`tools/validate_restart_safety.py`** — automates the "restart any one service, confirm no
  duplicate/dropped slice" check the brief's operational baseline requires.
- **`RUNBOOK.md`** — the four-scenario quick-reference (outage, malformed manifest, partial
  slice, restart) an overnight producer would actually use at 03:00.

## Data flow: one slice, start to finish

1. The vendor's clock crosses a 15-minute boundary; `_current_slice_ts()` picks the new
   `slice_ts` (unless stale-manifest chaos pins the prior one).
2. The poller's next `poll_once()` GETs `/v2/lastupdate.txt`, gets 3 manifest lines back, parses
   and sorts them deterministically.
3. For each `(slice_ts, file_type)` not already in `seen`: download the zip (retrying on
   transient failures), verify its byte length and SHA1 against the manifest's declared values,
   open the zip, and CSV-parse its single member into row dicts.
4. If a database URL is configured, `_persist()` upserts those rows into `events`/`mentions`/
   `articles`, marks `ingest_checkpoints` and `ingest_slice_status` for that file type, and — once
   all three file types are done for that slice — `ingest_slice_status.fully_processed_at` gets
   set, which is what `v_slice_lag` reads.
5. Every step along the way (manifest poll, file attempt, alert transition) also lands in the
   telemetry tables via `telemetry_hooks`, independent of whether the row-level upsert succeeded.
6. The dashboard's next `/metrics` poll picks up the new state within seconds; an analyst running
   one of the six SQL queries sees the new rows on their next execution.
7. If the poller restarts at any point, `load_seen()` reloads `done` checkpoints on boot and
   skips already-finished `(slice_ts, file_type)` pairs — no duplicate rows, no reprocessing.

## Key design decisions (why it's built this way)

- **Manifest-driven polling, not push.** The vendor contract is poll-only; polling once a minute
  in production (vs. GDELT's real 5s) is friendlier and still sub-slice-cadence.
- **Deterministic processing order.** Sorting by `(slice_ts, file-type order, url)` makes runs
  reproducible and test assertions and incident forensics simpler, at the cost of not
  parallelizing — a deliberate correctness-over-throughput tradeoff at this scale.
  Backfill entries get their file type appended in the same fixed order.
- **Two-signal integrity verification.** Byte length *and* SHA1 are both checked before a payload
  is parsed; either alone is weaker (byte-length collisions are easy, hash-only misses truncation
  patterns). Backfilled entries (fetched directly by URL after the manifest moved on) skip this —
  there's no manifest line to verify against — and rely on the zip/CSV structural check instead.
- **DB-optional poller.** `poller.py` has zero database imports; `storage.py` is only imported
  inside `_build_persist_callback` when `--database-url` is passed. This keeps smoke-testing and
  unit tests fast and isolates DB failures from the retrieval/verification path.
- **Composite dedup key, not a global watermark.** `(slice_ts, file_type)` matches the vendor's
  actual unit of work, so partial slices and late files are handled precisely instead of an
  all-or-nothing per-slice watermark that would either reprocess too much or skip a still-missing
  file type forever.
- **Backfill instead of silent gaps.** Because the manifest is a moving window (it never re-lists
  an old slice), a poller that's slow or was restarted would otherwise lose slices permanently.
  Reconstructing the expected slice sequence from timestamps and fetching directly by URL closes
  that gap, capped to bound worst-case burst size after a long outage.
- **Alerts fire/clear on transition, not every poll.** A naive "log alert state every poll"
  design floods the log during a long outage and buries the one signal that matters (state
  *changed*). `_transition_alert` only emits when `active != was_active`.
- **Upsert over insert-only or delete-and-reload.** Makes retries and replays idempotent by
  construction rather than requiring a separate reconciliation step.
- **No FK from mentions to events.** Chosen over strict referential integrity because the vendor
  explicitly does not publish the three files atomically (that's the partial-slice chaos mode) —
  a strict FK would convert expected, recoverable behavior into hard ingest failures.

## Running and verifying it

```bash
make run                 # full stack, prod poll profile (60s)
make run-dev              # full stack, fast replay-speed poll profile
curl localhost:18200/healthz     # vendor health
curl localhost:18600/metrics     # dashboard metrics JSON
open http://localhost:18600      # dashboard UI

make vendor-chaos        # turn on late/partial/stale/outage
make soak-capture        # record evidence into data/evidence/<run_id>/
make vendor-calm

make test                 # full suite (DB integration tests auto-skip without TREMOR_TEST_DATABASE_URL)
make validate-restart     # restart-safety check
```

For the SQL pack, connect to `postgresql://tremor:tremor@localhost:15432/tremor` and run any file
under `sql/queries/`.
