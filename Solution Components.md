# Solution Components

Organized by functional component. Each file's role is described in terms of what it
contributes to that component, not just what it contains.

## 1. Vendor — simulated GDELT source

Provides the manifest-driven, chaos-capable data source the rest of the system is built
against. Not something Tremor "solves" so much as the fixed external contract it ingests from.

| File | Role |
|---|---|
| `vendor/app/main.py` | FastAPI app exposing the GDELT contract: `GET /v2/lastupdate.txt` (manifest), `GET /v2/{slice_ts}.{type}.csv.zip` (file download), plus `/healthz`, `/stats`, `/simulated_now` for operational visibility into the vendor itself. |
| `vendor/app/clock.py` | Drives the simulated replay clock (`ReplayState`) — advances through the cached historical window at `REPLAY_SECONDS_PER_SLICE` wall-seconds per 15-minute slice, loops at the window boundary, and persists its position so a vendor restart resumes rather than rewinds. |
| `vendor/app/chaos.py` | Implements the four failure modes the brief requires (late slice, partial slice, stale manifest, outage), each gated by an env var and armed once per slice so behavior is stable within that slice. |
| `vendor/app/storage.py` | Reads curated slice files back off the `gdelt-cache` volume for `main.py` to serve; resolves "nearest available slice" for the current simulated time. |
| `vendor/app/__init__.py` | Marks `vendor/app` as a Python package so `uvicorn app.main:app` can import it. |
| `vendor/download_and_curate.py` | One-shot script (run as the `data-init` container) that downloads real GDELT 2.0 data for the configured window and curates it into the small per-slice CSVs the vendor serves — this is what populates `gdelt-cache` on first `make run`. |
| `vendor/Dockerfile` | Builds the single image used for both `data-init` and `gdelt-vendor` roles (compose picks the command per service). |
| `vendor/pyproject.toml` | Vendor-side Python dependencies (FastAPI, uvicorn, pyarrow for curation, pydantic, requests). |

## 2. Ingestion poller

The core pipeline component: polls the vendor, verifies and parses each slice, and hands
results off for persistence — resiliently, under all four chaos modes.

| File | Role |
|---|---|
| `src/ingest/poller.py` | Orchestrator. `poll_once()` fetches the manifest, deterministically orders entries, detects and backfills gaps, downloads/verifies/parses new files, computes lag, and evaluates alert transitions; `poll_forever()` loops it with a configurable interval and an optional persist callback. Also the CLI entrypoint (`--poll-profile dev|prod`, retry/threshold flags). |
| `src/ingest/manifest.py` | Pure parsing logic: turns raw manifest lines/filenames into `ManifestEntry` objects, normalizes compact and ISO timestamps to UTC, and computes the expected slice-boundary sequence between two timestamps (used for backfill). No I/O — this is what makes it unit-testable in isolation. |
| `src/ingest/http_client.py` | Owns "fetch a URL reliably and verify it": retry-with-backoff on transient HTTP/network failures, SHA1 + byte-length verification against the manifest's declared values, and zip/CSV parsing into row dicts. |
| `src/ingest/alerts.py` | Converts raw poll counters into the two operator-facing alerts (`vendor_feed_down`, `ingest_lag_high`) using stateful fire/clear transitions, so an alert only logs when its state actually changes. |
| `src/ingest/poller_logging.py` | Shared structured-JSON `log_event()` helper plus the `telemetry_hooks` dispatch mechanism, so `poller.py` can emit metrics without knowing (or importing) anything about how they're persisted. |

## 3. Persistence & observability schema

Shared foundation both the poller and the dashboard/SQL surface depend on: durable business
data, restart-safety metadata, and the telemetry that makes the dashboard's required signals
computable straight from SQL.

| File | Role |
|---|---|
| `src/ingest/storage.py` | `PostgresIngestStore` — the poller's only DB-facing module. Bootstraps the schema, upserts `events`/`mentions`/`articles`, maintains `ingest_checkpoints` (restart-safe dedup) with periodic compaction, and writes the four telemetry tables via the `telemetry_hooks` contract. |
| `src/schema.sql` | DDL for everything durable: business tables (`events`, `mentions`, `articles`), restart metadata (`ingest_checkpoints`), telemetry tables (`ingest_manifest_polls`, `ingest_file_attempts`, `ingest_alert_events`, `ingest_degraded_windows`, `ingest_slice_status`), and the five reporting views (`v_slice_lag`, `v_ingestion_rate_5m_by_type`, `v_manifest_poll_health`, `v_pipeline_self_audit`, `v_slice_completion`) that both the dashboard and the analyst SQL pack read from. Written idempotently (`CREATE ... IF NOT EXISTS`) so it self-applies on every connection. |

## 4. Operational dashboard

The always-on, single-screen surface the brief's overnight producer watches.

| File | Role |
|---|---|
| `dashboard/app.py` | FastAPI service: `/healthz` for the container healthcheck, `/metrics` aggregating the schema's views/tables into one JSON payload (slice lag, 5-minute ingestion rate by type, manifest poll health, active alerts, degraded windows), and `/` serving the static UI shell. |
| `dashboard/templates/index.html` | The dashboard page structure — KPI tiles, chart container, alerts list, degraded-windows table. |
| `dashboard/static/app.js` | Polls `/metrics` on an interval and renders the KPIs, an SVG sparkline of per-file-type ingestion rate, the active-alerts list, and a "connection lost" banner if polling itself starts failing. |
| `dashboard/static/app.css` | Styling for the dashboard UI. |
| `dashboard/Dockerfile` | Builds the dashboard's standalone image (installs `requirements.txt`, copies `app.py`/`static`/`templates`). |
| `dashboard/requirements.txt` | Dashboard's runtime dependencies (FastAPI, uvicorn, psycopg) — kept separate from the poller's `pyproject.toml` since it's a distinct deployable service. |

## 5. Analyst SQL surface

Six hand-written queries that let a producer answer the brief's editorial questions directly
against the same tables the poller writes.

| File | Role |
|---|---|
| `sql/queries/protest_hotspots.sql` | Top-10 countries by protest/clash volume for "yesterday," anchored to the latest ingested `event_time` rather than wall-clock `NOW()`. |
| `sql/queries/bilateral_trend.sql` | Daily mean `intensity` for a chosen actor↔target country pair. |
| `sql/queries/theme_surge.sql` | Top themes by mention growth, last 24 sim-hours vs. trailing 7-day baseline. |
| `sql/queries/outlet_amplification.sql` | Mention-to-event ratio per `source_domain`. |
| `sql/queries/geographic_overlay.sql` | Event volume vs. protest-article volume per 1° lat/lon bucket. |
| `sql/queries/pipeline_self_audit.sql` | Fraction of the window spent in a degraded (stale-manifest/outage) state, from `v_pipeline_self_audit` and `ingest_degraded_windows`. |
| `sql/queries/README.md` | Index of the six queries plus notes on non-obvious join choices (e.g., `theme_surge.sql` joining via `slice_ts` since mentions don't carry theme directly). |

## 6. Orchestration & deployment

Brings the whole stack up as a single system and wires services together.

| File | Role |
|---|---|
| `compose.yml` | Defines all five services (`data-init`, `gdelt-vendor`, `postgres`, `ingest-poller`, `dashboard`), their build contexts, healthchecks, dependency ordering, volumes, and port mappings. |
| `Makefile` | User-facing entrypoints: `make run`/`run-dev`/`run-prod-profile` (bring-up), `stop`/`reset`, `vendor-chaos`/`vendor-calm` (chaos toggles), `test`/`test-integration`/`test-ci`, `soak-capture`, `validate-restart`. |
| `Dockerfile` (root) | Builds the `ingest-poller` image: installs `pyproject.toml` dependencies, copies `src/`, runs `python -m src.ingest.poller`. |
| `pyproject.toml` (root) | Poller/dashboard-shared Python project definition — dependencies (psycopg, requests, pydantic, fastapi, uvicorn, pytest) and pytest configuration. |
| `.env` / `.env.example` | Configuration for replay speed, replay window, chaos rates, poll profile, and database URL — `.env.example` is the checked-in template; `.env` is the active (gitignored-in-spirit but present here) configuration `docker compose` reads. |

## 7. Testing & CI

Proves the poller's logic is correct in isolation and against a real database.

| File | Role |
|---|---|
| `tests/test_poller.py` | Unit tests: deterministic ordering, timestamp parsing, retry behavior, alert threshold emission, checkpoint-compaction scheduling, metric summary logging, and a guardrail asserting the poller has zero DB-driver imports. |
| `tests/test_storage_integration.py` | Postgres-backed integration tests: upsert correctness, checkpoint restart/idempotency, compaction behavior — auto-skipped when no test database is configured. |
| `.github/workflows/ci.yml` | GitHub Actions workflow: spins up a Postgres service container and runs `make test-ci` on every push/PR. |

## 8. Operational tooling & evidence

Turns "we believe it's resilient" into recorded, reproducible proof.

| File | Role |
|---|---|
| `tools/capture_soak_evidence.py` | Samples the vendor's `/healthz`, `/stats`, and manifest endpoints on an interval and writes `timeline.jsonl` + `summary.json` per run — the raw evidence used to tune alert thresholds against observed behavior. |
| `tools/validate_restart_safety.py` | Automates the "restart a service, confirm no duplicate/dropped slice" check the brief's operational baseline requires. |
| `tools/run_soak_matrix.sh` | Runs a calm-mode soak followed by a chaos-mode soak back-to-back against a live poller + vendor, archiving both under `data/evidence/calm/` and `data/evidence/chaos/`. |
| `RUNBOOK.md` | The four-scenario operator quick-reference (outage, malformed manifest, partial slice, restart) an overnight producer would use at 03:00. |
| `data/evidence/**` | Archived timelines/summaries from soak runs (not individual files worth listing — these are the *output* of the tools above, one directory per run). |

## 9. Documentation & defense materials

Explains the system to a reader (or a defense committee) rather than running any part of it.

| File | Role |
|---|---|
| `BRIEF.md` | The assignment itself — source of truth for requirements. |
| `README.md` | Quick-start: bring-up, endpoints, chaos toggles, replay speed, test workflow. |
| `SOLUTION.md` | Full component-by-component technical walkthrough of the built system. |
| `Solution description.md` | Short summary of the solution against the brief's requirements. |
| `Design Decisions.md` | Per-decision record of what was chosen, the alternatives, and why they were rejected. |
| `ARCHITECTURE.md` | Point-in-time architecture/status record from mid-development. |
| `ARCHITECTURE_BRIEF.md` | Condensed "what was done" summary plus a defense-prep section of decisions/alternatives/rationale. |
| `Architechture2.md` | A gap-analysis-turned-implementation-plan written partway through development, listing brief requirements not yet met (dashboard, SQL pack, observability schema) and the phased plan to close them. |
| `PROGRESS.md` | Phase-by-phase completion tracker. |
| `REVIEW.md` | An independent critical review that found real defects (a slice-loss bug in the default `make run` config from a poll-interval/replay-speed mismatch, `NOW()` vs. simulated-time bugs in two SQL queries) — both since fixed, per the current poller's backfill logic and `protest_hotspots.sql`'s watermark-anchored query. |
| `sql/queries/README.md` | Documented under component 5 above — the SQL pack's own index/notes file. |
| `data/samples/COLUMNS.md` | Reference documentation for the three CSVs' column contracts, with observed sample values and a note on which fields are empty in the sample slice. |

## Files not part of any component

Leftover, generated, or scaffold-provided files that aren't part of the running solution or its
documentation:

- **`.post-init.py`** — a course-scaffold hook (not project code) that runs once when the
  assignment scaffold is copied into a student's directory: it ensures `data/.gitkeep` exists
  and copies `.env.example` to `.env` if missing. Not invoked by anything in this solution's
  runtime or build path.
- **`1.txt`** — a stray one-line scratch note (`python.exe -m pytest -q -s -o log_cli=true
  --log-cli-level=INFO`) recording a test command for manual reference; not referenced by any
  script, Makefile target, or CI step.
- **`data/source_sampling.log`** — a one-off exploratory log from early data-source
  investigation (listing the vendor's endpoints); superseded by `BRIEF.md`'s own documented
  contract and not read by any code.
- **`data/.gitkeep`**, **`src/.gitkeep`** — empty placeholder files whose only purpose is
  keeping otherwise-empty directories tracked in git.
- **`src/tremor.egg-info/`** — auto-generated Python packaging metadata produced by `pip
  install .`; a build artifact, not authored source.
- **`__pycache__/` directories** (`dashboard/`, `src/ingest/`, `tests/`, `tools/`) — compiled
  bytecode caches produced by running Python; not source, safe to delete/regenerate at any
  time.
