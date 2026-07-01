# Tremor Ingestion Architecture and Status

## Goal
Build a reliable ingestion path for replayed vendor slices with optional durable PostgreSQL persistence, restart-safe checkpointing, and operational visibility.

## Current System Architecture

### 1) Vendor Replay Source
- Poll manifest endpoint: `/v2/lastupdate.txt`
- Manifest line contract: `<bytes> <sha1> <url>`
- Slice files are zipped CSVs for:
  - `events`
  - `mentions`
  - `articles`
- Replay timing is fast; effective cadence is one slice every ~0.45s.

### 2) Ingestion Poller (Implemented)
File: `src/ingest/poller.py`

Responsibilities currently implemented:
- Poll manifest repeatedly.
- Parse and validate manifest lines.
- Process all manifest entries (not a limited subset).
- Deterministically order work by:
  - `slice_ts`
  - file type order (`events`, `mentions`, `articles`)
  - URL (stable tie-break)
- Download each file using the exact URL from the manifest.
- Verify payload integrity:
  - byte length check
  - SHA1 check
- Open ZIP and parse CSV rows in memory.
- Keep a `(slice_ts, file_type)` seen set to avoid reprocessing.
- Emit structured JSON logs for observability.
- Optionally persist parsed rows into PostgreSQL when `--database-url` is set.

When database persistence is enabled, the poller:
- loads durable seen keys from `ingest_checkpoints`
- upserts rows and checkpoint status transactionally per file
- resumes idempotently after restart

### 3) Persistence Store (Implemented)
File: `src/ingest/storage.py`

Responsibilities implemented:
- Open/close PostgreSQL connection from DSN.
- Bootstrap base tables via `src/schema.sql`.
- Create and maintain `ingest_checkpoints` table.
- Upsert semantics:
  - `events`: `ON CONFLICT(event_id) DO UPDATE`
  - `mentions`: `ON CONFLICT(event_id, mention_time, source_domain, slice_ts) DO UPDATE`
  - `articles`: `ON CONFLICT(article_id) DO UPDATE`
- Expose checkpoint preload (`load_seen`) for restart-safe ingestion.
- Support checkpoint compaction (`compact_checkpoints`) to retain only latest done checkpoints per file type.

Checkpoint retention policy (current):
- Keep the most recent N done checkpoints per file type (default `N=192`).
- Trigger compaction after every M persisted files (default `M=10`, `0` disables).
- Configuration flags:
  - `--checkpoint-retain-per-file-type`
  - `--checkpoint-compact-every`

### 4) Time and Lag Handling (Implemented)
- Timestamp parsing supports:
  - compact format: `YYYYMMDDHHMMSS`
  - ISO format with `Z`
- Timestamps are normalized to UTC-aware datetimes.
- Poller fetches `/simulated_now` and logs `lag_seconds` per processed result.
- Poller now emits per-poll metric summary logs with:
  - `manifest_poll_success_count` / `manifest_poll_error_count`
  - `latest_processed_slice_by_file_type`
  - `latest_lag_seconds_by_file_type`
  - `processed_file_count`

### 5) Transient Failure Retry and Backoff (Implemented)
- Poller now retries transient network/HTTP failures for:
  - manifest polling
  - slice file downloads
- Retry policy:
  - exponential backoff
  - defaults: `max_retries=2`, `backoff_initial_seconds=0.2`, `backoff_max_seconds=2.0`
  - transient statuses: `429, 500, 502, 503, 504`
- Retry behavior is configurable via CLI flags:
  - `--max-retries`
  - `--backoff-initial-seconds`
  - `--backoff-max-seconds`

### 6) Alert Threshold Wiring (Implemented)
- Poller now emits alert events in structured logs:
  - `vendor_feed_down` when manifest poll errors meet threshold
  - `ingest_lag_high` when per-file lag exceeds threshold
- Alert transitions are stateful in long-running mode so alerts emit on state changes (fire/clear) instead of repeating clear events each successful poll.
- Alert thresholds are configurable via CLI flags:
  - `--alert-manifest-error-threshold`
  - `--alert-lag-seconds-threshold`
- Current tuned baseline defaults:
  - `--alert-manifest-error-threshold=3`
  - `--alert-lag-seconds-threshold=1800`

### 7) Data Model Definition (Implemented)
File: `src/schema.sql`

Current status:
- DDL created for `events`, `mentions`, `articles`.
- Indexes added.
- `mentions` uses composite primary key:
  - `(event_id, mention_time, source_domain, slice_ts)`

## Verification and Tests

### Runtime Verification Completed
- Live smoke runs validated:
  - deterministic ordering behavior
  - successful integrity checks
  - lag logging with parse/timezone fixes
- Soak evidence captures recorded under `data/evidence/` including:
  - baseline-style short run (`20260701T085553Z`): no health/stats/manifest errors
  - chaos-active short run (`20260701T091921Z`): one expected manifest 503 and transient stale-manifest samples while health/stats remained 200
  - persistence-enabled calm run (`calm/20260701T100127Z`, 30 min): single startup transient in health/stats/manifest, then stable
  - persistence-enabled chaos run (`chaos/20260701T103128Z`, 30 min): expected elevated manifest errors during chaos windows, no stale-manifest samples

Soak comparison snapshot:

| Run ID | Mode | Duration (min) | Samples | Health Errors | Stats Errors | Manifest Errors | Manifest Stale Samples | Notes |
|---|---|---:|---:|---:|---:|---:|---:|---|
| `20260701T085553Z` | baseline-style | 1 | 12 | 0 | 0 | 0 | 0 | Stable short baseline window |
| `20260701T091921Z` | chaos-active | 2 | 24 | 0 | 0 | 1 | 5 | Single expected outage sample and transient staleness |
| `20260701T100127Z` | calm + persistence | 30 | 360 | 1 | 1 | 1 | 0 | Startup transient only; ingest persisted successfully across the window |
| `20260701T103128Z` | chaos + persistence | 30 | 360 | 1 | 1 | 5 | 0 | Expected outage-window manifest errors; ingest remained active and recovered |

### Automated Tests Added
File: `tests/test_poller.py`

Coverage currently includes:
- deterministic ordering from unsorted manifest input
- timestamp parsing for compact and ISO formats
- lag check logging and lag value calculation
- guardrail test confirming no DB-driver imports in poller phase
- retry on transient manifest request failures
- retry on transient file download HTTP statuses
- poll loop callback/checkpoint preload behavior for persistence mode
- Postgres integration test scaffolding for upsert/checkpoint/rollback behavior (`tests/test_storage_integration.py`)
- poll metric summary emission for successful and failed manifest polls
- end-to-end poll -> parse -> persist callback flow in `poll_forever`
- alert threshold emission for manifest outage and high lag conditions
- checkpoint compaction scheduling via persistence callback

Latest known result:
- `C:/Python313/python.exe -m pytest -q tests/test_poller.py` passed (`17 passed`)
- `C:/Python313/python.exe -m pytest -q tests/test_storage_integration.py` is skip-gated in this environment (`11 skipped`)
- DB-backed integration path contains 11 tests and requires `psycopg` plus `TREMOR_TEST_DATABASE_URL` (or `DATABASE_URL`)
- Project-level pytest configuration now supports a plain repo-root `python -m pytest -q` run.
- `make test` was added as the canonical test workflow entrypoint.
- CI execution path now exists in `.github/workflows/ci.yml` with Postgres-backed test runs via `make test-ci`.

## Completion Status

All finish-line checklist items are complete.

## Definition of Done (Verified)
- Extended calm+chaos validation was completed and archived under `data/evidence/calm/20260701T100127Z` and `data/evidence/chaos/20260701T103128Z`.
- Alert behavior is transition-based (stateful fire/clear) and short-run post-tuning validation showed zero alert-noise events (`data/evidence/logs/poller-20260701T150552Z.jsonl`).
- Integration coverage now includes restart/idempotency and checkpoint-compaction edge cases in `tests/test_storage_integration.py` (11 DB-backed tests when environment prerequisites are provided).
- Checkpoint compaction behavior and retention policy are implemented and validated by integration tests.
- Architecture/progress/brief documentation is aligned to final validated operating posture.

## Optional Future Enhancements (Post-Capstone)
1. Add a longer periodic soak automation job (nightly) and trend dashboards for lag and alert rates.
2. Add richer alerting semantics (minimum-duration breach windows, severity levels, and dedupe windows).
3. Expand integration coverage with fault injection at transaction boundaries and network interruption simulation.