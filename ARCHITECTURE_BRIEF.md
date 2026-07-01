# Architecture Brief

This file summarizes [ARCHITECTURE.md](ARCHITECTURE.md) in two parts:
1. what was done
2. final completion status

## 1) What Was Done

- Vendor replay integration is in place using the manifest endpoint and per-slice file downloads (`events`, `mentions`, `articles`).
- Poller implementation is complete in [src/ingest/poller.py](src/ingest/poller.py):
  - manifest parsing and validation
  - deterministic processing order
  - file download, byte and SHA1 verification
  - ZIP/CSV parsing in memory
  - seen-key deduplication using `(slice_ts, file_type)`
- Optional PostgreSQL persistence is implemented and wired:
  - poller supports `--database-url`
  - durable checkpoint preload and idempotent resume behavior
- Persistence layer is implemented in [src/ingest/storage.py](src/ingest/storage.py):
  - schema bootstrap via [src/schema.sql](src/schema.sql)
  - upsert logic for `events`, `mentions`, `articles`
  - transactional per-file persist + checkpoint update
  - checkpoint compaction policy and controls
- Time/lag handling and observability were added:
  - UTC timestamp normalization
  - lag calculation against `/simulated_now`
  - per-poll summary metrics in structured logs
- Retry/backoff for transient errors is implemented:
  - transient HTTP/network retries with exponential backoff
  - configurable retry/backoff parameters
- Alert threshold wiring is implemented:
  - `vendor_feed_down` on manifest poll failures
  - `ingest_lag_high` on lag threshold breach
- Tests and CI are in place:
  - poller unit/integration-path tests in [tests/test_poller.py](tests/test_poller.py)
  - Postgres integration tests in [tests/test_storage_integration.py](tests/test_storage_integration.py)
  - CI workflow in [.github/workflows/ci.yml](.github/workflows/ci.yml)
  - latest verified local unit-path result: `C:/Python313/python.exe -m pytest -q tests/test_poller.py` => `17 passed`
  - latest local integration-path result in this environment: `C:/Python313/python.exe -m pytest -q tests/test_storage_integration.py` => `11 skipped` (DB prerequisites not set)
  - DB-backed integration path contains 11 tests and runs when `psycopg` plus `TREMOR_TEST_DATABASE_URL` (or `DATABASE_URL`) are configured
- Operational support artifacts were added:
  - runbook in [RUNBOOK.md](RUNBOOK.md)
  - soak evidence capture utility in [tools/capture_soak_evidence.py](tools/capture_soak_evidence.py)
- Short soak evidence runs were captured and archived:
  - baseline-style run [data/evidence/20260701T085553Z/summary.json](data/evidence/20260701T085553Z/summary.json) (no health/stats/manifest errors)
  - chaos-active run [data/evidence/20260701T091921Z/summary.json](data/evidence/20260701T091921Z/summary.json) (single expected manifest outage sample, transient stale-manifest samples)
- Long persistence-enabled soak evidence is now captured and archived:
  - calm run [data/evidence/calm/20260701T100127Z/summary.json](data/evidence/calm/20260701T100127Z/summary.json) (360 samples; startup transient only, then stable)
  - chaos run [data/evidence/chaos/20260701T103128Z/summary.json](data/evidence/chaos/20260701T103128Z/summary.json) (360 samples; elevated manifest errors during chaos windows, recovery observed)

## 2) Final Completion Status

- Project finish-line checklist is complete.
- Alert behavior is now stateful and validated against runtime evidence.
- Restart/idempotency and checkpoint-compaction edge cases are covered in integration tests and validated against a live Postgres instance.
- Architecture, progress, and operational runbook documentation are aligned with the validated final operating posture.

## 3) Defense Notes: Decisions, Alternatives, and Why

Use this section as a speaking guide in the defense conversation.

### A. Ingestion trigger model
- Decision made:
  - Use polling against `/v2/lastupdate.txt` as the ingestion trigger.
- Alternatives considered:
  - Webhook/push model from vendor.
  - Poll individual file URLs directly on a timer without manifest parsing.
- Why this decision:
  - Vendor contract is manifest-first and does not offer push.
  - Manifest gives explicit expected bytes and SHA1, enabling integrity checks before parse.
  - Polling keeps the control loop simple and robust under chaos modes.
- Teacher-facing short answer:
  - "I matched the upstream contract exactly, so reliability comes from validating what the vendor declares, not from guessing file arrival timing."

### B. Deterministic processing order
- Decision made:
  - Sort manifest entries by `slice_ts`, then fixed file-type order (`events`, `mentions`, `articles`), then URL tie-break.
- Alternatives considered:
  - Process manifest lines as-received.
  - Parallel process all entries immediately.
- Why this decision:
  - Deterministic ordering improves reproducibility and debugging.
  - It prevents behavior drift between runs when manifest line ordering changes.
  - It simplifies test assertions and incident forensics.
- Teacher-facing short answer:
  - "I optimized first for predictable behavior under failures, then for throughput. Determinism made correctness testable."

### C. Integrity verification before ingestion
- Decision made:
  - Verify both byte length and SHA1 for each downloaded zip before parsing.
- Alternatives considered:
  - Trust HTTP 200 and parse immediately.
  - Verify only one signal (just bytes or just hash).
- Why this decision:
  - Guards against partial/corrupt payloads and stale cache edge cases.
  - Prevents polluting persisted tables with invalid rows.
- Teacher-facing short answer:
  - "I treated integrity checks as a gate, not an optional validation. Bad payloads are dropped before they can affect state."

### D. In-memory parse, DB-optional architecture
- Decision made:
  - Keep poller independent of DB drivers; use an optional persist callback only when `--database-url` is provided.
- Alternatives considered:
  - Hard-couple poller and DB writes in one code path.
  - Always require DB even for smoke runs.
- Why this decision:
  - Separates concerns: retrieval/validation vs persistence.
  - Enables faster local testing and failure isolation.
  - Reduces blast radius when DB is unavailable.
- Teacher-facing short answer:
  - "I designed the poller as a pure ingestion engine first, then attached persistence as a pluggable concern."

### E. Idempotency and restart-safety strategy
- Decision made:
  - Use seen keys `(slice_ts, file_type)` in-memory plus durable `ingest_checkpoints` for restart-safe deduplication.
- Alternatives considered:
  - Depend only on table upserts for dedupe.
  - Use one global high-watermark timestamp.
- Why this decision:
  - Per-file-type checkpoints handle partial slices correctly.
  - Restart can skip already-done work immediately after loading checkpoints.
  - Works with late slices and repeated manifests.
- Teacher-facing short answer:
  - "The dedupe key matches the vendor unit-of-work: one file type in one slice. That is why restarts are safe even with partial publishes."

### F. Upsert-centered persistence design
- Decision made:
  - Use `ON CONFLICT ... DO UPDATE` per table key (`event_id`, `article_id`, composite key for mentions).
- Alternatives considered:
  - Insert-only with periodic cleanup.
  - Delete-and-reload entire slices.
- Why this decision:
  - Keeps writes idempotent and cheap.
  - Handles replays and corrected payloads without duplicate growth.
  - Preserves simple operational semantics under retries.
- Teacher-facing short answer:
  - "Upsert semantics are the simplest way to make retries harmless and keep the dataset convergent."

### G. Mentions key design and FK posture
- Decision made:
  - Use composite PK for `mentions`: `(event_id, mention_time, source_domain, slice_ts)`.
  - Explicitly drop strict FK from `mentions.event_id` to `events.event_id` if present.
- Alternatives considered:
  - Single surrogate ID for mentions.
  - Strict FK enforcement to events.
- Why this decision:
  - Mention rows can validly appear when corresponding event row is delayed/missing in current slice.
  - Strict FK would cause avoidable ingest failures and reduce resilience.
  - Composite key captures natural uniqueness at ingestion grain.
- Teacher-facing short answer:
  - "I favored ingestion continuity over strict referential blocking because vendor publish timing can be non-atomic across files."

### H. Retry/backoff policy tuning
- Decision made:
  - Retry transient network and HTTP statuses (`429, 500, 502, 503, 504`) with exponential backoff.
- Alternatives considered:
  - No retries.
  - Aggressive retries with high attempt counts.
- Why this decision:
  - No retries creates false incidents during short outages.
  - Too many retries can amplify load and delay forward progress.
  - Current defaults balance responsiveness and stability.
- Teacher-facing short answer:
  - "I tuned retries to absorb short turbulence without turning the poller into a retry storm."

### I. Alerting strategy: transition-based state
- Decision made:
  - Alerts fire and clear on state transitions only, not every poll.
- Alternatives considered:
  - Emit alert state on every loop.
  - No explicit clear event.
- Why this decision:
  - Reduces alert fatigue and noise.
  - Produces clean operator signals in long-running sessions.
  - Better matches real on-call workflows.
- Teacher-facing short answer:
  - "I optimized alerts for human usability; repeated non-actionable alerts are operationally harmful."

### J. Metric surface chosen for operations
- Decision made:
  - Emit per-poll summary metrics in structured logs (manifest success/error counts, processed file count, latest slice and lag by file type).
- Alternatives considered:
  - Log only raw events and derive everything later.
  - Build dashboard first, metrics second.
- Why this decision:
  - Gives immediate observability with minimal infrastructure.
  - Keeps evidence capture simple and reproducible.
  - Supports both ad-hoc debugging and soak analysis.
- Teacher-facing short answer:
  - "I built a stable metric contract first, so dashboards and evidence tooling can evolve without touching core ingest logic."

### K. Checkpoint compaction policy
- Decision made:
  - Keep only latest done checkpoints per file type (default retain 192), compact periodically (default every 10 persisted files).
- Alternatives considered:
  - Never compact checkpoints.
  - Time-based purge without file-type partitioning.
- Why this decision:
  - Prevents unbounded checkpoint growth while preserving enough restart history.
  - File-type partitioning keeps retention fair across events/mentions/articles.
- Teacher-facing short answer:
  - "Checkpoint data is operational metadata, not business history; compaction controls cost while preserving restart guarantees."

### L. Evidence-first validation approach
- Decision made:
  - Validate with both automated tests and soak evidence under calm and chaos modes.
- Alternatives considered:
  - Unit tests only.
  - Manual runtime spot checks only.
- Why this decision:
  - Tests prove local correctness; soak evidence proves system behavior over time.
  - Defense claims are backed by archived artifacts, not memory.
- Teacher-facing short answer:
  - "I used two proof layers: code-level tests and runtime evidence under failure injection."

### M. How to answer common follow-up questions
1. Why not process in parallel for speed?
   - "Parallelism was a possible optimization, but deterministic sequencing gave better correctness, easier replay, and simpler idempotency reasoning for this capstone scope."
2. Why no strict FK from mentions to events?
   - "Because vendor files are not atomic; strict FK turned expected partial/late behavior into hard failures. I preserved data flow and auditability instead."
3. How do you prove restart safety?
   - "On startup I preload durable done checkpoints, then skip already-seen `(slice_ts, file_type)` units; integration tests cover restart/idempotency and runtime logs show skip behavior."
4. Why these alert thresholds?
   - "They were tuned from calm/chaos evidence to suppress startup noise while still firing in sustained outage windows."
5. What would you improve next?
   - "Add longer automated soak runs, richer alert semantics (duration/severity), and fault injection around DB transaction boundaries."
