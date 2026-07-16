# Design Decisions

Each decision below states what was chosen, what alternatives were considered, and why they
were rejected. Framed against data engineering fundamentals — ingestion contracts, idempotency,
schema design, fault tolerance, and observability — since that's what this project exercises.

## 1. Poll the manifest, don't guess at file arrival

**Decision:** Trigger ingestion by polling `/v2/lastupdate.txt` once a minute and trusting its
declared `bytes`/`sha1` per file, rather than polling file URLs on a timer.

**Alternatives considered:**
- A push/webhook model.
- Polling the three file URLs directly, on a fixed schedule, without a manifest.

**Why rejected:** The vendor (matching real GDELT) is poll-only — there is no push, so a
webhook design isn't buildable against this contract. Polling file URLs directly without the
manifest throws away the one thing the manifest gives you for free: expected byte length and
SHA1 to verify *before* trusting a payload. This is the classic "match the source contract,
push validation as early as possible" lesson — verifying at the edge of the system means
downstream code never has to distrust its own tables.

## 2. Deterministic processing order over parallel throughput

**Decision:** Sort manifest entries by `(slice_ts, fixed file-type order, url)` and process
them in that fixed order, single-threaded.

**Alternatives considered:**
- Process entries in whatever order the manifest lines arrive.
- Fan out and process all entries concurrently for speed.

**Why rejected:** At this data volume (tens of KB per slice), throughput was never the
bottleneck — correctness and reproducibility were. Non-deterministic ordering makes bugs
non-reproducible ("it failed on Tuesday's run but not today's") and complicates reasoning about
partial-slice states. This is a deliberate correctness-over-throughput tradeoff appropriate to
the scale: parallelism is the right call when volume demands it, not by default.

## 3. Two-signal integrity verification (byte length + SHA1), not one

**Decision:** Verify both the declared byte length and SHA1 hash of every downloaded zip before
parsing it.

**Alternatives considered:**
- Trust an HTTP 200 and parse immediately.
- Verify only one signal (bytes-only or hash-only).

**Why rejected:** Trusting a 200 status ignores that the vendor explicitly simulates corrupted
and truncated payloads (late-slice 404-then-200 races). Byte-length alone is a weak check —
truncation and corruption can coincidentally preserve length; hash-only misses nothing content
related but doesn't catch the "served slightly-wrong-sized garbage" case as cheaply. Layering a
cheap check (length) in front of an expensive one (hash) before parsing keeps invalid payloads
out of persisted tables entirely, which is cheaper than detecting bad data after it's in the
warehouse.

## 4. `(slice_ts, file_type)` as the idempotency key, not a global watermark

**Decision:** Dedup and checkpoint at the grain of one file type within one slice, both
in-memory (`seen`) and durably (`ingest_checkpoints`).

**Alternatives considered:**
- A single global high-watermark timestamp ("everything before T is done").
- Dedup relying purely on table-level upserts, with no separate checkpoint table.

**Why rejected:** A global watermark is an all-or-nothing model — it can't express "events for
slice X are done but mentions for slice X aren't yet," which is exactly what a partial-slice
chaos event produces. The dedup key has to match the vendor's actual unit of publication, or
the pipeline either reprocesses too much or silently waits forever on a file type that already
arrived. Relying only on upserts for dedup would make retries safe but wouldn't let a restarted
poller skip already-fetched work without re-downloading it — checkpointing exists specifically
to make restart cheap, not just correct.

## 5. Upsert (`ON CONFLICT ... DO UPDATE`) over insert-only or delete-and-reload

**Decision:** All three business tables (`events`, `mentions`, `articles`) are written with
upsert semantics.

**Alternatives considered:**
- Insert-only, with deduplication handled downstream (e.g., a dedup view or periodic cleanup
  job).
- Delete-and-reload the whole slice on every (re)write.

**Why rejected:** Insert-only pushes the "what's the real current row" question onto every
downstream reader — every analyst query would need a window function or a "latest row per key"
filter, which defeats the point of a clean analyst SQL surface. Delete-and-reload is wasteful
and introduces a window where a reader can see a slice half-deleted. Upsert makes retries and
replays convergent and idempotent by construction, at the (accepted) cost of losing row history
for updated records — an acceptable tradeoff since GDELT rows are append-only facts, not
frequently-revised ones.

## 6. No foreign key from `mentions.event_id` to `events.event_id`

**Decision:** `mentions` has a composite primary key `(event_id, mention_time, source_domain,
slice_ts)` but carries no FK constraint pointing at `events`.

**Alternatives considered:**
- Enforce a strict FK for referential integrity.
- Give `mentions` its own surrogate key instead of a composite natural key.

**Why rejected:** The vendor explicitly does not publish the three files atomically — a
mentions row can legitimately land before its event row during a partial-slice event. A strict
FK would turn an expected, recoverable ingestion pattern into a hard failure, which is the wrong
trade in a system whose whole premise is "the source is messy, don't fall over." This is a
conscious relaxation of textbook referential integrity in favor of ingestion continuity —
appropriate here because the two tables are still joinable and auditable later; it would be the
wrong call in a system where inconsistent joins have financial or safety consequences.

## 7. Retry with bounded exponential backoff, not zero retries or unbounded retries

**Decision:** Retry transient statuses (`429/500/502/503/504`) and connection errors with
exponential backoff, capped attempts.

**Alternatives considered:**
- No retries — treat any failed request as final.
- Aggressive/unbounded retry counts.

**Why rejected:** Zero retries turns every short vendor hiccup (the brief's late-slice and
outage chaos modes are *designed* to be transient) into a false operator-facing incident.
Unbounded or overly aggressive retries risk turning a struggling vendor into a hammered one
(retry storms), and delay forward progress on other work the poll loop should be doing. Bounded
backoff is the standard middle path: absorb turbulence without amplifying it.

## 8. Alerts fire/clear on state transition, not every poll

**Decision:** `vendor_feed_down` and `ingest_lag_high` only emit a log/telemetry event when
their active/inactive state changes.

**Alternatives considered:**
- Emit the current alert state on every poll, active or not.
- Fire without ever emitting an explicit "cleared" event.

**Why rejected:** Level-triggered logging during a multi-minute outage produces hundreds of
identical "still down" lines that bury the two facts an operator actually needs (when it broke,
when it recovered). This is a direct application of "design for the tired human at 03:00" from
the brief: the signal-to-noise ratio of the alerting channel matters as much as its correctness.
Never emitting a clear event would leave the operator unsure whether a resolved incident is
still open.

## 9. Separate observability schema from business data, expose it through views

**Decision:** Business facts (`events`/`mentions`/`articles`) live in their own tables;
manifest polls, file attempts, alert transitions, and degraded windows live in four dedicated
telemetry tables, summarized by five SQL views (`v_slice_lag`, `v_ingestion_rate_5m_by_type`,
`v_manifest_poll_health`, `v_pipeline_self_audit`, `v_slice_completion`).

**Alternatives considered:**
- Compute dashboard metrics from application-level counters/logs only, without persisting
  telemetry to the database.
- Mix operational metadata into the business tables (e.g., a status column on `events`).

**Why rejected:** Log-only metrics can't be queried live in SQL and don't survive a poller
restart, which fails both the "self-audit query" requirement and the general restart-safety
bar. Mixing operational status into business tables conflates two lifecycles that change for
different reasons and at different retention needs (a business fact should be permanent; a
checkpoint or poll record is disposable operational metadata) — keeping them apart is what
makes checkpoint compaction (see #10) possible without touching business data at all.

## 10. Checkpoint compaction instead of unbounded retention

**Decision:** `ingest_checkpoints` keeps only the most recent N `done` rows per file type
(default 192), compacted periodically rather than every write.

**Alternatives considered:**
- Never compact — keep every checkpoint forever.
- Purge checkpoints by wall-clock age instead of by count-per-file-type.

**Why rejected:** Checkpoints are operational metadata whose only job is enabling restart-safe
resume; keeping them forever grows an operationally-unbounded table for no query benefit.
Purely time-based purging would risk deleting the very checkpoint a slow file type still needs
to resume from if that file type falls behind the others — partitioning retention per file type
keeps the "how far back can I safely resume" guarantee fair across `events`/`mentions`/
`articles` independently.

## 11. Backfill missing slices from a moving manifest window, don't just alert on them

**Decision:** When the durably-completed slice is more than one step behind the manifest's
current slice, the poller reconstructs the expected `(slice_ts, file_type)` sequence and fetches
each directly by URL (the vendor still serves old slices on demand even though the manifest
stops listing them), capped at a max slices-per-poll.

**Alternatives considered:**
- Only ever process what the current manifest lists, and alert when lag is detected.
- Ask the vendor for a manifest that lists a backlog instead of just the current slice.

**Why rejected:** The manifest is a moving window — once a slice ages out, it is never relisted
— so "just alert" leaves a silent, permanent hole in the dataset whenever the poller is briefly
slower than the source (a restart, a GC pause, a slow poll). That directly violates the brief's
"survive a restart without dropping a slice" requirement. Changing the vendor's contract wasn't
an option — the whole point of the exercise is building a robust consumer against a fixed,
externally-defined contract, not renegotiating the contract. Reconstructing the sequence and
backfilling by direct URL uses a capability the vendor already exposes (any slice is servable by
URL even when unlisted), so the fix stays entirely on the consumer side.

## 12. Poll interval matched to source velocity, not left at a dev-friendly default

**Decision:** Ship two poll profiles — a fast `dev` profile matched to the vendor's accelerated
replay clock for local iteration, and a `prod` profile that polls once a minute as the brief
specifies — and make sure the manifest's "current slice only" behavior (#11) can't silently
outrun whichever profile is active.

**Alternatives considered:**
- One fixed poll interval used everywhere, dev and prod alike.
- Poll as fast as possible regardless of profile, to minimize lag in all cases.

**Why rejected:** A single fast interval tuned for local development, if left in place, works
by accident until the simulated replay speed and the poll cadence are pushed out of sync (e.g.,
a slower dev machine or a demo running a faster replay setting) — at which point the manifest's
one-current-slice window starts silently skipping slices between polls with no error, because
lag is measured against the *current* slice's timestamp, not against how many slices were
missed. Sizing the poll cadence deliberately per environment, and pairing it with the backfill
mechanism in #11 rather than trusting cadence alone, closes that gap instead of relying on the
two numbers happening to stay aligned.

## 13. Anchor "yesterday" / "now" in analyst SQL to the data's own max timestamp, not `NOW()`

**Decision:** Time-relative queries (`protest_hotspots.sql`, `theme_surge.sql`) compute
`(SELECT MAX(event_time) FROM events)` as their reference "now" instead of Postgres's `NOW()`.

**Alternatives considered:**
- Use `NOW()` directly, since that's the natural instinct for "yesterday" style queries.
- Require the operator to manually re-point the replay window to straddle the real calendar
  date before every demo.

**Why rejected:** The replay window is a fixed historical date range (e.g., 2024-01-01 through
2024-01-08) running on a simulated clock — wall-clock `NOW()` and the data's own timestamps are
only ever accidentally aligned. A query that silently returns zero rows during a live defense
because the calendar date moved on is a correctness bug wearing a demo-day costume. Anchoring to
the data's own watermark makes the query correct regardless of which historical window is being
replayed or when it's run, which is the more general and more testable fix versus a manual
"remember to reset the clock" operational step.

## 14. DB-optional poller: persistence as a pluggable callback, not a hard dependency

**Decision:** `poller.py` has no database imports; `storage.py` is only imported when
`--database-url` is passed, via a `persist_callback`/`telemetry_hooks` indirection.

**Alternatives considered:**
- Hard-couple retrieval, parsing, and DB writes into one code path.
- Always require a database connection, even for local smoke tests.

**Why rejected:** Coupling ingestion to persistence means a DB outage becomes an ingestion
outage too, and unit-testing the retrieval/verification logic would require a live Postgres
instance for every test run. Separating "fetch and validate" from "persist" follows the
classic extract/transform vs. load separation: the poller is a pure ingestion engine that stays
fast and independently testable, and persistence is attached as a swappable concern — useful
both for test speed and for isolating failure domains in production.

## 15. Evidence-first validation: soak runs under calm *and* chaos, not unit tests alone

**Decision:** Validate behavior with automated unit/integration tests plus recorded soak
evidence (`data/evidence/`) captured while the vendor's chaos modes are active, not just green
CI.

**Alternatives considered:**
- Rely on unit tests only.
- Spot-check manually during development and trust that it "seemed fine."

**Why rejected:** Unit tests prove local logic correctness but can't prove *system* behavior
over time — alert thresholds, lag under sustained chaos, and restart timing are emergent
properties that only show up in a running system. Manual spot-checking doesn't leave an
artifact anyone else (including a defense committee) can independently inspect. Archiving
timelines and summaries per run turns "trust me, I tested it" into a reproducible claim backed
by data — the same reason production systems keep runbooks and incident timelines instead of
relying on institutional memory. (This is also what caught the poll-interval/replay-speed
mismatch in #12: a soak run under the exact profile the deployed system uses, not a dev-speed
approximation of it, was needed to surface the gap at all.)
