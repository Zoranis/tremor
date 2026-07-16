# Solution Description

Tremor ingests the simulated GDELT 2.0 feed and turns it into a trustworthy, always-on
newsroom system with two deliverables required by `BRIEF.md`: an operational dashboard and
an analyst SQL surface — plus the restart-safety and chaos-tolerance baseline demanded of a
production pipeline.

**Pipeline.** An ingestion poller (`src/ingest/`) polls the vendor's manifest once a minute,
downloads each slice's three CSVs, verifies them by byte-length and SHA1, and upserts rows
into Postgres (`events`, `mentions`, `articles`). A `(slice_ts, file_type)` checkpoint table
makes ingestion idempotent and restart-safe — a killed poller resumes without dropping or
duplicating a slice. The poller also reconstructs and backfills any slices it fell behind on,
since the manifest window moves and never re-lists old slices.

**Resilience.** The pipeline is built to survive the four chaos modes the vendor injects —
late slices, partial slices (files not atomic across types), stale manifests, and outages —
via retry-with-backoff, per-file-type completion tracking (no phantom holes from partial
slices), and stateful fire/clear alerting so a degraded state is only logged when it changes.

**Operational dashboard.** A FastAPI service (`dashboard/`) exposes a `/metrics` endpoint and
a single-screen UI showing the four signals the brief requires: slice lag, per-file-type
5-minute ingestion rate, manifest poll health, and an outage alert that fires/clears within 60
seconds of the underlying vendor state change.

**Analyst SQL surface.** Six SQL queries (`sql/queries/`) run directly against the ingested
tables and answer the brief's six editorial questions: protest hotspots, bilateral trend,
theme surge, outlet amplification, geographic overlay, and pipeline self-audit (what fraction
of the window ran degraded).

**Operational baseline.** `make run` brings up the full stack (vendor, Postgres, poller,
dashboard) on a fresh machine; `docker compose restart` of any one service is safe by design
thanks to upserts, checkpointing, and idempotent schema setup — no human intervention needed
to recover.

See [`SOLUTION.md`](SOLUTION.md) for the full component-by-component walkthrough.
