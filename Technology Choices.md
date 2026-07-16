# Technology Choices

Each entry names a technology actually used in the solution and why it was picked over the
alternatives that were considered. Companion to [`Design Decisions.md`](Design%20Decisions.md),
which covers architectural/design choices rather than tooling.

## 1. PostgreSQL as the datastore

**Chosen:** PostgreSQL 16 for both business data (`events`/`mentions`/`articles`) and pipeline
observability tables.

**Alternatives considered:** A dedicated time-series database (e.g., TimescaleDB/InfluxDB) for
the metrics side; a NoSQL document store (e.g., MongoDB); SQLite for a lighter-weight local
setup.

**Why chosen:** The brief's second deliverable is explicitly a **SQL surface** producers query
directly — that alone rules out anything without a real SQL engine. A single relational
database also lets business data and observability data live side by side and be joined (e.g.,
`pipeline_self_audit.sql` correlating degraded windows with event volume), which a
split "Postgres for data, InfluxDB for metrics" setup would complicate for no real benefit at
this data volume. SQLite was rejected because the brief requires the stack to survive
`docker compose restart` of *any* service including the database — SQLite's single-file,
single-writer model is a poor fit for a service meant to run as its own container with
concurrent readers (dashboard) and a writer (poller). Postgres's `ON CONFLICT DO UPDATE`
(upsert) support is also load-bearing for the idempotency design (see Design Decisions #5).

## 2. psycopg (v3), not an ORM

**Chosen:** `psycopg[binary]` — a thin, direct Postgres driver — for all database access in
both the poller (`storage.py`) and the dashboard (`app.py`).

**Alternatives considered:** SQLAlchemy (or another ORM); a query builder library.

**Why chosen:** The write path is a small, fixed set of upserts against three known tables plus
a handful of telemetry inserts — an ORM's main value (mapping complex object graphs, migrations
tooling, engine-agnostic queries) doesn't pay for itself at this scale and adds an abstraction
layer between "what SQL actually runs" and the code, which matters when byte-for-byte
idempotency (`ON CONFLICT` clauses) is a correctness requirement, not a convenience. The
dashboard and SQL pack are Postgres-specific by design (the brief asks for a SQL surface
producers query directly against this database), so there's no portability benefit to buy with
an ORM's abstraction. Writing raw parameterized SQL keeps the query the reader sees identical to
the query that runs — important for defending exact upsert/conflict semantics live.

## 3. `requests`, not an async HTTP client

**Chosen:** The synchronous `requests` library for polling the manifest and downloading slice
files.

**Alternatives considered:** `httpx` or `aiohttp` with an async poll loop.

**Why chosen:** The poller does one thing at a time by design (see Design Decisions #2 —
deterministic, sequential processing order was chosen over concurrency for correctness and
reproducibility). An async client's entire value proposition is concurrent I/O; adopting one
while deliberately processing entries sequentially would add complexity (event loop, `async
def` propagation through `manifest.py`/`http_client.py`/`alerts.py`) with no throughput benefit
actually realized. `requests` is also simpler to unit-test with straightforward mocking, which
matters for `tests/test_poller.py`'s retry/backoff coverage.

## 4. Standard library `csv`/`zipfile`/`hashlib`, not pandas

**Chosen:** Python's built-in `csv`, `zipfile`, and `hashlib` modules to open each downloaded
zip, verify its SHA1, and parse its single CSV member into row dicts.

**Alternatives considered:** `pandas.read_csv` for parsing; a dedicated zip/checksum utility
library.

**Why chosen:** Slices are small (5–30 KB per file) — pandas's columnar/vectorized machinery is
built for a scale this pipeline never reaches, and it would add a heavy dependency and a
DataFrame-to-dict conversion step before rows could be handed to `psycopg` anyway. Hashing and
zip inspection are one-shot, well-covered stdlib operations with no missing feature that would
justify a third-party library. Staying on the standard library also keeps the poller's
dependency surface minimal, consistent with the "DB-optional poller" design (Design Decisions
#14) — fewer dependencies means fewer things that can break the ingestion path independent of
the database.

## 5. FastAPI + uvicorn for both HTTP services

**Chosen:** FastAPI (with uvicorn as the ASGI server) for both the vendor mock and the
dashboard API.

**Alternatives considered:** Flask; Django; a plain `http.server`-based service.

**Why chosen:** FastAPI wasn't actually up for debate for the vendor — it's the framework the
course scaffold ships the vendor in, so matching it for the dashboard keeps the codebase to one
web framework instead of two. Beyond that, FastAPI's automatic OpenAPI/Swagger docs (`/docs`)
were genuinely useful during development for exercising the vendor's manifest/file endpoints by
hand, and its typed request/response handling caught shape mistakes early. Flask or a bare
`http.server` would have meant hand-rolling routing, validation, and JSON responses that FastAPI
provides for free; Django's batteries (ORM, admin, templating, migrations) are aimed at a much
larger application than a two-endpoint metrics API and a small mock server.

## 6. Hand-rolled HTML/CSS/JS dashboard, not Grafana/Prometheus

**Chosen:** A static HTML page (`dashboard/templates/index.html`) with vanilla JS
(`dashboard/static/app.js`) polling a FastAPI `/metrics` endpoint and rendering SVG charts by
hand, instead of a dedicated observability stack.

**Alternatives considered:** Grafana + Prometheus (or Grafana + a Postgres data source); a
JS framework (React/Vue) frontend.

**Why chosen:** Prometheus's data model (scraped time-series counters/gauges) is a natural fit
for infrastructure metrics but is a poor fit for the brief's actual requirement — a **SQL-first**
system where the same Postgres views that back the dashboard are also the tables an analyst
queries directly. Standing up Grafana+Prometheus would mean maintaining metrics in two places
(Postgres for SQL analysis, Prometheus for the dashboard) or building a Postgres-to-Prometheus
exporter — real infrastructure for four KPI tiles and a sparkline. Grafana alone, pointed at
Postgres, was closer but still adds a service, an image, and a provisioning/dashboard-JSON
config layer to maintain for a screen simple enough to hand-build. A JS framework was rejected
for the same proportionality reason: the dashboard has one screen and no client-side routing or
component reuse need that would justify a build step and bundler.

## 7. Docker Compose for orchestration, not Kubernetes

**Chosen:** A single `compose.yml` defining all five services, brought up with `make run`.

**Alternatives considered:** Kubernetes (even a local `kind`/`minikube` cluster); running
services directly on the host with a process manager (systemd, `supervisord`) instead of
containers.

**Why chosen:** The brief's operational baseline is explicit: "come up with a single command on
a fresh laptop." Kubernetes' value — scheduling, scaling, rolling updates across a fleet — has
no target here; there is one instance of each service, on one machine, for one operator.
Bringing in Kubernetes would trade a one-command, low-dependency bring-up for cluster tooling
the project doesn't need and the overnight producer (per the brief's own persona: "one screen,
no DBA on call") could never operate. Running processes directly on the host instead of in
containers was rejected because it would leave dependency management (Python version, Postgres
install, port conflicts) to the local machine instead of the compose file, undermining "fresh
laptop" reproducibility.

## 8. Hand-written SQL files, not an ORM query builder or dbt models

**Chosen:** Six plain, human-readable `.sql` files under `sql/queries/`, run directly against
the live tables.

**Alternatives considered:** dbt models with a transformation layer; building the six answers
through an ORM's query builder in Python; materialized views refreshed on a schedule.

**Why chosen:** The brief's language is specific — "you will provide the SQL queries ... and run
them live." dbt is built around scheduled, versioned transformation pipelines materializing
into new tables, which is more infrastructure than six ad hoc analyst queries warrant, and would
add a layer between "the query" and "the SQL that runs" that works against a live defense where
the actual SQL needs to be legible and explainable. An ORM query builder was rejected for the
same reason as #2: it would render generated SQL rather than let the query itself be the
artifact under discussion. Plain (non-materialized) views were used for the metrics layer (see
`v_slice_lag` etc. in `src/schema.sql`) instead of materialized views because the dashboard and
analyst queries need current data, not a snapshot on a refresh schedule — freshness mattered
more than the query-time cost saved by materializing, at this data volume.

## 9. pytest for testing

**Chosen:** `pytest`, configured via `pyproject.toml` (`testpaths = ["tests"]`).

**Alternatives considered:** The standard library's `unittest`.

**Why chosen:** pytest's plain-function tests and fixture system made it straightforward to
parametrize timestamp-parsing and retry-behavior cases, and its auto-skip mechanism
(`pytest.mark.skipif`) is what lets `tests/test_storage_integration.py` cleanly no-op when no
test database is configured rather than erroring — important since the brief expects the suite
to run on "a fresh laptop" that may not have Postgres credentials wired up yet. `unittest`
supports skipping too, but its class-based, more verbose style buys nothing extra for this
project's mostly-function-shaped test cases.

## 10. Makefile as the task runner

**Chosen:** A `Makefile` exposing `run`, `run-dev`, `stop`, `reset`, `test`, `vendor-chaos`,
`soak-capture`, etc.

**Alternatives considered:** A collection of standalone shell scripts; a Python-based task
runner (`invoke`, `nox`); documenting raw `docker compose`/`pytest` commands in the README only.

**Why chosen:** `make` is preinstalled on essentially every Unix-like dev machine and the course
scaffold's own instructions already lean on it (`make run` is the brief's literal acceptance
command), so it needed no new dependency and matched the "single command on a fresh laptop"
requirement directly. A Python task runner would add a dependency that has to be installed
*before* the environment it's meant to help set up even exists — a bootstrapping problem `make`
doesn't have. Standalone shell scripts were rejected only for discoverability: `make help` gives
one place listing every operational command, where a folder of scripts requires already knowing
which file to run.

## 11. Structured JSON logging via the standard `logging` module

**Chosen:** Python's built-in `logging`, emitting single-line JSON records through a shared
`log_event()` helper (`poller_logging.py`), rather than a third-party structured-logging
library.

**Alternatives considered:** `structlog` or `loguru`; plain unstructured text logs.

**Why chosen:** The only requirement was "one consistent JSON shape, every module, machine-
parseable for the soak-evidence tooling" — a need fully met by wrapping stdlib `logging` calls
in one small helper. A dedicated structured-logging library would add configuration surface
(processors, renderers) for a benefit already achieved with a ~10-line function. Plain text logs
were rejected because `tools/capture_soak_evidence.py` and incident forensics both depend on
parsing log lines programmatically; unstructured text would require regex-scraping instead of
`json.loads`.

## 12. GitHub Actions for CI

**Chosen:** `.github/workflows/ci.yml`, running `make test-ci` against a Postgres service
container on every push/PR.

**Alternatives considered:** No CI (rely on local `make test` runs only); a different CI
provider (GitLab CI, CircleCI, Jenkins).

**Why chosen:** GitHub Actions ships free for a repo already hosted on GitHub and its
service-containers feature maps directly onto "spin up Postgres for the duration of the test
job" with a few lines of YAML — no separate CI account or infrastructure to provision. Skipping
CI entirely was rejected because the integration test suite specifically exercises
restart/idempotency and checkpoint-compaction behavior against a real database; without CI,
those tests only run when a developer remembers to set `TREMOR_TEST_DATABASE_URL` locally,
which is exactly the kind of "worked on my machine" gap CI exists to close.
