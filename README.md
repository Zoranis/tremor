# Tremor — newsroom story-lead desk (capstone C)

Your job is to design the system. **This scaffold only ships the upstream
vendor mock and the dataset bootstrap.** Everything else — ingest, storage,
the operational dashboard, the analyst SQL surface, monitoring — is yours.

**Start here:** [`BRIEF.md`](./BRIEF.md).

## Bring it up

```bash
make run
```

The first run downloads 7 sim-days of GDELT 2.0 (672 slices × 3 file types
≈ 2,000 HTTP requests) into a docker volume (`gdelt-cache`), curates each
raw file into a small self-explanatory CSV, and precomputes a manifest.
With 16-way parallelism (default `DATA_INIT_WORKERS=16`) this takes
**~1–3 wall-minutes** on a decent connection. Watch progress:

```bash
docker compose logs -f data-init
```

Subsequent runs reuse the volume and start in seconds. `make reset` wipes
the volume and forces a re-download.

## What's running

| Service        | Purpose                                                   | URL                           |
|----------------|-----------------------------------------------------------|-------------------------------|
| `data-init`    | One-shot: download + curate 7-day GDELT window.           | (no port; exits when done)    |
| `gdelt-vendor` | FastAPI mock of GDELT 2.0. Serves manifest + curated zips. | http://localhost:18200       |

Useful endpoints on `gdelt-vendor`:

- `GET /healthz` — `{ready, simulated_now, slices_total, slices_served}`
- `GET /docs` — OpenAPI / Swagger UI
- `GET /stats` — GET counters, current chaos rates, outage state
- `GET /v2/lastupdate.txt` — manifest: 3 lines of `<bytes> <sha1> <url>`
- `GET /v2/{YYYYMMDDHHMMSS}.{events|mentions|articles}.csv.zip` — curated zip
- `GET /simulated_now` — debug helper

### Sample calls

```bash
# Health
curl -s http://localhost:18200/healthz | jq

# Latest manifest
curl -s http://localhost:18200/v2/lastupdate.txt

# Download the latest events zip and peek at its header
url=$(curl -s http://localhost:18200/v2/lastupdate.txt | head -1 | awk '{print $3}')
curl -s -O "$url" && unzip -p "$(basename "$url")" | head -2
```

## Chaos toggle

The vendor can simulate the real-world ugliness mentioned in `BRIEF.md`
(late slice, partial slice, stale manifest, outage).

```bash
make vendor-chaos   # turn it on
make vendor-calm    # turn it off
```

Knobs live in `.env` (see comments in `.env.example` for what each does).
`make vendor-chaos` shell-exports a sensible default mix and recreates
`gdelt-vendor`; values exported at the shell win over `.env`.

## Replay speed

Default `REPLAY_SECONDS_PER_SLICE=0.45` means one 15-min slice per 0.45
wall-seconds — about 8 simulated minutes per wall-second, or one sim-week
per 5 wall-minutes. If your pipeline can't keep up, raise the value in
`.env` (1.0 is reasonable for early development) and restart
`gdelt-vendor`.

## Adding your own services

The scaffold deliberately ships a thin `compose.yml`. Add services as you
need them — copy snippets from the course's `labs/streaming/compose.yml`,
`labs/monitoring/compose.yml`, etc. There's no opinionated module layout
under `src/` — design as a team and make your choices defensible.
