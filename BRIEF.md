# Tremor Newsroom Desk — Project Brief

## Business context

Tremor is the data team inside a national news organisation. The newsroom's
morning editorial meeting starts at 06:30 and is the moment the day's
front-page priorities get set — what to chase, what to under-cover, who to
put in front of a camera before lunchtime. The editor running that meeting
has, for the last decade, opened it with a printed wire-service summary plus
whatever the night desk happened to flag. Tremor's mandate is to replace that
with something better: a live, queryable view of the world's event stream as
it lands, surfaced through a SQL surface the desk producers can interrogate
themselves at 06:00 without paging a data engineer.

The raw feed is GDELT 2.0 — a public firehose run out of Georgetown that
parses every news article it can scrape, attaches geographic and topical
metadata, and re-publishes the result every 15 minutes. It is loud, messy,
and large enough that nobody at Tremor wants to run it raw against an
analyst's laptop. Your job is to build the pipeline that turns the firehose
into something the morning desk trusts: a small number of well-shaped tables,
updated within minutes of GDELT publishing, with a dashboard that tells the
overnight producer whether the feed is healthy and a SQL surface the
producers can query live.

The pipeline must run 24/7. If it falls behind, the 06:30 brief goes out
blind — the editor has nothing on protests that broke overnight in
Bangkok, or on an outlet over-amplifying a story the competition is about
to lead with. The overnight desk producer is the operator who babysits the
system between 23:00 and 06:30; assume one screen, no DBA on call, tired
human. You have three weeks before you defend the system live in class.

## Data source

The vendor sidecar is already running in this scaffold as `gdelt-vendor`.
It serves the GDELT 2.0 manifest contract, on a simulated clock, against a
local cache of curated CSVs. Discovery is by polling — there is no webhook
push.

**Manifest endpoint.** Poll it once per minute (GDELT's own clients poll
once every five seconds; for a 15-minute slice cadence, once-per-minute is
fine and friendlier on the vendor).

```http
GET /v2/lastupdate.txt
→ 200 text/plain
   <bytes> <sha1> http://localhost:18200/v2/<YYYYMMDDHHMMSS>.events.csv.zip
   <bytes> <sha1> http://localhost:18200/v2/<YYYYMMDDHHMMSS>.mentions.csv.zip
   <bytes> <sha1> http://localhost:18200/v2/<YYYYMMDDHHMMSS>.articles.csv.zip
```

Three lines, one per file type, exact whitespace = single space, trailing
newline. The `sha1` is the sha1 of the zip bytes — verify after download to
catch corruption. The `bytes` is the unzipped-on-the-wire length the vendor
expects to serve. Slices land on `:00 / :15 / :30 / :45` UTC boundaries.

**File endpoint.** Stream the curated zip and decompress in your ingest:

```http
GET /v2/{YYYYMMDDHHMMSS}.{events|mentions|articles}.csv.zip
→ 200 application/zip   (one CSV member inside, with a header row)
```

Each slice publishes three small CSVs. **events.csv** (~5–15 KB):

| column           | type  | notes                                       |
|------------------|-------|---------------------------------------------|
| event_id         | int   | preserved GDELT GLOBALEVENTID               |
| event_time       | ts    | ISO-8601 UTC, minute precision              |
| actor_country    | str   | ISO-3 country code or ""                    |
| target_country   | str   | ISO-3 country code or ""                    |
| event_type       | enum  | statement / agreement / protest / clash / aid / sanction |
| intensity        | float | Goldstein scale (-10..+10), 1dp             |
| location_country | str   | ISO-3                                       |
| location_lat     | float | 2dp                                         |
| location_lon     | float | 2dp                                         |
| source_url       | str   |                                             |

**mentions.csv** (~10–30 KB) — one row per article-mentions-event:

| column         | type  | notes                              |
|----------------|-------|------------------------------------|
| event_id       | int   | joins to events.event_id           |
| mention_time   | ts    | ISO-8601 UTC, minute precision     |
| source_domain  | str   | lowercased, no protocol            |
| tone           | float | AvgTone (-100..+100), 1dp          |

**articles.csv** (~5–20 KB) — one row per article in the slice:

| column           | type  | notes                                       |
|------------------|-------|---------------------------------------------|
| article_id       | str   | opaque per-slice id (slice_ts + row_idx)    |
| article_time     | ts    | ISO-8601 UTC                                |
| source_domain    | str   |                                             |
| primary_theme    | enum  | protest / election / economy / health / conflict / disaster / sports / tech / environment / crime / military / other |
| location_country | str   | ISO-3 or ""                                 |

The vendor advances a simulated clock at a configurable replay speed so the
historical dataset behaves like a live feed. Default
`REPLAY_SECONDS_PER_SLICE=0.45` → one simulated week per ~5 wall-minutes.
Slow it down for demos via `.env`; restart `gdelt-vendor` after editing.

## Things to expect from the provider

The vendor simulates each of these on demand (see `make vendor-chaos`); your
system must cope without operator intervention.

- **Late slice.** A slice's files appear in the manifest, but `GET /v2/...`
  returns 404 for 30–90 seconds before serving cleanly. Your ingest must
  retry with backoff and not double-process when the file lands.
- **Partial slice.** The manifest only lists two of the three file types
  for a slice; the third appears in a later manifest. Don't assume the
  three files are atomic — they aren't.
- **Stale manifest.** The manifest endpoint keeps returning the prior
  slice for 1–2 polls past the expected advance. Your slice-lag alert
  must detect this and surface it to the operator.
- **Outage.** The vendor will return 503 on the manifest endpoint AND the
  file endpoint for minutes at a time. Your operational alert that says
  "the feed is gone" must fire within 60 wall-seconds of the first 503.

## Definition of done

Tremor's defining the success of this project around two concrete
deliverables, plus an operational baseline they expect of any
production-grade system you hand them.

### Operational dashboard — what the overnight producer watches

The overnight desk sits in front of one screen. Treat this as an always-on
observability surface, not a one-time report — it must work continuously,
survive the chaos modes above, and refresh itself without operator
intervention.

- **Slice lag.** Difference between the manifest's latest slice and the last
  slice your pipeline fully processed (all three file types ingested and
  loaded). Make the threshold for "fallen behind" unambiguous to a tired
  human at 03:00.
- **Per-file-type ingestion rate.** Rolling 5-minute window of slices
  successfully ingested per file type. A flat line on `articles` while
  `events` keeps ticking means a partial-slice chaos event has stuck.
- **Manifest poll health.** Success rate and latency of the last N polls.
  A spike in 503s means the vendor is in an outage window — surface it.
- **Outage alert.** Fires within 60 wall-seconds of the first 503 and
  clears within 60 wall-seconds of recovery.

### Analyst SQL surface — what the desk producers query themselves

The morning desk producers need to interrogate the data directly with SQL.
The surface you expose must be fast enough that someone can iterate during
the editorial meeting, and shaped cleanly enough that they can answer
questions you didn't anticipate.

The team should be able to answer questions like:

1. **Protest hotspots.** Yesterday's top 10 countries by event volume
   restricted to `event_type IN ('protest','clash')`. Useful for picking
   which overnight story leads.
2. **Bilateral trend.** Daily mean `intensity` for a chosen actor↔target
   country pair (e.g. USA↔CHN) over the full window — is the relationship
   trending cooperative or conflictual?
3. **Theme surge.** Top 10 `primary_theme` values by mention growth: last
   24 sim-hours vs. the trailing 7 sim-day baseline. Tells the editor
   which topics are unusually loud.
4. **Outlet amplification.** Mention-to-event ratio per `source_domain` —
   which outlets over-amplify events vs. the median? Useful when deciding
   which competitor's story to trust at face value.
5. **Geographic overlay.** Event volume per 1° lat/lon bucket overlaid
   with `primary_theme = 'protest'` article volume in the same region.
   Spots cases where the article stream is moving ahead of the event
   stream — i.e. the story is breaking now.
6. **Pipeline self-audit.** Slices where the team's pipeline detected a
   stale manifest — what fraction of the window ran degraded? Required
   reading before defending any of the answers above.

The data behind these answers must be trustworthy. A late-arriving file
must not double-count. A partial slice must not leave a phantom hole in
the joined view. A stale-manifest window must be visible in the
self-audit query.

At your defence you will provide the SQL queries that answer each of the
six questions above and run them live against your system. Bring the
queries written, prepared to explain the data model behind them and the
pipeline feeding it.

### Operational baseline

The system must come up with a single command on a fresh laptop — `make
run` in your project root — and survive a `docker compose restart` of any
one service without losing data, dropping a slice, or requiring a human to
babysit it back to a healthy state. The overnight desk producer will be
restarting things at 02:00; assume they're tired.
