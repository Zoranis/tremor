#!/usr/bin/env python3
"""data-init sidecar entrypoint.

Downloads a contiguous range of GDELT 2.0 slices (events/mentions/gkg) and
curates each raw file into a much smaller, self-explanatory CSV, writing the
following into the named volume:

  <cache_dir>/raw/<YYYYMMDDHHMMSS>.export.CSV.zip       raw download (ephemeral)
  <cache_dir>/raw/<YYYYMMDDHHMMSS>.mentions.CSV.zip
  <cache_dir>/raw/<YYYYMMDDHHMMSS>.gkg.csv.zip
  <cache_dir>/curated/<YYYYMMDDHHMMSS>.events.csv.zip   curated zipped CSV
  <cache_dir>/curated/<YYYYMMDDHHMMSS>.mentions.csv.zip
  <cache_dir>/curated/<YYYYMMDDHHMMSS>.articles.csv.zip
  <cache_dir>/curated/_manifests.json                   per-slice bytes+sha1
  <cache_dir>/_READY                                    sentinel — last

Idempotent: if <cache_dir>/_READY exists, exits 0 immediately.

Design choices (recorded so future readers don't re-litigate them):
  * Raw GDELT is served as TAB-separated CSVs (despite the `.CSV` extension)
    with no header row; column order is documented at gdeltproject.org. We
    parse with stdlib `csv.reader(delimiter='\\t')` — pyarrow's CSV reader
    chokes on the ragged GKG rows.
  * Curated columns deliberately drop CAMEO codes, the 30-field actor
    signature, and the kitchen-sink GKG fields. The capstone is about the
    pipeline, not about decoding GDELT.
  * FIPS country codes (what GDELT uses) are converted to ISO-3 via a small
    baked lookup (~70 entries). Unknown FIPS → empty string.
  * The vendor serves the curated zips verbatim; we precompute sha1+size
    per slice in `_manifests.json` so the manifest endpoint is O(1).
  * Raw zips are kept under /cache/raw/ on first download to allow resume
    after a partial-failure restart. If disk pressure is a concern, the
    raw cache can be wiped post-curation (left in place by default for
    debuggability).
"""
from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import os
import sys
import time
import urllib.request
import urllib.error
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable


# Network is the bottleneck — 7 sim-days × 96 slices × 3 file types = ~2k HTTP
# GETs against GDELT's CDN. At ~200 ms per request, sequential would take
# 5–10 min; 16-way parallelism drops it to under a wall-minute. Bumping
# higher gives diminishing returns and risks tripping GDELT's rate limiter.
DATA_INIT_WORKERS = int(os.environ.get("DATA_INIT_WORKERS", "16"))


GDELT_BASE = "http://data.gdeltproject.org/gdeltv2"

# GDELT 2.0 publishes on :00/:15/:30/:45 boundaries.
SLICE_MINUTES = 15

# File types served per slice.
FILE_TYPES = ("events", "mentions", "articles")

# (file_type → (raw_suffix, curated_filename_template))
_RAW_SUFFIX = {
    "events":   "export.CSV.zip",
    "mentions": "mentions.CSV.zip",
    "articles": "gkg.csv.zip",
}


def log(msg: str) -> None:
    print(f"[data-init] {msg}", flush=True)


# ───────────────────────── Env / config ─────────────────────────────────────

def parse_window_env() -> tuple[date, date]:
    start = date.fromisoformat(os.environ.get("REPLAY_WINDOW_START", "2024-01-01"))
    end = date.fromisoformat(os.environ.get("REPLAY_WINDOW_END", "2024-01-08"))
    if end < start:
        raise SystemExit(f"REPLAY_WINDOW_END ({end}) is before REPLAY_WINDOW_START ({start})")
    return start, end


def slice_timestamps(window_start: date, window_end: date) -> list[str]:
    """Yield 'YYYYMMDDHHMMSS' strings for every 15-min slice in the window
    (inclusive at both ends, so window_end is the last *day* covered)."""
    out: list[str] = []
    start = datetime.combine(window_start, datetime.min.time()).replace(tzinfo=timezone.utc)
    # Inclusive of the whole end-day → run up to (end + 1 day) exclusive.
    end = datetime.combine(window_end, datetime.min.time()).replace(tzinfo=timezone.utc) + timedelta(days=1)
    cur = start
    step = timedelta(minutes=SLICE_MINUTES)
    while cur < end:
        out.append(cur.strftime("%Y%m%d%H%M%S"))
        cur += step
    return out


# ───────────────────────── FIPS → ISO-3 lookup ──────────────────────────────

FIPS_TO_ISO3 = {
    "US": "USA", "UK": "GBR", "CH": "CHN", "RS": "RUS", "FR": "FRA",
    "GM": "DEU", "IS": "ISR", "IZ": "IRQ", "IR": "IRN", "SY": "SYR",
    "EG": "EGY", "SA": "SAU", "AE": "ARE", "TU": "TUR", "IN": "IND",
    "JA": "JPN", "KS": "KOR", "KN": "PRK", "TW": "TWN", "VM": "VNM",
    "ID": "IDN", "AS": "AUS", "NZ": "NZL", "CA": "CAN", "MX": "MEX",
    "BR": "BRA", "AR": "ARG", "NI": "NGA", "SF": "ZAF", "KE": "KEN",
    "UP": "UKR", "PL": "POL", "IT": "ITA", "SP": "ESP", "NL": "NLD",
    "BE": "BEL", "SZ": "CHE", "AU": "AUT", "SW": "SWE", "NO": "NOR",
    "FI": "FIN", "DA": "DNK", "EZ": "CZE", "HU": "HUN", "RO": "ROU",
    "BU": "BGR", "GR": "GRC", "EN": "EST", "LH": "LTU", "LG": "LVA",
    "PK": "PAK", "BG": "BGD", "AF": "AFG", "JO": "JOR", "LE": "LBN",
    "MO": "MAR", "AG": "DZA", "LY": "LBY", "SU": "SDN", "ET": "ETH",
    "UG": "UGA", "TZ": "TZA", "RW": "RWA", "BL": "BLR", "MD": "MDA",
    "KZ": "KAZ", "UZ": "UZB", "AJ": "AZE", "AM": "ARM", "GG": "GEO",
}


def fips_to_iso3(fips: str) -> str:
    if not fips:
        return ""
    return FIPS_TO_ISO3.get(fips.strip().upper(), "")


# ───────────────────────── Curation maps ────────────────────────────────────

# CAMEO EventRootCode (2-digit string "01".."20") → curated event_type enum.
def cameo_root_to_event_type(root: str) -> str:
    if not root:
        return "statement"
    r = root.strip().zfill(2)
    if r in ("01", "02"):
        return "statement"
    if r in ("03", "04"):
        return "agreement"
    if r in ("07", "08"):
        return "aid"
    if r in ("11", "12", "13"):
        return "sanction"
    if r == "14":
        return "protest"
    if r in ("18", "19", "20"):
        return "clash"
    return "statement"


# GKG V1THEMES → primary_theme. Priority-ordered: first match wins.
_THEME_RULES: list[tuple[str, tuple[str, ...]]] = [
    ("protest",     ("PROTEST", "STRIKE")),
    ("election",    ("ELECTION", "DEMOCRACY")),
    ("conflict",    ("MILITARY", "ARMEDCONFLICT", "KILL")),
    ("military",    ("MIL_", "WEAPONS")),
    ("disaster",    ("NATURAL_DISASTER", "DISASTER")),
    ("health",      ("HEALTH", "DISEASE", "EPIDEMIC", "MEDICAL")),
    ("economy",     ("ECON_", "TAX", "INFLATION", "TRADE")),
    ("crime",       ("CRIME", "CORRUPTION", "TERROR")),
    ("environment", ("ENV_", "CLIMATE")),
    ("tech",        ("TECH_", "INTERNET", "CYBER")),
    ("sports",      ("SPORTS",)),
]


def gkg_themes_to_primary(themes_field: str) -> str:
    """`themes_field` is a semicolon-delimited list of theme tags.

    We don't care about offsets — the V1THEMES field is plain `;`-delimited;
    V2 themes carry `,offset` suffixes. Either way, substring match on the
    upper-cased blob picks up the rule.
    """
    if not themes_field:
        return "other"
    blob = themes_field.upper()
    for label, needles in _THEME_RULES:
        for n in needles:
            if n in blob:
                return label
    return "other"


# ───────────────────────── HTTP + zip helpers ───────────────────────────────

def _http_get_bytes(url: str, timeout: float = 60.0) -> bytes | None:
    """GET with a small retry. Returns None on 404 (slice missing); raises on
    other persistent errors so the caller can decide whether to abort."""
    last_err: Exception | None = None
    for attempt in range(4):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "tremor-data-init/0.1"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            last_err = e
        except Exception as e:
            last_err = e
        time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed after retries: {last_err}")


def _extract_member(zip_bytes: bytes) -> bytes:
    """GDELT zips contain exactly one CSV member. Return its raw bytes."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        names = zf.namelist()
        if not names:
            raise RuntimeError("zip is empty")
        return zf.read(names[0])


# ───────────────────────── Curation ─────────────────────────────────────────

# Column indices (0-based) in the raw GDELT files. Documented in the GDELT 2.0
# codebook; baked in here to avoid shipping the spec.
COL_EV_GLOBALEVENTID    = 0
COL_EV_SQLDATE          = 1
COL_EV_ACTOR1CC         = 7    # Actor1CountryCode (FIPS)
COL_EV_ACTOR2CC         = 15   # Actor2CountryCode (FIPS)
COL_EV_EVENTROOTCODE    = 28   # CAMEO root, "01".."20"
COL_EV_QUADCLASS        = 29
COL_EV_GOLDSTEIN        = 30
COL_EV_AVGTONE          = 34
COL_EV_ACTIONGEO_CC     = 51
COL_EV_ACTIONGEO_LAT    = 53
COL_EV_ACTIONGEO_LONG   = 54
COL_EV_DATEADDED        = 57
COL_EV_SOURCEURL        = 60

COL_MN_GLOBALEVENTID    = 0
COL_MN_MENTIONTIMEDATE  = 2
COL_MN_MENTIONSOURCE    = 4
COL_MN_MENTIONIDENT     = 5
COL_MN_MENTIONDOCTONE   = 13

COL_GK_GKGRECORDID      = 0
COL_GK_V21DATE          = 1
COL_GK_SRCCOMMONNAME    = 3
COL_GK_DOCIDENT         = 4
COL_GK_V1THEMES         = 7


def _parse_gdelt_ts(raw: str) -> str:
    """'YYYYMMDDHHMMSS' → 'YYYY-MM-DDTHH:MM:00Z' (minute precision)."""
    if not raw or len(raw) < 12:
        return ""
    try:
        dt = datetime.strptime(raw[:14].ljust(14, "0"), "%Y%m%d%H%M%S")
        return dt.strftime("%Y-%m-%dT%H:%M:00Z")
    except ValueError:
        return ""


def _parse_sqldate(raw: str) -> str:
    """'YYYYMMDD' → 'YYYY-MM-DDT00:00:00Z'."""
    if not raw or len(raw) < 8:
        return ""
    try:
        return datetime.strptime(raw[:8], "%Y%m%d").strftime("%Y-%m-%dT00:00:00Z")
    except ValueError:
        return ""


def _f(raw: str, default: float = 0.0, ndigits: int = 1) -> float:
    if not raw:
        return default
    try:
        return round(float(raw), ndigits)
    except ValueError:
        return default


def _domain(url: str) -> str:
    """Cheap, dependency-free domain extraction. Lowercased, no protocol."""
    if not url:
        return ""
    s = url.lower().strip()
    if "://" in s:
        s = s.split("://", 1)[1]
    s = s.split("/", 1)[0]
    if "@" in s:
        s = s.split("@", 1)[1]
    if ":" in s:
        s = s.split(":", 1)[0]
    return s


def curate_events(raw_csv: bytes, slice_ts: str) -> bytes:
    """Raw events.CSV → curated events.csv bytes (UTF-8, comma-separated, with header)."""
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow([
        "event_id", "event_time", "actor_country", "target_country",
        "event_type", "intensity", "location_country",
        "location_lat", "location_lon", "source_url",
    ])
    text = raw_csv.decode("utf-8", errors="replace")
    rdr = csv.reader(io.StringIO(text), delimiter="\t")
    for row in rdr:
        if len(row) < 61:
            continue
        # Drop rows with no QuadClass (real-world ~5% of raw).
        if not row[COL_EV_QUADCLASS].strip():
            continue
        # event_time: prefer DATEADDED (the slice timestamp) → fall back to SQLDATE.
        event_time = _parse_gdelt_ts(row[COL_EV_DATEADDED]) or _parse_sqldate(row[COL_EV_SQLDATE])
        if not event_time:
            continue
        try:
            event_id = int(row[COL_EV_GLOBALEVENTID])
        except ValueError:
            continue
        lat = _f(row[COL_EV_ACTIONGEO_LAT], 0.0, 2)
        lon = _f(row[COL_EV_ACTIONGEO_LONG], 0.0, 2)
        w.writerow([
            event_id,
            event_time,
            fips_to_iso3(row[COL_EV_ACTOR1CC]),
            fips_to_iso3(row[COL_EV_ACTOR2CC]),
            cameo_root_to_event_type(row[COL_EV_EVENTROOTCODE]),
            _f(row[COL_EV_GOLDSTEIN], 0.0, 1),
            fips_to_iso3(row[COL_EV_ACTIONGEO_CC]),
            lat,
            lon,
            row[COL_EV_SOURCEURL].strip(),
        ])
    return out.getvalue().encode("utf-8")


def curate_mentions(raw_csv: bytes, slice_ts: str) -> bytes:
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["event_id", "mention_time", "source_domain", "tone"])
    text = raw_csv.decode("utf-8", errors="replace")
    rdr = csv.reader(io.StringIO(text), delimiter="\t")
    for row in rdr:
        if len(row) < 16:
            continue
        try:
            event_id = int(row[COL_MN_GLOBALEVENTID])
        except ValueError:
            continue
        mention_time = _parse_gdelt_ts(row[COL_MN_MENTIONTIMEDATE])
        if not mention_time:
            continue
        domain = (row[COL_MN_MENTIONSOURCE] or "").strip().lower()
        if not domain:
            domain = _domain(row[COL_MN_MENTIONIDENT])
        w.writerow([
            event_id,
            mention_time,
            domain,
            _f(row[COL_MN_MENTIONDOCTONE], 0.0, 1),
        ])
    return out.getvalue().encode("utf-8")


def curate_articles(raw_csv: bytes, slice_ts: str) -> bytes:
    """GKG → articles. article_id = slice_ts + row index (opaque, stable)."""
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow([
        "article_id", "article_time", "source_domain",
        "primary_theme", "location_country",
    ])
    text = raw_csv.decode("utf-8", errors="replace")
    rdr = csv.reader(io.StringIO(text), delimiter="\t")
    row_idx = 0
    for row in rdr:
        if len(row) < 8:
            # GKG rows are wide (27 cols nominally) but ragged; tolerate
            # anything that has the columns we actually read.
            continue
        article_time = _parse_gdelt_ts(row[COL_GK_V21DATE])
        if not article_time:
            continue
        domain = (row[COL_GK_SRCCOMMONNAME] or "").strip().lower()
        if not domain:
            domain = _domain(row[COL_GK_DOCIDENT])
        # GKG has no single location country field in V2.1; leave empty.
        # (V2 LOCATIONS is semicolon-delimited tuples; not worth parsing for
        # the curated surface — students still get protest/event geo from
        # events.csv.)
        article_id = f"{slice_ts}_{row_idx:05d}"
        w.writerow([
            article_id,
            article_time,
            domain,
            gkg_themes_to_primary(row[COL_GK_V1THEMES]),
            "",
        ])
        row_idx += 1
    return out.getvalue().encode("utf-8")


CURATORS = {
    "events":   curate_events,
    "mentions": curate_mentions,
    "articles": curate_articles,
}


# ───────────────────────── Slice driver ─────────────────────────────────────

def _zip_curated_csv(csv_bytes: bytes, member_name: str) -> bytes:
    """Wrap curated CSV bytes in a ZIP container matching GDELT's shape."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(member_name, csv_bytes)
    return buf.getvalue()


def process_slice(cache_dir: Path, slice_ts: str) -> dict[str, dict] | None:
    """Download + curate one slice. Idempotent per file. Returns a per-type
    {bytes, sha1} manifest entry, or None if the slice can't be assembled
    (e.g. all three files 404)."""
    raw_dir = cache_dir / "raw"
    cur_dir = cache_dir / "curated"
    raw_dir.mkdir(parents=True, exist_ok=True)
    cur_dir.mkdir(parents=True, exist_ok=True)

    manifest: dict[str, dict] = {}

    for ftype in FILE_TYPES:
        raw_suffix = _RAW_SUFFIX[ftype]
        raw_name = f"{slice_ts}.{raw_suffix}"
        cur_name = f"{slice_ts}.{ftype}.csv.zip"
        raw_path = raw_dir / raw_name
        cur_path = cur_dir / cur_name

        if cur_path.exists():
            # Already curated; just record manifest entry.
            blob = cur_path.read_bytes()
            manifest[ftype] = {"bytes": len(blob), "sha1": hashlib.sha1(blob).hexdigest()}
            continue

        # Need raw.
        if not raw_path.exists():
            url = f"{GDELT_BASE}/{raw_name}"
            data = _http_get_bytes(url)
            if data is None:
                # Slice missing on the upstream — skip this file type.
                continue
            raw_path.write_bytes(data)

        try:
            raw_csv = _extract_member(raw_path.read_bytes())
        except Exception as e:
            log(f"  WARN: extract failed for {raw_name}: {e}")
            continue

        try:
            curated_bytes = CURATORS[ftype](raw_csv, slice_ts)
        except Exception as e:
            log(f"  WARN: curate failed for {raw_name}: {e}")
            continue

        # Member name inside the curated zip: match GDELT's naming for
        # familiarity (e.g. `<slice>.events.CSV`).
        member_name = f"{slice_ts}.{ftype}.CSV"
        zipped = _zip_curated_csv(curated_bytes, member_name)
        cur_path.write_bytes(zipped)
        manifest[ftype] = {"bytes": len(zipped), "sha1": hashlib.sha1(zipped).hexdigest()}

    if not manifest:
        return None
    return manifest


# ───────────────────────── Entrypoint ───────────────────────────────────────

def main(cache_dir: Path) -> int:
    cache_dir.mkdir(parents=True, exist_ok=True)
    ready = cache_dir / "_READY"
    if ready.exists():
        log(f"{ready} exists — skipping (idempotent re-run)")
        return 0

    window_start, window_end = parse_window_env()
    timestamps = slice_timestamps(window_start, window_end)
    log(f"replay window: {window_start} → {window_end}; {len(timestamps)} slices × 3 files")

    manifests: dict[str, dict[str, dict]] = {}
    started = time.monotonic()
    last_log = started
    completed = 0
    missing = 0

    # Parallelise across slices. Each worker downloads + curates one slice
    # end-to-end (download is I/O-bound, curation is small CPU work — sharing
    # a thread is fine). Progress is logged from the main thread as futures
    # resolve in completion order.
    log(f"downloading + curating with {DATA_INIT_WORKERS} workers")
    with ThreadPoolExecutor(max_workers=DATA_INIT_WORKERS) as pool:
        future_to_ts = {pool.submit(process_slice, cache_dir, ts): ts for ts in timestamps}
        for fut in as_completed(future_to_ts):
            ts = future_to_ts[fut]
            try:
                entry = fut.result()
            except Exception as e:
                log(f"  WARN: slice {ts} failed: {e}")
                entry = None
            if entry is None:
                missing += 1
            else:
                manifests[ts] = entry
            completed += 1
            now = time.monotonic()
            if now - last_log >= 5:
                rate = completed / max(now - started, 0.001)
                eta = (len(timestamps) - completed) / max(rate, 1e-6)
                log(f"  slice {completed}/{len(timestamps)} ({rate:.1f}/s, ETA {eta/60:.1f} min); missing={missing}")
                last_log = now

    if not manifests:
        raise SystemExit("zero slices curated — check REPLAY_WINDOW_* and connectivity")

    # Persist per-slice manifest so the vendor doesn't have to recompute
    # sha1+size on every request.
    out_manifests = cache_dir / "curated" / "_manifests.json"
    out_manifests.parent.mkdir(parents=True, exist_ok=True)
    out_manifests.write_text(json.dumps(manifests, indent=2, sort_keys=True))
    log(f"wrote {out_manifests} ({len(manifests):,} slices)")

    ready.write_text(datetime.now(timezone.utc).isoformat() + "\n")
    log(f"_READY at {ready}")
    return 0


if __name__ == "__main__":
    target = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("/cache")
    sys.exit(main(target))
