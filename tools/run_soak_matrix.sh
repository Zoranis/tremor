#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "Usage: $0 <database_url> [minutes]" >&2
  exit 2
fi

DATABASE_URL="$1"
MINUTES="${2:-30}"

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p data/evidence/calm data/evidence/chaos data/evidence/logs

echo "[soak] starting calm mode"
VENDOR_LATE_SLICE_RATE=0.0 \
VENDOR_PARTIAL_SLICE_RATE=0.0 \
VENDOR_STALE_MANIFEST_RATE=0.0 \
VENDOR_OUTAGE_SCHEDULE= \
docker compose up -d --no-deps --force-recreate gdelt-vendor

ts="$(date -u +%Y%m%dT%H%M%SZ)"
poller_log="data/evidence/logs/poller-${ts}.jsonl"

python3 -m src.ingest.poller \
  --database-url "$DATABASE_URL" \
  --interval-seconds 0.45 \
  --alert-manifest-error-threshold 3 \
  --alert-lag-seconds-threshold 1800 \
  > "$poller_log" 2>&1 &
POLLER_PID=$!

cleanup() {
  if kill -0 "$POLLER_PID" >/dev/null 2>&1; then
    kill "$POLLER_PID" || true
    wait "$POLLER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

python3 tools/capture_soak_evidence.py \
  --minutes "$MINUTES" \
  --interval-seconds 5 \
  --output-dir data/evidence/calm

echo "[soak] switching to chaos mode"
VENDOR_LATE_SLICE_RATE=0.05 \
VENDOR_PARTIAL_SLICE_RATE=0.03 \
VENDOR_STALE_MANIFEST_RATE=0.04 \
VENDOR_OUTAGE_SCHEDULE=03:15-03:20 \
docker compose up -d --no-deps --force-recreate gdelt-vendor

python3 tools/capture_soak_evidence.py \
  --minutes "$MINUTES" \
  --interval-seconds 5 \
  --output-dir data/evidence/chaos

echo "[soak] completed calm+chaos soak matrix"
echo "[soak] poller log: $poller_log"