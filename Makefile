# ─── Tremor capstone project ────────────────────────────────────────────────
#
# Run from the project root. The scaffold ships only the upstream:
#   * data-init    — one-shot sidecar that downloads + curates a 7-day GDELT
#                    window into a named docker volume. ~5-15 min on first
#                    run (~2,000 HTTP requests). Idempotent — exits in <2s
#                    after that.
#   * gdelt-vendor — FastAPI service that serves the GDELT manifest
#                    (/v2/lastupdate.txt) and the three curated CSVs per
#                    15-minute slice, advancing through the historical
#                    window at configurable replay speed.
#
# Everything else (ingest, storage, dashboards, alerting, monitoring) is yours
# to design. Add services to compose.yml as you need them.
# ────────────────────────────────────────────────────────────────────────────

.PHONY: run stop reset logs vendor-chaos vendor-calm help

help:
	@echo ""
	@echo "  make run            Build vendor image, run data-init, start gdelt-vendor"
	@echo "  make stop           Stop containers (keeps the gdelt-cache volume)"
	@echo "  make reset          Stop + wipe volumes (next run re-downloads + re-curates)"
	@echo "  make logs           Tail gdelt-vendor logs"
	@echo "  make vendor-chaos   Restart gdelt-vendor with late/partial/stale/outage on"
	@echo "  make vendor-calm    Restart gdelt-vendor with chaos all-zero"
	@echo ""
	@echo "  Vendor API:  http://localhost:18200/docs"
	@echo "  Healthcheck: http://localhost:18200/healthz"
	@echo ""

run:
	docker compose up -d --build
	@echo ""
	@echo "=============================================================="
	@echo " Tremor vendor mock is starting."
	@echo "   First run downloads + curates 7 sim-days of GDELT (5-15 min)."
	@echo "   Watch progress:"
	@echo "     docker compose logs -f data-init"
	@echo "   Once gdelt-vendor is healthy:"
	@echo "     curl http://localhost:18200/healthz"
	@echo "     curl http://localhost:18200/v2/lastupdate.txt"
	@echo "     open http://localhost:18200/docs"
	@echo "=============================================================="

stop:
	docker compose down --remove-orphans

reset:
	docker compose down -v --remove-orphans

logs:
	docker compose logs -f gdelt-vendor

vendor-chaos:
	VENDOR_LATE_SLICE_RATE=0.05 \
	VENDOR_PARTIAL_SLICE_RATE=0.03 \
	VENDOR_STALE_MANIFEST_RATE=0.04 \
	VENDOR_OUTAGE_SCHEDULE=03:15-03:20 \
	docker compose up -d --no-deps --force-recreate gdelt-vendor
	@echo "[chaos] gdelt-vendor restarted with late/partial/stale/outage on."

vendor-calm:
	VENDOR_LATE_SLICE_RATE=0.0 \
	VENDOR_PARTIAL_SLICE_RATE=0.0 \
	VENDOR_STALE_MANIFEST_RATE=0.0 \
	VENDOR_OUTAGE_SCHEDULE= \
	docker compose up -d --no-deps --force-recreate gdelt-vendor
	@echo "[calm] gdelt-vendor restarted with chaos disabled."
