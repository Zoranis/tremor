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

.PHONY: run run-dev run-prod-profile stop reset logs logs-dashboard test test-integration test-ci soak-capture validate-restart vendor-chaos vendor-calm help

help:
	@echo ""
	@echo "  make run            Build and start full stack (vendor + postgres + ingest + dashboard)"
	@echo "  make run-dev        Start full stack with fast polling profile"
	@echo "  make run-prod-profile  Start full stack with 60s polling profile"
	@echo "  make stop           Stop containers (keeps the gdelt-cache volume)"
	@echo "  make reset          Stop + wipe volumes (next run re-downloads + re-curates)"
	@echo "  make logs           Tail gdelt-vendor logs"
	@echo "  make logs-dashboard Tail dashboard logs"
	@echo "  make test           Run the current automated test suite"
	@echo "  make test-integration  Run DB integration tests (requires TREMOR_TEST_DATABASE_URL)"
	@echo "  make test-ci        Run full test suite for CI (requires TREMOR_TEST_DATABASE_URL)"
	@echo "  make soak-capture   Capture vendor health/manifest evidence into data/evidence"
	@echo "  make validate-restart Validate a docker compose restart path"
	@echo "  make vendor-chaos   Restart gdelt-vendor with late/partial/stale/outage on"
	@echo "  make vendor-calm    Restart gdelt-vendor with chaos all-zero"
	@echo ""
	@echo "  Vendor API:      http://localhost:18200/docs"
	@echo "  Dashboard API:   http://localhost:18600/metrics"
	@echo "  Postgres host:   localhost:15432 (db/user/pass: tremor)"
	@echo "  Healthcheck: http://localhost:18200/healthz"
	@echo ""

run:
	INGEST_POLL_PROFILE=prod docker compose up -d --build
	@echo ""
	@echo "=============================================================="
	@echo " Tremor full stack is starting (prod profile)."
	@echo "   Poll cadence: 60s"
	@echo "   Vendor docs:    http://localhost:18200/docs"
	@echo "   Dashboard API:  http://localhost:18600/metrics"
	@echo "   Postgres:       localhost:15432"
	@echo "=============================================================="

run-dev:
	INGEST_POLL_PROFILE=dev docker compose up -d --build
	@echo "[dev] Full stack started with fast poll profile (0.45s default)."

run-prod-profile:
	INGEST_POLL_PROFILE=prod docker compose up -d --build
	@echo ""
	@echo "=============================================================="
	@echo " Tremor full stack is starting (prod profile)."
	@echo "   Poll cadence: 60s"
	@echo "   First run downloads + curates 7 sim-days of GDELT (5-15 min)."
	@echo "   Watch progress:"
	@echo "     docker compose logs -f data-init"
	@echo "   Once healthy:"
	@echo "     curl http://localhost:18200/healthz"
	@echo "     curl http://localhost:18200/v2/lastupdate.txt"
	@echo "     curl http://localhost:18600/metrics"
	@echo "=============================================================="

stop:
	docker compose down --remove-orphans

reset:
	docker compose down -v --remove-orphans

logs:
	docker compose logs -f gdelt-vendor

logs-dashboard:
	docker compose logs -f dashboard

test:
	python -m pytest -q

test-integration:
	@if [ -z "$$TREMOR_TEST_DATABASE_URL" ]; then \
		echo "TREMOR_TEST_DATABASE_URL is required"; \
		exit 1; \
	fi
	python -m pytest -q tests/test_storage_integration.py

test-ci:
	@if [ -z "$$TREMOR_TEST_DATABASE_URL" ]; then \
		echo "TREMOR_TEST_DATABASE_URL is required"; \
		exit 1; \
	fi
	python -m pytest -q

soak-capture:
	python tools/capture_soak_evidence.py --minutes 30 --interval-seconds 5 --output-dir data/evidence

validate-restart:
	python tools/validate_restart_safety.py

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
