import inspect
import io
import logging
import sys
import types
import zipfile
from datetime import timezone

import pytest
import requests

from src.ingest import poller


class _FakeResponse:
    def __init__(self, *, status_code: int, text: str, json_payload=None, content: bytes | None = None):
        self.status_code = status_code
        self.text = text
        self._json_payload = json_payload
        self.content = text.encode("utf-8") if content is None else content

    def json(self):
        if self._json_payload is None:
            raise ValueError("no json body")
        return self._json_payload


class _FakeSession:
    def __init__(self, responses_by_url):
        self._responses_by_url = responses_by_url

    def get(self, url, timeout):
        response = self._responses_by_url.get(url)
        if response is None:
            raise AssertionError(f"unexpected URL requested: {url}")
        return response


class _SequenceSession:
    def __init__(self, responses_by_url):
        self._responses_by_url = {k: list(v) for k, v in responses_by_url.items()}

    def get(self, url, timeout):
        items = self._responses_by_url.get(url)
        if not items:
            raise AssertionError(f"unexpected URL requested: {url}")
        item = items.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class _ContextSequenceSession(_SequenceSession):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def _test_logger() -> logging.Logger:
    logger = logging.getLogger("tremor.ingest.poller.tests")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger


def test_parse_manifest_orders_by_slice_then_file_type():
    logger = _test_logger()
    manifest_text = "\n".join(
        [
            "111 1111111111111111111111111111111111111111 http://localhost:18200/v2/20240101101500.articles.csv.zip",
            "111 1111111111111111111111111111111111111111 http://localhost:18200/v2/20240101100000.mentions.csv.zip",
            "111 1111111111111111111111111111111111111111 http://localhost:18200/v2/20240101100000.events.csv.zip",
            "111 1111111111111111111111111111111111111111 http://localhost:18200/v2/20240101100000.articles.csv.zip",
            "111 1111111111111111111111111111111111111111 http://localhost:18200/v2/20240101101500.events.csv.zip",
        ]
    )

    entries = poller.parse_manifest(manifest_text, logger)

    ordered = [(e.slice_ts, e.file_type) for e in entries]
    assert ordered == [
        ("20240101100000", "events"),
        ("20240101100000", "mentions"),
        ("20240101100000", "articles"),
        ("20240101101500", "events"),
        ("20240101101500", "articles"),
    ]


def test_parse_timestamp_accepts_compact_and_isoz():
    compact = poller._parse_timestamp("20240101101500")
    isoz = poller._parse_timestamp("2024-01-01T10:15:00Z")

    assert compact is not None
    assert isoz is not None
    assert compact.tzinfo == timezone.utc
    assert isoz.tzinfo is not None
    assert compact == isoz


def test_poll_once_emits_lag_check_with_expected_lag_seconds(monkeypatch):
    logger = _test_logger()
    events = []

    def _capture_log_event(logger, **kwargs):
        events.append(kwargs)

    def _fake_download_and_verify(
        session,
        entry,
        logger,
        timeout_seconds,
        max_retries,
        backoff_initial_seconds,
        backoff_max_seconds,
        telemetry_hooks=None,
    ):
        return b"zip-bytes"

    def _fake_parse_zip_csv(zip_bytes, *, slice_ts, file_type, logger):
        return [{"ok": "1"}]

    monkeypatch.setattr(poller, "log_event", _capture_log_event)
    monkeypatch.setattr(poller, "_download_and_verify", _fake_download_and_verify)
    monkeypatch.setattr(poller, "_parse_zip_csv", _fake_parse_zip_csv)

    manifest_url = "http://localhost:18200/v2/lastupdate.txt"
    simulated_now_url = "http://localhost:18200/simulated_now"
    manifest_text = (
        "100 1111111111111111111111111111111111111111 "
        "http://localhost:18200/v2/20240101100000.events.csv.zip\n"
    )

    session = _FakeSession(
        {
            manifest_url: _FakeResponse(status_code=200, text=manifest_text),
            simulated_now_url: _FakeResponse(
                status_code=200,
                text='{"simulated_now":"2024-01-01T10:15:00Z"}',
                json_payload={"simulated_now": "2024-01-01T10:15:00Z"},
            ),
        }
    )

    results = poller.poll_once(
        session,
        base_url="http://localhost:18200",
        seen=set(),
        logger=logger,
        timeout_seconds=5.0,
    )

    assert len(results) == 1
    lag_events = [e for e in events if e.get("action") == "lag_check" and e.get("result") == "ok"]
    assert len(lag_events) == 1
    assert lag_events[0]["slice_ts"] == "20240101100000"
    assert lag_events[0]["simulated_now"] == "2024-01-01T10:15:00Z"
    assert lag_events[0]["lag_seconds"] == 900.0


def test_poll_once_emits_poll_metrics_summary_on_success(monkeypatch):
    logger = _test_logger()
    events = []

    def _capture_log_event(logger, **kwargs):
        events.append(kwargs)

    def _fake_download_and_verify(
        session,
        entry,
        logger,
        timeout_seconds,
        max_retries,
        backoff_initial_seconds,
        backoff_max_seconds,
        telemetry_hooks=None,
    ):
        return b"zip-bytes"

    def _fake_parse_zip_csv(zip_bytes, *, slice_ts, file_type, logger):
        return [{"ok": "1"}]

    monkeypatch.setattr(poller, "log_event", _capture_log_event)
    monkeypatch.setattr(poller, "_download_and_verify", _fake_download_and_verify)
    monkeypatch.setattr(poller, "_parse_zip_csv", _fake_parse_zip_csv)

    manifest_url = "http://localhost:18200/v2/lastupdate.txt"
    simulated_now_url = "http://localhost:18200/simulated_now"
    manifest_text = (
        "100 1111111111111111111111111111111111111111 "
        "http://localhost:18200/v2/20240101100000.events.csv.zip\n"
        "100 1111111111111111111111111111111111111111 "
        "http://localhost:18200/v2/20240101100000.articles.csv.zip\n"
    )

    session = _FakeSession(
        {
            manifest_url: _FakeResponse(status_code=200, text=manifest_text),
            simulated_now_url: _FakeResponse(
                status_code=200,
                text='{"simulated_now":"2024-01-01T10:15:00Z"}',
                json_payload={"simulated_now": "2024-01-01T10:15:00Z"},
            ),
        }
    )

    _ = poller.poll_once(
        session,
        base_url="http://localhost:18200",
        seen=set(),
        logger=logger,
        timeout_seconds=5.0,
    )

    metric_events = [e for e in events if e.get("action") == "poll_metrics" and e.get("result") == "summary"]
    assert len(metric_events) == 1
    summary = metric_events[0]
    assert summary["manifest_poll_success_count"] == 1
    assert summary["manifest_poll_error_count"] == 0
    assert summary["processed_file_count"] == 2
    assert summary["latest_processed_slice_by_file_type"] == {
        "events": "20240101100000",
        "articles": "20240101100000",
    }
    assert summary["latest_lag_seconds_by_file_type"] == {
        "events": 900.0,
        "articles": 900.0,
    }


def test_poll_once_emits_poll_metrics_summary_on_manifest_error(monkeypatch):
    logger = _test_logger()
    events = []

    def _capture_log_event(logger, **kwargs):
        events.append(kwargs)

    monkeypatch.setattr(poller, "log_event", _capture_log_event)

    manifest_url = "http://localhost:18200/v2/lastupdate.txt"
    session = _FakeSession(
        {
            manifest_url: _FakeResponse(status_code=503, text="service unavailable"),
        }
    )

    results = poller.poll_once(
        session,
        base_url="http://localhost:18200",
        seen=set(),
        logger=logger,
        timeout_seconds=5.0,
    )

    assert results == []
    metric_events = [e for e in events if e.get("action") == "poll_metrics" and e.get("result") == "summary"]
    assert len(metric_events) == 1
    summary = metric_events[0]
    assert summary["manifest_poll_success_count"] == 0
    assert summary["manifest_poll_error_count"] == 1
    assert summary["processed_file_count"] == 0
    assert summary["latest_processed_slice_by_file_type"] == {}
    assert summary["latest_lag_seconds_by_file_type"] == {}


def test_poll_once_emits_vendor_feed_down_alert_on_manifest_error(monkeypatch):
    logger = _test_logger()
    events = []

    def _capture_log_event(logger, **kwargs):
        events.append(kwargs)

    monkeypatch.setattr(poller, "log_event", _capture_log_event)

    manifest_url = "http://localhost:18200/v2/lastupdate.txt"
    session = _FakeSession(
        {
            manifest_url: _FakeResponse(status_code=503, text="service unavailable"),
        }
    )

    results = poller.poll_once(
        session,
        base_url="http://localhost:18200",
        seen=set(),
        logger=logger,
        timeout_seconds=5.0,
        alert_manifest_error_threshold=1,
    )

    assert results == []
    alert_events = [
        e
        for e in events
        if e.get("action") == "alert"
        and e.get("result") == "firing"
        and e.get("alert_name") == "vendor_feed_down"
    ]
    assert len(alert_events) == 1
    assert alert_events[0]["manifest_poll_error_count"] == 1


def test_poll_once_vendor_feed_down_alert_fires_within_timing_budget(monkeypatch):
    logger = _test_logger()
    events = []
    sleeps = []

    def _capture_log_event(logger, **kwargs):
        events.append(kwargs)

    monkeypatch.setattr(poller, "log_event", _capture_log_event)
    monkeypatch.setattr(poller.time, "sleep", lambda seconds: sleeps.append(seconds))

    manifest_url = "http://localhost:18200/v2/lastupdate.txt"
    session = _FakeSession(
        {
            manifest_url: _FakeResponse(status_code=503, text="service unavailable"),
        }
    )

    results = poller.poll_once(
        session,
        base_url="http://localhost:18200",
        seen=set(),
        logger=logger,
        timeout_seconds=5.0,
        max_retries=2,
        backoff_initial_seconds=0.01,
        backoff_max_seconds=0.02,
        alert_manifest_error_threshold=1,
    )

    assert results == []
    assert sum(sleeps) < 60.0
    alert_events = [
        e
        for e in events
        if e.get("action") == "alert"
        and e.get("result") == "firing"
        and e.get("alert_name") == "vendor_feed_down"
    ]
    assert len(alert_events) == 1


def test_poll_once_emits_high_lag_alert_when_threshold_exceeded(monkeypatch):
    logger = _test_logger()
    events = []

    def _capture_log_event(logger, **kwargs):
        events.append(kwargs)

    def _fake_download_and_verify(
        session,
        entry,
        logger,
        timeout_seconds,
        max_retries,
        backoff_initial_seconds,
        backoff_max_seconds,
    ):
        return b"zip-bytes"

    def _fake_parse_zip_csv(zip_bytes, *, slice_ts, file_type, logger):
        return [{"ok": "1"}]

    monkeypatch.setattr(poller, "log_event", _capture_log_event)
    monkeypatch.setattr(poller, "_download_and_verify", _fake_download_and_verify)
    monkeypatch.setattr(poller, "_parse_zip_csv", _fake_parse_zip_csv)

    manifest_url = "http://localhost:18200/v2/lastupdate.txt"
    simulated_now_url = "http://localhost:18200/simulated_now"
    manifest_text = (
        "100 1111111111111111111111111111111111111111 "
        "http://localhost:18200/v2/20240101100000.events.csv.zip\n"
    )

    session = _FakeSession(
        {
            manifest_url: _FakeResponse(status_code=200, text=manifest_text),
            simulated_now_url: _FakeResponse(
                status_code=200,
                text='{"simulated_now":"2024-01-01T10:15:00Z"}',
                json_payload={"simulated_now": "2024-01-01T10:15:00Z"},
            ),
        }
    )

    _ = poller.poll_once(
        session,
        base_url="http://localhost:18200",
        seen=set(),
        logger=logger,
        timeout_seconds=5.0,
        alert_lag_seconds_threshold=600.0,
    )

    lag_alert_events = [
        e
        for e in events
        if e.get("action") == "alert"
        and e.get("result") == "firing"
        and e.get("alert_name") == "ingest_lag_high"
    ]
    assert len(lag_alert_events) == 1
    assert lag_alert_events[0]["file_type"] == "events"
    assert lag_alert_events[0]["lag_seconds"] == 900.0


def test_poll_once_does_not_emit_vendor_clear_without_prior_fire(monkeypatch):
    logger = _test_logger()
    events = []

    def _capture_log_event(logger, **kwargs):
        events.append(kwargs)

    monkeypatch.setattr(poller, "log_event", _capture_log_event)

    manifest_url = "http://localhost:18200/v2/lastupdate.txt"
    simulated_now_url = "http://localhost:18200/simulated_now"
    session = _FakeSession(
        {
            manifest_url: _FakeResponse(status_code=200, text=""),
            simulated_now_url: _FakeResponse(
                status_code=200,
                text='{"simulated_now":"2024-01-01T10:15:00Z"}',
                json_payload={"simulated_now": "2024-01-01T10:15:00Z"},
            ),
        }
    )

    alert_state = {"vendor_feed_down_active": False, "lag_high_active_by_file_type": {}}
    _ = poller.poll_once(
        session,
        base_url="http://localhost:18200",
        seen=set(),
        logger=logger,
        timeout_seconds=5.0,
        alert_state=alert_state,
    )

    vendor_cleared_events = [
        e
        for e in events
        if e.get("action") == "alert"
        and e.get("alert_name") == "vendor_feed_down"
        and e.get("result") == "cleared"
    ]
    assert vendor_cleared_events == []


def test_poll_once_vendor_feed_down_transitions_fire_then_clear(monkeypatch):
    logger = _test_logger()
    events = []

    def _capture_log_event(logger, **kwargs):
        events.append(kwargs)

    monkeypatch.setattr(poller, "log_event", _capture_log_event)

    manifest_url = "http://localhost:18200/v2/lastupdate.txt"
    simulated_now_url = "http://localhost:18200/simulated_now"
    alert_state = {"vendor_feed_down_active": False, "lag_high_active_by_file_type": {}}

    session_error = _FakeSession(
        {
            manifest_url: _FakeResponse(status_code=503, text="service unavailable"),
        }
    )
    _ = poller.poll_once(
        session_error,
        base_url="http://localhost:18200",
        seen=set(),
        logger=logger,
        timeout_seconds=5.0,
        alert_manifest_error_threshold=1,
        alert_state=alert_state,
    )

    session_success = _FakeSession(
        {
            manifest_url: _FakeResponse(status_code=200, text=""),
            simulated_now_url: _FakeResponse(
                status_code=200,
                text='{"simulated_now":"2024-01-01T10:15:00Z"}',
                json_payload={"simulated_now": "2024-01-01T10:15:00Z"},
            ),
        }
    )
    _ = poller.poll_once(
        session_success,
        base_url="http://localhost:18200",
        seen=set(),
        logger=logger,
        timeout_seconds=5.0,
        alert_manifest_error_threshold=1,
        alert_state=alert_state,
    )

    vendor_firing_events = [
        e
        for e in events
        if e.get("action") == "alert"
        and e.get("alert_name") == "vendor_feed_down"
        and e.get("result") == "firing"
    ]
    vendor_cleared_events = [
        e
        for e in events
        if e.get("action") == "alert"
        and e.get("alert_name") == "vendor_feed_down"
        and e.get("result") == "cleared"
    ]

    assert len(vendor_firing_events) == 1
    assert len(vendor_cleared_events) == 1


def test_poller_module_has_no_db_driver_imports():
    src = inspect.getsource(poller)
    disallowed = ("psycopg", "sqlalchemy", "sqlite3")
    for token in disallowed:
        assert token not in src


def test_poll_once_retries_manifest_request_error_then_succeeds(monkeypatch):
    logger = _test_logger()
    events = []
    sleeps = []

    def _capture_log_event(logger, **kwargs):
        events.append(kwargs)

    def _fake_download_and_verify(
        session,
        entry,
        logger,
        timeout_seconds,
        max_retries,
        backoff_initial_seconds,
        backoff_max_seconds,
    ):
        return b"zip-bytes"

    def _fake_parse_zip_csv(zip_bytes, *, slice_ts, file_type, logger):
        return [{"ok": "1"}]

    monkeypatch.setattr(poller, "log_event", _capture_log_event)
    monkeypatch.setattr(poller, "_download_and_verify", _fake_download_and_verify)
    monkeypatch.setattr(poller, "_parse_zip_csv", _fake_parse_zip_csv)
    monkeypatch.setattr(poller.time, "sleep", lambda seconds: sleeps.append(seconds))

    manifest_url = "http://localhost:18200/v2/lastupdate.txt"
    simulated_now_url = "http://localhost:18200/simulated_now"
    manifest_text = (
        "100 1111111111111111111111111111111111111111 "
        "http://localhost:18200/v2/20240101100000.events.csv.zip\n"
    )

    session = _SequenceSession(
        {
            manifest_url: [
                requests.RequestException("temporary network glitch"),
                _FakeResponse(status_code=200, text=manifest_text),
            ],
            simulated_now_url: [
                _FakeResponse(
                    status_code=200,
                    text='{"simulated_now":"2024-01-01T10:15:00Z"}',
                    json_payload={"simulated_now": "2024-01-01T10:15:00Z"},
                )
            ],
        }
    )

    results = poller.poll_once(
        session,
        base_url="http://localhost:18200",
        seen=set(),
        logger=logger,
        timeout_seconds=5.0,
        max_retries=2,
        backoff_initial_seconds=0.01,
        backoff_max_seconds=0.02,
    )

    assert len(results) == 1
    assert sleeps == [0.01]
    retry_events = [
        e for e in events if e.get("action") == "manifest_poll" and e.get("result") == "retry_request_error"
    ]
    assert len(retry_events) == 1


def test_download_and_verify_retries_transient_http_status(monkeypatch):
    logger = _test_logger()
    events = []
    sleeps = []

    def _capture_log_event(logger, **kwargs):
        events.append(kwargs)

    monkeypatch.setattr(poller, "log_event", _capture_log_event)
    monkeypatch.setattr(poller.time, "sleep", lambda seconds: sleeps.append(seconds))

    payload = b"hello"
    entry = poller.ManifestEntry(
        expected_bytes=len(payload),
        expected_sha1=poller._sha1_bytes(payload),
        url="http://localhost:18200/v2/20240101100000.events.csv.zip",
        slice_ts="20240101100000",
        file_type="events",
    )
    session = _SequenceSession(
        {
            entry.url: [
                _FakeResponse(status_code=503, text="service unavailable", content=b""),
                _FakeResponse(status_code=200, text="ok", content=payload),
            ]
        }
    )

    zip_bytes = poller._download_and_verify(
        session,
        entry,
        logger,
        timeout_seconds=5.0,
        max_retries=2,
        backoff_initial_seconds=0.01,
        backoff_max_seconds=0.02,
    )

    assert zip_bytes == payload
    assert sleeps == [0.01]
    retry_events = [
        e for e in events if e.get("action") == "file_download" and e.get("result") == "retry_http_status"
    ]
    assert len(retry_events) == 1


def test_poll_forever_uses_provided_seen_and_persist_callback(monkeypatch):
    seen_in = {("20240101100000", "events")}
    captured_seen = []
    persisted = []

    result = poller.PollResult(
        entry=poller.ManifestEntry(
            expected_bytes=1,
            expected_sha1="1" * 40,
            url="http://localhost:18200/v2/20240101101500.events.csv.zip",
            slice_ts="20240101101500",
            file_type="events",
        ),
        rows=[{"event_id": "1"}],
    )

    calls = {"count": 0}

    def _fake_poll_once(session, *, base_url, seen, logger, timeout_seconds, **kwargs):
        captured_seen.append(set(seen))
        calls["count"] += 1
        if calls["count"] == 1:
            return [result]
        raise KeyboardInterrupt("stop loop")

    monkeypatch.setattr(poller, "poll_once", _fake_poll_once)
    monkeypatch.setattr(poller.time, "sleep", lambda _: None)

    iterator = poller.poll_forever(
        base_url="http://localhost:18200",
        interval_seconds=0,
        timeout_seconds=1,
        seen=seen_in,
        persist_callback=lambda item: persisted.append(item),
    )

    first = next(iterator)
    assert first == result
    assert persisted == [result]
    assert captured_seen[0] == seen_in

    with pytest.raises(KeyboardInterrupt):
        next(iterator)

    # poll_forever should not mutate the provided set instance.
    assert seen_in == {("20240101100000", "events")}


def test_poll_forever_end_to_end_poll_parse_persist(monkeypatch):
    logger = _test_logger()
    events = []
    persisted = []

    def _capture_log_event(logger, **kwargs):
        events.append(kwargs)

    def _make_zip_csv(payload_text: str) -> bytes:
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("events.csv", payload_text)
        return buf.getvalue()

    zip_bytes = _make_zip_csv(
        "event_id,event_time,actor_country,target_country,event_type,intensity,location_country,location_lat,location_lon,source_url\n"
        "101,2024-01-01T10:00:00Z,USA,CHN,statement,1.2,USA,40.71,-74.01,https://example.com/story\n"
    )
    expected_sha1 = poller._sha1_bytes(zip_bytes)
    expected_bytes = len(zip_bytes)

    manifest_url = "http://localhost:18200/v2/lastupdate.txt"
    file_url = "http://localhost:18200/v2/20240101100000.events.csv.zip"
    simulated_now_url = "http://localhost:18200/simulated_now"
    manifest_text = f"{expected_bytes} {expected_sha1} {file_url}\n"

    session = _ContextSequenceSession(
        {
            manifest_url: [_FakeResponse(status_code=200, text=manifest_text)],
            file_url: [_FakeResponse(status_code=200, text="ok", content=zip_bytes)],
            simulated_now_url: [
                _FakeResponse(
                    status_code=200,
                    text='{"simulated_now":"2024-01-01T10:15:00Z"}',
                    json_payload={"simulated_now": "2024-01-01T10:15:00Z"},
                )
            ],
        }
    )

    monkeypatch.setattr(poller.requests, "Session", lambda: session)
    monkeypatch.setattr(poller, "_setup_logger", lambda: logger)
    monkeypatch.setattr(poller, "log_event", _capture_log_event)

    iterator = poller.poll_forever(
        base_url="http://localhost:18200",
        interval_seconds=999,
        timeout_seconds=5.0,
        persist_callback=lambda item: persisted.append(item),
    )

    first = next(iterator)
    iterator.close()

    assert first.entry.slice_ts == "20240101100000"
    assert first.entry.file_type == "events"
    assert len(first.rows) == 1
    assert first.rows[0]["event_id"] == "101"

    assert len(persisted) == 1
    assert persisted[0].entry.slice_ts == "20240101100000"

    db_events = [e for e in events if e.get("action") == "db_persist" and e.get("result") == "ok"]
    assert len(db_events) == 1
    assert db_events[0]["file_type"] == "events"
    assert db_events[0]["row_count"] == 1

    metric_events = [e for e in events if e.get("action") == "poll_metrics" and e.get("result") == "summary"]
    assert len(metric_events) == 1
    assert metric_events[0]["processed_file_count"] == 1


def test_build_persist_callback_compacts_checkpoints_on_schedule(monkeypatch):
    logger = _test_logger()
    events = []

    def _capture_log_event(logger, **kwargs):
        events.append(kwargs)

    class _FakeStore:
        def __init__(self, *, dsn):
            self.dsn = dsn
            self.persist_calls = 0
            self.compact_calls = 0

        def open(self):
            return None

        def load_seen(self):
            return set()

        def persist(self, result):
            self.persist_calls += 1

        def compact_checkpoints(self, *, retain_per_file_type):
            self.compact_calls += 1
            return 3

    fake_module = types.ModuleType("src.ingest.storage")
    fake_module.PersistableResult = lambda **kwargs: kwargs
    fake_module.PostgresIngestStore = _FakeStore

    monkeypatch.setattr(poller, "log_event", _capture_log_event)
    monkeypatch.setitem(sys.modules, "src.ingest.storage", fake_module)

    persist_callback, seen, _store = poller._build_persist_callback(
        database_url="postgresql://example/test",
        logger=logger,
        checkpoint_retain_per_file_type=5,
        checkpoint_compact_every=2,
    )

    result = poller.PollResult(
        entry=poller.ManifestEntry(
            expected_bytes=1,
            expected_sha1="1" * 40,
            url="http://localhost:18200/v2/20240101100000.events.csv.zip",
            slice_ts="20240101100000",
            file_type="events",
        ),
        rows=[{"event_id": "1"}],
    )

    persist_callback(result)
    persist_callback(result)

    assert seen == set()
    compact_events = [
        e
        for e in events
        if e.get("action") == "db_checkpoint" and e.get("result") == "compacted"
    ]
    assert len(compact_events) == 1
    assert compact_events[0]["deleted_count"] == 3
    assert compact_events[0]["retain_per_file_type"] == 5
    assert compact_events[0]["compact_every"] == 2


def test_poll_once_logs_missing_file_type_for_partial_slice(monkeypatch):
    logger = _test_logger()
    events = []

    def _capture_log_event(logger, **kwargs):
        events.append(kwargs)

    def _fake_download_and_verify(
        session,
        entry,
        logger,
        timeout_seconds,
        max_retries,
        backoff_initial_seconds,
        backoff_max_seconds,
    ):
        return b"zip-bytes"

    def _fake_parse_zip_csv(zip_bytes, *, slice_ts, file_type, logger):
        return [{"ok": file_type}]

    monkeypatch.setattr(poller, "log_event", _capture_log_event)
    monkeypatch.setattr(poller, "_download_and_verify", _fake_download_and_verify)
    monkeypatch.setattr(poller, "_parse_zip_csv", _fake_parse_zip_csv)

    manifest_url = "http://localhost:18200/v2/lastupdate.txt"
    simulated_now_url = "http://localhost:18200/simulated_now"
    # Deliberately omit mentions for the same slice to mimic partial-slice publication.
    manifest_text = (
        "100 1111111111111111111111111111111111111111 "
        "http://localhost:18200/v2/20240101100000.events.csv.zip\n"
        "100 1111111111111111111111111111111111111111 "
        "http://localhost:18200/v2/20240101100000.articles.csv.zip\n"
    )

    session = _FakeSession(
        {
            manifest_url: _FakeResponse(status_code=200, text=manifest_text),
            simulated_now_url: _FakeResponse(
                status_code=200,
                text='{"simulated_now":"2024-01-01T10:15:00Z"}',
                json_payload={"simulated_now": "2024-01-01T10:15:00Z"},
            ),
        }
    )

    results = poller.poll_once(
        session,
        base_url="http://localhost:18200",
        seen=set(),
        logger=logger,
        timeout_seconds=5.0,
    )

    assert len(results) == 2
    missing_events = [
        e
        for e in events
        if e.get("action") == "manifest_validate"
        and e.get("result") == "missing_file_type"
        and e.get("slice_ts") == "20240101100000"
    ]
    assert len(missing_events) == 1
    assert missing_events[0]["file_type"] == "mentions"


def test_poll_once_repoll_skips_already_seen_files(monkeypatch):
    logger = _test_logger()
    events = []

    def _capture_log_event(logger, **kwargs):
        events.append(kwargs)

    def _fake_download_and_verify(
        session,
        entry,
        logger,
        timeout_seconds,
        max_retries,
        backoff_initial_seconds,
        backoff_max_seconds,
    ):
        return b"zip-bytes"

    def _fake_parse_zip_csv(zip_bytes, *, slice_ts, file_type, logger):
        return [{"ok": "1"}]

    monkeypatch.setattr(poller, "log_event", _capture_log_event)
    monkeypatch.setattr(poller, "_download_and_verify", _fake_download_and_verify)
    monkeypatch.setattr(poller, "_parse_zip_csv", _fake_parse_zip_csv)

    manifest_url = "http://localhost:18200/v2/lastupdate.txt"
    simulated_now_url = "http://localhost:18200/simulated_now"
    manifest_text = (
        "100 1111111111111111111111111111111111111111 "
        "http://localhost:18200/v2/20240101100000.events.csv.zip\n"
    )

    session = _FakeSession(
        {
            manifest_url: _FakeResponse(status_code=200, text=manifest_text),
            simulated_now_url: _FakeResponse(
                status_code=200,
                text='{"simulated_now":"2024-01-01T10:15:00Z"}',
                json_payload={"simulated_now": "2024-01-01T10:15:00Z"},
            ),
        }
    )

    seen = set()
    first = poller.poll_once(
        session,
        base_url="http://localhost:18200",
        seen=seen,
        logger=logger,
        timeout_seconds=5.0,
    )
    second = poller.poll_once(
        session,
        base_url="http://localhost:18200",
        seen=seen,
        logger=logger,
        timeout_seconds=5.0,
    )

    assert len(first) == 1
    assert second == []
    skip_seen_events = [
        e
        for e in events
        if e.get("action") == "file_process"
        and e.get("result") == "skip_seen"
        and e.get("slice_ts") == "20240101100000"
        and e.get("file_type") == "events"
    ]
    assert len(skip_seen_events) == 1


def test_poll_once_calls_manifest_poll_telemetry_hook(monkeypatch):
    logger = _test_logger()
    records = []

    def _fake_download_and_verify(
        session,
        entry,
        logger,
        timeout_seconds,
        max_retries,
        backoff_initial_seconds,
        backoff_max_seconds,
        telemetry_hooks=None,
    ):
        return b"zip-bytes"

    def _fake_parse_zip_csv(zip_bytes, *, slice_ts, file_type, logger):
        return [{"ok": "1"}]

    monkeypatch.setattr(poller, "_download_and_verify", _fake_download_and_verify)
    monkeypatch.setattr(poller, "_parse_zip_csv", _fake_parse_zip_csv)

    manifest_url = "http://localhost:18200/v2/lastupdate.txt"
    simulated_now_url = "http://localhost:18200/simulated_now"
    manifest_text = (
        "100 1111111111111111111111111111111111111111 "
        "http://localhost:18200/v2/20240101100000.events.csv.zip\n"
    )
    session = _FakeSession(
        {
            manifest_url: _FakeResponse(status_code=200, text=manifest_text),
            simulated_now_url: _FakeResponse(
                status_code=200,
                text='{"simulated_now":"2024-01-01T10:15:00Z"}',
                json_payload={"simulated_now": "2024-01-01T10:15:00Z"},
            ),
        }
    )

    _ = poller.poll_once(
        session,
        base_url="http://localhost:18200",
        seen=set(),
        logger=logger,
        timeout_seconds=5.0,
        telemetry_hooks={"record_manifest_poll": lambda **kw: records.append(kw)},
    )

    assert len(records) == 1
    assert records[0]["success"] is True
    assert records[0]["line_count"] == 1
    assert records[0]["manifest_latest_slice"] == "20240101100000"


def test_poll_once_stale_manifest_degraded_window_transitions(monkeypatch):
    logger = _test_logger()
    degraded_calls = []

    def _fake_download_and_verify(
        session,
        entry,
        logger,
        timeout_seconds,
        max_retries,
        backoff_initial_seconds,
        backoff_max_seconds,
        telemetry_hooks=None,
    ):
        return b"zip-bytes"

    def _fake_parse_zip_csv(zip_bytes, *, slice_ts, file_type, logger):
        return [{"ok": file_type}]

    monkeypatch.setattr(poller, "_download_and_verify", _fake_download_and_verify)
    monkeypatch.setattr(poller, "_parse_zip_csv", _fake_parse_zip_csv)

    manifest_url = "http://localhost:18200/v2/lastupdate.txt"
    simulated_now_url = "http://localhost:18200/simulated_now"
    manifest_a = (
        "100 1111111111111111111111111111111111111111 "
        "http://localhost:18200/v2/20240101100000.events.csv.zip\n"
    )
    manifest_b = (
        "100 1111111111111111111111111111111111111111 "
        "http://localhost:18200/v2/20240101101500.events.csv.zip\n"
    )

    alert_state = {
        "vendor_feed_down_active": False,
        "lag_high_active_by_file_type": {},
        "stale_manifest_active": False,
        "manifest_repeat_count": 0,
        "last_manifest_latest_slice": None,
    }

    shared_responses = {
        simulated_now_url: _FakeResponse(
            status_code=200,
            text='{"simulated_now":"2024-01-01T10:15:00Z"}',
            json_payload={"simulated_now": "2024-01-01T10:15:00Z"},
        )
    }

    session_1 = _FakeSession({manifest_url: _FakeResponse(status_code=200, text=manifest_a), **shared_responses})
    session_2 = _FakeSession({manifest_url: _FakeResponse(status_code=200, text=manifest_a), **shared_responses})
    session_3 = _FakeSession({manifest_url: _FakeResponse(status_code=200, text=manifest_b), **shared_responses})

    hooks = {"set_degraded_state": lambda **kw: degraded_calls.append(kw)}

    _ = poller.poll_once(
        session_1,
        base_url="http://localhost:18200",
        seen=set(),
        logger=logger,
        timeout_seconds=5.0,
        alert_state=alert_state,
        telemetry_hooks=hooks,
    )
    _ = poller.poll_once(
        session_2,
        base_url="http://localhost:18200",
        seen=set(),
        logger=logger,
        timeout_seconds=5.0,
        alert_state=alert_state,
        telemetry_hooks=hooks,
    )
    _ = poller.poll_once(
        session_3,
        base_url="http://localhost:18200",
        seen=set(),
        logger=logger,
        timeout_seconds=5.0,
        alert_state=alert_state,
        telemetry_hooks=hooks,
    )

    stale_calls = [c for c in degraded_calls if c.get("degraded_type") == "stale_manifest"]
    assert stale_calls[0]["active"] is True
    assert stale_calls[-1]["active"] is False
