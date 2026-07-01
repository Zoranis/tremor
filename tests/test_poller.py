import inspect
import logging
from datetime import timezone

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
