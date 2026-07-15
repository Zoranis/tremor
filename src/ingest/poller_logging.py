"""Structured logging shared by every poller submodule.

Other submodules must call these via qualified module access
(``from . import poller_logging as _log`` then ``_log.log_event(...)``)
rather than ``from .poller_logging import log_event`` — tests monkeypatch
``poller_logging.log_event`` and rely on that single patch point being seen
by every caller, which only holds for attribute access on the module object.
"""
from __future__ import annotations

import json
import logging
import time
from typing import Callable


TelemetryHook = Callable[..., None]


def _setup_logger() -> logging.Logger:
    logger = logging.getLogger("tremor.ingest.poller")
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    logger.propagate = False
    return logger


def log_event(
    logger: logging.Logger,
    *,
    action: str,
    result: str,
    slice_ts: str | None = None,
    file_type: str | None = None,
    level: int = logging.INFO,
    **fields: object,
) -> None:
    payload: dict[str, object] = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "action": action,
        "result": result,
        "slice_ts": slice_ts,
        "file_type": file_type,
    }
    payload.update(fields)
    logger.log(level, json.dumps(payload, sort_keys=True, default=str))


def _call_hook(hooks: dict[str, TelemetryHook] | None, name: str, **kwargs: object) -> None:
    if hooks is None:
        return
    hook = hooks.get(name)
    if hook is None:
        return
    try:
        hook(**kwargs)
    except Exception:
        # Telemetry must not break ingestion.
        return
