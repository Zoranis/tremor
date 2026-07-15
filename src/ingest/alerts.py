"""Poll-metric summaries and stateful alert fire/clear transitions."""
from __future__ import annotations

import logging

from . import poller_logging as _log
from .poller_logging import TelemetryHook, _call_hook


def _log_poll_metrics(
    logger: logging.Logger,
    *,
    manifest_poll_success_count: int,
    manifest_poll_error_count: int,
    processed_file_count: int,
    latest_processed_slice_by_file_type: dict[str, str],
    latest_lag_seconds_by_file_type: dict[str, float],
) -> None:
    _log.log_event(
        logger,
        action="poll_metrics",
        result="summary",
        manifest_poll_success_count=manifest_poll_success_count,
        manifest_poll_error_count=manifest_poll_error_count,
        processed_file_count=processed_file_count,
        latest_processed_slice_by_file_type=latest_processed_slice_by_file_type,
        latest_lag_seconds_by_file_type=latest_lag_seconds_by_file_type,
    )


def _transition_alert(
    logger: logging.Logger,
    *,
    alert_name: str,
    file_type: str | None,
    active: bool,
    was_active: bool,
    value_field_name: str,
    value: float,
    threshold: float,
    telemetry_hooks: dict[str, TelemetryHook] | None,
) -> bool:
    """Log + fire the telemetry hook on a fire/clear transition.

    No-ops (and returns `was_active` unchanged) when `active` matches
    `was_active` — alerts should only emit on state changes, not on every
    poll they remain in the same state. Returns the alert's new state.
    """
    if active == was_active:
        return was_active

    result = "firing" if active else "cleared"
    _log.log_event(
        logger,
        action="alert",
        result=result,
        alert_name=alert_name,
        file_type=file_type,
        threshold=threshold,
        **{value_field_name: value},
    )
    _call_hook(
        telemetry_hooks,
        "record_alert_event",
        alert_name=alert_name,
        file_type=file_type,
        state=result,
        value=float(value),
        threshold=float(threshold),
    )
    return active


def _emit_alert_events(
    logger: logging.Logger,
    *,
    manifest_poll_success_count: int,
    manifest_poll_error_count: int,
    latest_lag_seconds_by_file_type: dict[str, float],
    alert_manifest_error_threshold: int,
    alert_lag_seconds_threshold: float,
    alert_state: dict[str, object] | None = None,
    telemetry_hooks: dict[str, TelemetryHook] | None = None,
) -> None:
    vendor_feed_down_active = False
    lag_high_active_by_file_type: dict[str, bool] = {}
    if alert_state is not None:
        vendor_feed_down_active = bool(alert_state.get("vendor_feed_down_active", False))
        lag_state = alert_state.get("lag_high_active_by_file_type")
        if isinstance(lag_state, dict):
            lag_high_active_by_file_type = {
                str(k): bool(v) for k, v in lag_state.items()
            }

    # Below threshold + no successful poll this cycle is a real third state
    # (a lone transient failure) — deliberately leave the alert untouched
    # rather than treating "not firing" as "clear".
    if manifest_poll_error_count >= alert_manifest_error_threshold:
        vendor_feed_down_active = _transition_alert(
            logger,
            alert_name="vendor_feed_down",
            file_type=None,
            active=True,
            was_active=vendor_feed_down_active,
            value_field_name="manifest_poll_error_count",
            value=manifest_poll_error_count,
            threshold=alert_manifest_error_threshold,
            telemetry_hooks=telemetry_hooks,
        )
    elif manifest_poll_success_count > 0:
        vendor_feed_down_active = _transition_alert(
            logger,
            alert_name="vendor_feed_down",
            file_type=None,
            active=False,
            was_active=vendor_feed_down_active,
            value_field_name="manifest_poll_error_count",
            value=manifest_poll_error_count,
            threshold=alert_manifest_error_threshold,
            telemetry_hooks=telemetry_hooks,
        )

    for file_type, lag_seconds in latest_lag_seconds_by_file_type.items():
        was_active = lag_high_active_by_file_type.get(file_type, False)
        lag_high_active_by_file_type[file_type] = _transition_alert(
            logger,
            alert_name="ingest_lag_high",
            file_type=file_type,
            active=lag_seconds >= alert_lag_seconds_threshold,
            was_active=was_active,
            value_field_name="lag_seconds",
            value=lag_seconds,
            threshold=alert_lag_seconds_threshold,
            telemetry_hooks=telemetry_hooks,
        )

    if alert_state is not None:
        alert_state["vendor_feed_down_active"] = vendor_feed_down_active
        alert_state["lag_high_active_by_file_type"] = lag_high_active_by_file_type
