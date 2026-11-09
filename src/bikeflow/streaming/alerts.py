"""Empty/full alert lifecycle: open, escalate, close (a pure state machine).

One open alert per station at a time, because a station is either out of bikes or
out of docks. The rules (Deliverable 1, section 4, and docs/SPEED_LAYER.md):

* the condition must hold for ``open_after`` before an alert opens, so one noisy
  snapshot (a bike returned 10 seconds later) does not page an operator;
* an open alert escalates from ``warning`` to ``critical`` after ``critical_after``;
* it closes when the station recovers, when it leaves the alerts (out of service,
  docs/DATA_CONTRACT.md) or when the condition flips to the other kind.

The state machine takes the previously open alert (read from Redis) and returns
what the sink must write. No Spark, no Redis here: everything is testable.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta

from bikeflow.streaming import model, redis_keys

SEVERITY_WARNING = "warning"
SEVERITY_CRITICAL = "critical"

EVENT_OPENED = "opened"
EVENT_ESCALATED = "escalated"
EVENT_CLOSED = "closed"

CLOSE_RECOVERED = "recovered"
CLOSE_OUT_OF_SERVICE = "out_of_service"
CLOSE_SUPERSEDED = "superseded"


@dataclass(frozen=True)
class Thresholds:
    """How long a condition must hold before it becomes an alert, and before it escalates."""

    open_after: timedelta = timedelta(minutes=2)
    critical_after: timedelta = timedelta(minutes=10)

    @classmethod
    def from_settings(cls, settings) -> "Thresholds":
        return cls(
            open_after=timedelta(minutes=settings.alert_open_after_minutes),
            critical_after=timedelta(minutes=settings.alert_critical_after_minutes),
        )


@dataclass
class AlertOutcome:
    """What this micro-batch does to the alerts of one station."""

    condition_since: datetime | None = None
    active: dict | None = None  # the alert open after this batch (written every time)
    opened: list[dict] = field(default_factory=list)
    escalated: list[dict] = field(default_factory=list)
    closed: list[dict] = field(default_factory=list)

    @property
    def events(self) -> list[tuple[str, dict]]:
        """``[(event, alert), ...]`` in the order they happened."""
        return (
            [(EVENT_OPENED, alert) for alert in self.opened]
            + [(EVENT_ESCALATED, alert) for alert in self.escalated]
            + [(EVENT_CLOSED, alert) for alert in self.closed]
        )

    @property
    def changed(self) -> bool:
        return bool(self.opened or self.escalated or self.closed)


def make_alert_id(system_id: str, station_id: str, kind: str, opened_at: datetime) -> str:
    """Deterministic id: re-processing a micro-batch cannot open the same alert twice."""
    return redis_keys.alert_id(system_id, station_id, kind, model.epoch(opened_at))


def open_alert_from_hash(stored: dict | None) -> dict | None:
    """The open alert kept in the station hash, or ``None`` when the station has none."""
    if not stored:
        return None
    alert_id = stored.get("alert_id")
    if not alert_id:
        return None
    return {
        "alert_id": str(alert_id),
        "kind": str(stored.get("alert_kind") or ""),
        "severity": str(stored.get("alert_severity") or SEVERITY_WARNING),
        "opened_at": str(stored.get("alert_opened_at") or ""),
    }


def _record(
    observation: model.StationObservation,
    kind: str,
    condition_since: datetime,
    opened_at: datetime,
    severity: str,
    now: datetime,
) -> dict:
    """The hash written to ``bf:alert:{alert_id}`` for an open alert."""
    return {
        "alert_id": make_alert_id(observation.system_id, observation.station_id, kind, opened_at),
        "station_id": observation.station_id,
        "system_id": observation.system_id,
        "kind": kind,
        "severity": severity,
        "status": "open",
        "opened_at": model.iso(opened_at),
        "condition_since": model.iso(condition_since),
        "updated_at": model.iso(now),
        "event_time": model.iso(observation.event_time),
        "bikes": observation.bikes,
        "docks": observation.docks,
        "station_size": observation.size,
        "local_hour": observation.local_hour,
        "weekday_num": observation.weekday_num,
        "closed_at": "",
        "close_reason": "",
    }


def _close(previous: dict, observation: model.StationObservation, now: datetime, reason: str) -> dict:
    """The hash written to ``bf:alert:{alert_id}`` when an alert closes."""
    return {
        "alert_id": str(previous.get("alert_id") or ""),
        "station_id": observation.station_id,
        "system_id": observation.system_id,
        "kind": str(previous.get("kind") or ""),
        "severity": str(previous.get("severity") or SEVERITY_WARNING),
        "status": "closed",
        "opened_at": str(previous.get("opened_at") or ""),
        "updated_at": model.iso(now),
        "event_time": model.iso(observation.event_time),
        "bikes": observation.bikes,
        "docks": observation.docks,
        "station_size": observation.size,
        "local_hour": observation.local_hour,
        "weekday_num": observation.weekday_num,
        "closed_at": model.iso(now),
        "close_reason": reason,
    }


def evaluate(
    observation: model.StationObservation,
    status: str,
    previous: dict | None,
    condition_since: datetime | None,
    now: datetime,
    thresholds: Thresholds | None = None,
) -> AlertOutcome:
    """Decide the alerts of one station from its previous alert and its new status."""
    thresholds = thresholds or Thresholds()
    outcome = AlertOutcome()
    previous = previous or None
    previous_kind = (previous or {}).get("kind")
    previous_opened_at = model.as_utc((previous or {}).get("opened_at"))

    if status not in model.ALERTABLE_STATUSES:
        outcome.condition_since = None
        if previous:
            reason = CLOSE_RECOVERED if status == model.STATUS_OK else CLOSE_OUT_OF_SERVICE
            outcome.closed.append(_close(previous, observation, now, reason))
        return outcome

    if previous and previous_kind != status:
        # Empty -> full (or back): close the old alert and start the debounce again.
        outcome.closed.append(_close(previous, observation, now, CLOSE_SUPERSEDED))
        previous = None
        previous_opened_at = None
        condition_since = None

    since = condition_since or now
    outcome.condition_since = since

    if previous is None:
        if now - since >= thresholds.open_after:
            alert = _record(observation, status, since, now, SEVERITY_WARNING, now)
            outcome.opened.append(alert)
            outcome.active = alert
        return outcome

    alert = _record(observation, status, since, previous_opened_at or now, previous.get("severity"), now)
    if alert["severity"] != SEVERITY_CRITICAL and previous_opened_at:
        if now - previous_opened_at >= thresholds.critical_after:
            alert["severity"] = SEVERITY_CRITICAL
            outcome.escalated.append(alert)
    outcome.active = alert
    return outcome

