"""The empty/full alert lifecycle: debounce, escalation, closing, idempotency."""

from datetime import datetime, timedelta, timezone

from bikeflow.streaming import alerts, model

NOW = datetime(2026, 11, 4, 12, 0, tzinfo=timezone.utc)
THRESHOLDS = alerts.Thresholds(open_after=timedelta(minutes=2), critical_after=timedelta(minutes=10))


def observation(bikes: int = 0, docks: int = 10, at: datetime = NOW) -> model.StationObservation:
    return model.StationObservation(
        station_id="station-1",
        system_id="citibike_nyc",
        bikes=bikes,
        docks=docks,
        bikes_disabled=0,
        docks_disabled=0,
        ebikes=0,
        is_installed=True,
        is_renting=True,
        is_returning=True,
        event_time=at,
        last_reported=at,
        local_hour=7,
        weekday_num=3,
    )


def open_alert(kind: str = "empty", severity: str = "warning") -> dict:
    return {"alert_id": "alert-1", "kind": kind, "severity": severity, "opened_at": model.iso(NOW)}


def test_a_single_empty_snapshot_does_not_open_an_alert():
    outcome = alerts.evaluate(observation(), model.STATUS_EMPTY, None, None, NOW, THRESHOLDS)
    assert outcome.opened == []
    assert outcome.active is None
    assert outcome.condition_since == NOW


def test_the_alert_opens_once_the_condition_has_held():
    later = NOW + timedelta(minutes=2)
    outcome = alerts.evaluate(observation(at=later), model.STATUS_EMPTY, None, NOW, later, THRESHOLDS)
    assert len(outcome.opened) == 1
    alert = outcome.active
    assert alert["kind"] == model.STATUS_EMPTY
    assert alert["severity"] == alerts.SEVERITY_WARNING
    assert alert["opened_at"] == model.iso(later)
    assert alert["condition_since"] == model.iso(NOW)
    assert alert["alert_id"] == f"citibike_nyc:station-1:empty:{model.epoch(later)}"


def test_the_alert_escalates_to_critical():
    later = NOW + timedelta(minutes=11)
    outcome = alerts.evaluate(observation(at=later), model.STATUS_EMPTY, open_alert(), NOW, later, THRESHOLDS)
    assert len(outcome.escalated) == 1
    assert outcome.opened == []
    assert outcome.active["severity"] == alerts.SEVERITY_CRITICAL


def test_a_critical_alert_does_not_escalate_twice():
    later = NOW + timedelta(minutes=30)
    previous = open_alert(severity=alerts.SEVERITY_CRITICAL)
    outcome = alerts.evaluate(observation(at=later), model.STATUS_EMPTY, previous, NOW, later, THRESHOLDS)
    assert outcome.escalated == []
    assert outcome.active["severity"] == alerts.SEVERITY_CRITICAL


def test_recovery_closes_the_alert():
    later = NOW + timedelta(minutes=5)
    outcome = alerts.evaluate(observation(bikes=3, at=later), model.STATUS_OK, open_alert(), NOW, later, THRESHOLDS)
    assert len(outcome.closed) == 1
    assert outcome.closed[0]["close_reason"] == alerts.CLOSE_RECOVERED
    assert outcome.closed[0]["status"] == "closed"
    assert outcome.active is None
    assert outcome.condition_since is None


def test_out_of_service_closes_the_alert_with_its_own_reason():
    later = NOW + timedelta(minutes=5)
    outcome = alerts.evaluate(
        observation(docks=0, at=later), model.STATUS_OUT_OF_SERVICE, open_alert("full"), NOW, later, THRESHOLDS
    )
    assert outcome.closed[0]["close_reason"] == alerts.CLOSE_OUT_OF_SERVICE
    assert outcome.active is None


def test_empty_to_full_supersedes_the_old_alert():
    later = NOW + timedelta(minutes=5)
    outcome = alerts.evaluate(observation(docks=0, at=later), model.STATUS_FULL, open_alert(), NOW, later, THRESHOLDS)
    assert outcome.closed[0]["close_reason"] == alerts.CLOSE_SUPERSEDED
    assert outcome.opened == []  # the new kind has to earn its own debounce
    assert outcome.condition_since == later


def test_reprocessing_the_same_batch_keeps_the_same_alert_id():
    later = NOW + timedelta(minutes=2)
    first = alerts.evaluate(observation(at=later), model.STATUS_EMPTY, None, NOW, later, THRESHOLDS).opened[0]
    second = alerts.evaluate(observation(at=later), model.STATUS_EMPTY, None, NOW, later, THRESHOLDS).opened[0]
    assert first["alert_id"] == second["alert_id"]


def test_open_alert_from_hash_reads_the_station_hash():
    stored = {
        "alert_id": "alert-1",
        "alert_kind": "empty",
        "alert_severity": "critical",
        "alert_opened_at": model.iso(NOW),
    }
    assert alerts.open_alert_from_hash(stored) == {
        "alert_id": "alert-1",
        "kind": "empty",
        "severity": "critical",
        "opened_at": model.iso(NOW),
    }
    assert alerts.open_alert_from_hash({}) is None
    assert alerts.open_alert_from_hash(None) is None


def test_events_report_what_happened():
    later = NOW + timedelta(minutes=11)
    outcome = alerts.evaluate(observation(at=later), model.STATUS_EMPTY, open_alert(), NOW, later, THRESHOLDS)
    assert [event for event, _ in outcome.events] == [alerts.EVENT_ESCALATED]
    assert outcome.changed is True
