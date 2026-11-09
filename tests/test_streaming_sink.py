"""The Redis writer against an in-memory Redis: state, index, alerts, warnings."""

from datetime import datetime, timedelta, timezone

import pytest

from bikeflow.streaming import alerts, model, redis_keys
from bikeflow.streaming.redis_sink import RedisStateWriter

from fake_redis import FakeRedis

NOW = datetime(2026, 11, 4, 12, 0, tzinfo=timezone.utc)


def observation(station_id: str = "station-1", bikes: int = 5, docks: int = 10, at: datetime = NOW):
    return model.StationObservation(
        station_id=station_id,
        system_id="citibike_nyc",
        bikes=bikes,
        docks=docks,
        bikes_disabled=0,
        docks_disabled=0,
        ebikes=1,
        is_installed=True,
        is_renting=True,
        is_returning=True,
        event_time=at,
        last_reported=at,
        local_hour=7,
        weekday_num=3,
    )


def warning_for(station_id: str = "station-1", minutes: int = 15) -> dict:
    expires = NOW + timedelta(minutes=minutes)
    return {
        "station_id": station_id,
        "kind": "likely_empty",
        "eta_minutes": 12,
        "expires_at": model.iso(expires),
        "expires_at_epoch": model.epoch(expires),
    }


@pytest.fixture
def writer():
    return RedisStateWriter(FakeRedis())


def apply(writer, observations, statuses=None, outcomes=None, warnings_list=None, metadata=None, now=NOW) -> dict:
    statuses = statuses or {obs.station_id: model.STATUS_OK for obs in observations}
    return writer.write(
        observations,
        statuses,
        outcomes or {},
        warnings_list or [],
        metadata or {},
        {"now": now, "epoch_id": 1, "rows": len(observations)},
    )


def test_station_state_and_status_index(writer):
    observations = [
        observation(),
        observation("station-2", bikes=0),
        observation("station-3", bikes=2, docks=0),
    ]
    statuses = {"station-1": model.STATUS_OK, "station-2": model.STATUS_EMPTY, "station-3": model.STATUS_FULL}
    stats = apply(writer, observations, statuses)

    record = writer.client.hgetall(redis_keys.station("station-1"))
    assert record["status"] == "ok"
    assert record["bikes"] == "5"
    assert record["station_size"] == "15"
    assert record["event_time"] == model.iso(NOW)
    assert writer.client.smembers(redis_keys.stations_by_status("empty")) == {"station-2"}
    assert writer.client.smembers(redis_keys.stations_by_status("full")) == {"station-3"}
    assert stats["counts"] == {"ok": 1, "empty": 1, "full": 1, "out_of_service": 0}
    assert writer.client.hgetall(redis_keys.stats_live())["total"] == "3"


def test_a_status_change_moves_the_station_between_sets(writer):
    apply(writer, [observation()], {"station-1": model.STATUS_OK})
    apply(writer, [observation(bikes=0)], {"station-1": model.STATUS_EMPTY})
    assert writer.client.smembers(redis_keys.stations_by_status("ok")) == set()
    assert writer.client.smembers(redis_keys.stations_by_status("empty")) == {"station-1"}
    assert writer.client.hgetall(redis_keys.stats_live())["empty"] == "1"


def test_an_opened_alert_is_indexed_and_streamed(writer):
    later = NOW + timedelta(minutes=2)
    outcome = alerts.evaluate(observation(bikes=0, at=later), model.STATUS_EMPTY, None, NOW, later)
    stats = apply(
        writer,
        [observation(bikes=0, at=later)],
        {"station-1": model.STATUS_EMPTY},
        {"station-1": outcome},
        now=later,
    )
    alert_id = outcome.active["alert_id"]
    assert writer.client.zrange(redis_keys.alerts_active(), 0, -1) == [alert_id]
    assert writer.client.hgetall(redis_keys.alert(alert_id))["status"] == "open"
    assert writer.client.zrange(redis_keys.station_alerts("station-1"), 0, -1) == [alert_id]
    assert writer.client.xlen(redis_keys.alerts_events()) == 1
    assert writer.client.hgetall(redis_keys.station("station-1"))["alert_kind"] == model.STATUS_EMPTY
    assert stats["alerts_opened"] == 1
    assert stats["alerts_open"] == 1


def test_a_closed_alert_leaves_the_active_index(writer):
    later = NOW + timedelta(minutes=2)
    opened = alerts.evaluate(observation(bikes=0, at=later), model.STATUS_EMPTY, None, NOW, later)
    apply(
        writer, [observation(bikes=0, at=later)], {"station-1": model.STATUS_EMPTY}, {"station-1": opened}, now=later
    )
    alert_id = opened.active["alert_id"]

    recovered_at = later + timedelta(minutes=3)
    previous = {"alert_id": alert_id, "kind": "empty", "severity": "warning", "opened_at": model.iso(later)}
    closed = alerts.evaluate(observation(bikes=4, at=recovered_at), model.STATUS_OK, previous, NOW, recovered_at)
    apply(
        writer,
        [observation(bikes=4, at=recovered_at)],
        {"station-1": model.STATUS_OK},
        {"station-1": closed},
        now=recovered_at,
    )

    assert writer.client.zrange(redis_keys.alerts_active(), 0, -1) == []
    assert writer.client.zrange(redis_keys.alerts_recent(), 0, -1) == [alert_id]
    assert writer.client.hgetall(redis_keys.alert(alert_id))["close_reason"] == "recovered"
    assert writer.client.zrange(redis_keys.station_alerts("station-1"), 0, -1) == []
    assert writer.client.xlen(redis_keys.alerts_events()) == 2


def test_warnings_are_stored_with_an_expiry(writer):
    apply(writer, [observation()], {"station-1": model.STATUS_OK}, {}, [warning_for()])
    payload = writer.client.hgetall(redis_keys.warning("station-1"))
    assert payload["kind"] == "likely_empty"
    assert "expires_at_epoch" not in payload  # used as the sorted-set score only
    assert writer.client.zrange(redis_keys.warnings_active(), 0, -1) == ["station-1"]
    assert writer.client.hgetall(redis_keys.stats_live())["warnings"] == "1"


def test_expired_warnings_are_removed(writer):
    apply(writer, [observation()], {"station-1": model.STATUS_OK}, {}, [warning_for()])
    later = NOW + timedelta(minutes=20)
    apply(writer, [observation(at=later)], {"station-1": model.STATUS_OK}, {}, [], now=later)
    assert writer.client.zrange(redis_keys.warnings_active(), 0, -1) == []
    assert writer.client.hgetall(redis_keys.warning("station-1")) == {}


def test_a_warning_that_no_longer_applies_is_removed(writer):
    apply(writer, [observation()], {"station-1": model.STATUS_OK}, {}, [warning_for()])
    apply(writer, [observation(at=NOW + timedelta(minutes=1))], {"station-1": model.STATUS_OK}, {}, [])
    assert writer.client.hgetall(redis_keys.warning("station-1")) == {}


def test_station_metadata_is_copied_into_the_hash(writer):
    metadata = {"station-1": {"name": "W 21 St & 6 Ave", "lat": 40.74, "lon": -73.99, "capacity": 30}}
    apply(writer, [observation()], None, {}, [], metadata)
    record = writer.client.hgetall(redis_keys.station("station-1"))
    assert record["name"] == "W 21 St & 6 Ave"
    assert record["lat"] == "40.74"
    assert record["capacity"] == "30"


def test_batch_health_is_recorded(writer):
    apply(writer, [observation()], None, {}, [])
    meta = writer.client.hgetall(redis_keys.meta_batch())
    assert meta["epoch_id"] == "1"
    assert meta["stations"] == "1"
    assert meta["event_time_max"] == model.iso(NOW)


def test_profiles_status_is_recorded(writer):
    writer.write_profiles_status(False)
    assert writer.client.hgetall(redis_keys.meta_profiles())["status"] == "missing"
    writer.write_profiles_status(True, 42, "demand_profiles")
    profiles = writer.client.hgetall(redis_keys.meta_profiles())
    assert profiles["status"] == "loaded"
    assert profiles["rows"] == "42"


def test_snapshot_is_what_the_dashboard_reads(writer):
    apply(writer, [observation(bikes=0)], {"station-1": model.STATUS_EMPTY})
    snapshot = writer.snapshot(limit=5)
    assert snapshot["stats"]["empty"] == "1"
    assert snapshot["empty_stations"][0]["station_id"] == "station-1"
    assert snapshot["alerts"] == []


def test_reset_deletes_only_the_speed_layer_keys(writer):
    apply(writer, [observation()], None, {}, [])
    writer.client.hset("other:key", mapping={"a": 1})
    assert writer.reset() > 0
    assert writer.client.keys() == ["other:key"]


def test_read_previous_returns_one_hash_per_station(writer):
    apply(
        writer,
        [observation(), observation("station-2", bikes=0)],
        {"station-1": model.STATUS_OK, "station-2": model.STATUS_EMPTY},
    )
    previous = writer.read_previous(["station-1", "station-2", "station-3"])
    assert previous["station-1"]["status"] == "ok"
    assert previous["station-2"]["status"] == "empty"
    assert previous["station-3"] == {}

