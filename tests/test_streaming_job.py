"""``SpeedLayer.process`` end to end with an in-memory Redis (no Spark, no Kafka)."""

from datetime import datetime, timedelta, timezone

import pytest

from bikeflow.streaming import model, redis_keys, warnings
from bikeflow.streaming.job import SpeedLayer
from bikeflow.streaming.redis_sink import RedisStateWriter

from fake_redis import FakeRedis

NOW = datetime(2026, 11, 4, 12, 0, tzinfo=timezone.utc)
AT_0745 = datetime(2026, 11, 4, 12, 45, tzinfo=timezone.utc)


def row(station_id: str = "station-1", bikes: int = 5, docks: int = 10, at: datetime = NOW, **overrides) -> dict:
    """A parsed station_status row, shaped like the rows Spark hands to foreachBatch."""
    data = {
        "station_id": station_id,
        "system_id": "citibike_nyc",
        "source": "live",
        "num_bikes_available": bikes,
        "num_ebikes_available": 1,
        "num_bikes_disabled": 0,
        "num_docks_available": docks,
        "num_docks_disabled": 0,
        "is_installed": True,
        "is_renting": True,
        "is_returning": True,
        "last_reported": int(at.timestamp()),
        "event_time": at,
        "local_hour": 7,
        "weekday_num": 3,
    }
    data.update(overrides)
    return data


@pytest.fixture
def layer():
    return SpeedLayer(RedisStateWriter(FakeRedis()))


def test_the_first_batch_only_starts_the_condition(layer):
    result = layer.process([row(bikes=0)], epoch_id=1)
    assert result.statuses["station-1"] == model.STATUS_EMPTY
    assert result.counts["empty"] == 1
    assert layer.writer.client.zrange(redis_keys.alerts_active(), 0, -1) == []
    assert layer.writer.client.hgetall(redis_keys.station("station-1"))["condition_since"] == model.iso(NOW)


def test_the_second_batch_opens_the_alert(layer):
    layer.process([row(bikes=0)], epoch_id=1)
    later = NOW + timedelta(minutes=2)
    result = layer.process([row(bikes=0, at=later)], epoch_id=2)
    assert result.stats["alerts_opened"] == 1
    active = layer.writer.client.zrange(redis_keys.alerts_active(), 0, -1)
    assert len(active) == 1
    assert layer.writer.client.hgetall(redis_keys.station("station-1"))["alert_kind"] == model.STATUS_EMPTY


def test_a_bike_returned_before_the_debounce_never_alerts(layer):
    layer.process([row(bikes=0)], epoch_id=1)
    later = NOW + timedelta(minutes=1)
    result = layer.process([row(bikes=3, at=later)], epoch_id=2)
    assert result.stats["alerts_opened"] == 0
    assert layer.writer.client.hgetall(redis_keys.station("station-1"))["condition_since"] == ""


def test_the_latest_observation_of_a_station_wins(layer):
    later = NOW + timedelta(minutes=1)
    result = layer.process([row(bikes=0), row(bikes=4, at=later)])
    assert len(result.observations) == 1
    assert result.statuses["station-1"] == model.STATUS_OK


def test_out_of_service_stations_are_not_alerted(layer):
    result = layer.process([row(bikes=0, is_renting=False)])
    assert result.statuses["station-1"] == model.STATUS_OUT_OF_SERVICE
    assert result.stats["alerts_opened"] == 0
    assert layer.writer.client.smembers(redis_keys.stations_by_status("out_of_service")) == {"station-1"}


def test_stale_stations_are_out_of_service(layer):
    stale = NOW - timedelta(hours=2)
    result = layer.process([row(bikes=0, last_reported=int(stale.timestamp()))])
    assert result.statuses["station-1"] == model.STATUS_OUT_OF_SERVICE


def test_rows_without_a_station_or_event_time_are_ignored(layer):
    result = layer.process([row(station_id=None), row(event_time=None), row()])
    assert len(result.observations) == 1


def test_warnings_are_written_when_profiles_exist():
    profiles = warnings.profiles_from_rows(
        [
            {"station_id": "station-1", "weekday_num": 3, "hour": 7, "departures": 20, "arrivals": 5},
            {"station_id": "station-1", "weekday_num": 3, "hour": 8, "departures": 30, "arrivals": 5},
        ]
    )
    writer = RedisStateWriter(FakeRedis())
    result = SpeedLayer(writer, profiles=profiles).process([row(bikes=4, at=AT_0745)])
    assert len(result.warnings) == 1
    assert writer.client.hgetall(redis_keys.warning("station-1"))["kind"] == warnings.LIKELY_EMPTY


def test_no_warnings_without_profiles(layer):
    assert layer.process([row(bikes=4, at=AT_0745)]).warnings == []
    assert layer.writer.client.zrange(redis_keys.warnings_active(), 0, -1) == []


def test_metadata_lands_in_the_station_hash():
    writer = RedisStateWriter(FakeRedis())
    layer = SpeedLayer(writer, station_metadata={"station-1": {"name": "W 21 St & 6 Ave", "lat": 40.74}})
    layer.process([row()])
    record = writer.client.hgetall(redis_keys.station("station-1"))
    assert record["name"] == "W 21 St & 6 Ave"
    assert record["lat"] == "40.74"


def test_a_full_station_gets_a_full_alert(layer):
    layer.process([row(docks=0)], epoch_id=1)
    later = NOW + timedelta(minutes=2)
    result = layer.process([row(docks=0, at=later)], epoch_id=2)
    assert result.statuses["station-1"] == model.STATUS_FULL
    assert result.stats["alerts_opened"] == 1
    alert_id = layer.writer.client.zrange(redis_keys.alerts_active(), 0, -1)[0]
    assert layer.writer.client.hgetall(redis_keys.alert(alert_id))["kind"] == model.STATUS_FULL
