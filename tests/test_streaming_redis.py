"""The Redis writer against a real Redis server (skipped when none is reachable).

``tests/fake_redis.py`` covers the logic; this proves the real wire format: string
values in hashes, sorted-set scores, the alert stream, the counters and TTLs.

    docker compose up -d redis
    pytest tests/test_streaming_redis.py -q

It writes under its own key prefix (``bftest``) and deletes only that, so a running
live job is never touched.
"""

import os
import socket
import urllib.parse
from datetime import datetime, timedelta, timezone

import pytest

pytestmark = pytest.mark.redis

redis = pytest.importorskip("redis")

from bikeflow import config  # noqa: E402
from bikeflow.streaming import alerts, model, redis_keys  # noqa: E402
from bikeflow.streaming.redis_sink import RedisStateWriter  # noqa: E402

NOW = datetime(2026, 11, 4, 12, 0, tzinfo=timezone.utc)


def client_or_skip():
    """Skip fast when nothing listens (redis-py retries can take a while)."""
    url = os.getenv("REDIS_URL") or config.settings.redis_url
    parsed = urllib.parse.urlparse(url)
    host, port = parsed.hostname or "localhost", parsed.port or 6379
    try:
        with socket.create_connection((host, port), timeout=1):
            pass
    except OSError as exc:
        pytest.skip(f"no Redis listening at {host}:{port} ({exc})")
    return redis.Redis.from_url(url, decode_responses=True, socket_connect_timeout=2)


@pytest.fixture
def writer(monkeypatch):
    client = client_or_skip()
    monkeypatch.setenv("REDIS_KEY_PREFIX", "bftest")
    config.reload_settings()
    writer = RedisStateWriter(client)
    writer.reset()
    yield writer
    writer.reset()
    monkeypatch.delenv("REDIS_KEY_PREFIX", raising=False)
    config.reload_settings()


def observation(station_id: str = "station-1", bikes: int = 0, docks: int = 10) -> model.StationObservation:
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
        event_time=NOW,
        last_reported=NOW,
        local_hour=7,
        weekday_num=3,
        feed_last_updated=model.epoch(NOW),
    )


def test_state_alerts_and_counters_survive_a_real_redis(writer):
    later = NOW + timedelta(minutes=2)
    outcome = alerts.evaluate(observation(), model.STATUS_EMPTY, None, NOW, later)
    stats = writer.write(
        [observation()],
        {"station-1": model.STATUS_EMPTY},
        {"station-1": outcome},
        [],
        {"station-1": {"name": "W 21 St & 6 Ave", "lat": 40.74, "lon": -73.99}},
        {"now": later, "epoch_id": 7, "rows": 1},
    )

    record = writer.client.hgetall(redis_keys.station("station-1"))
    assert record["status"] == model.STATUS_EMPTY
    assert record["bikes"] == "0"
    assert record["name"] == "W 21 St & 6 Ave"
    assert record["event_time"] == "2026-11-04T12:00:00Z"
    assert writer.client.scard(redis_keys.stations_by_status("empty")) == 1

    alert_id = outcome.active["alert_id"]
    assert writer.client.zrange(redis_keys.alerts_active(), 0, -1) == [alert_id]
    assert writer.client.hgetall(redis_keys.alert(alert_id))["severity"] == "warning"
    assert writer.client.xlen(redis_keys.alerts_events()) > 0  # a stream entry was added
    assert writer.client.ttl(redis_keys.alert(alert_id)) > 0

    live = writer.client.hgetall(redis_keys.stats_live())
    assert live["empty"] == "1" and live["alerts_open"] == "1"
    assert stats["alerts_opened"] == 1


def test_recovery_closes_the_alert_in_redis(writer):
    opened_at = NOW + timedelta(minutes=2)
    opened = alerts.evaluate(observation(), model.STATUS_EMPTY, None, NOW, opened_at)
    writer.write([observation()], {"station-1": model.STATUS_EMPTY}, {"station-1": opened}, [], {}, {"now": opened_at})

    recovered_at = opened_at + timedelta(minutes=3)
    previous = {
        "alert_id": opened.active["alert_id"],
        "kind": "empty",
        "severity": "warning",
        "opened_at": model.iso(opened_at),
    }
    closed = alerts.evaluate(observation(bikes=4), model.STATUS_OK, previous, NOW, recovered_at)
    writer.write(
        [observation(bikes=4)],
        {"station-1": model.STATUS_OK},
        {"station-1": closed},
        [],
        {},
        {"now": recovered_at},
    )

    assert writer.client.zcard(redis_keys.alerts_active()) == 0
    assert writer.client.zrange(redis_keys.alerts_recent(), 0, -1) == [opened.active["alert_id"]]
    assert writer.client.hgetall(redis_keys.alert(opened.active["alert_id"]))["close_reason"] == "recovered"
    assert writer.client.smembers(redis_keys.stations_by_status("ok")) == {"station-1"}


def test_warnings_get_a_ttl_and_are_removed_when_they_expire(writer):
    writer.write(
        [observation(bikes=4)],
        {"station-1": model.STATUS_OK},
        {},
        [
            {
                "station_id": "station-1",
                "kind": "likely_empty",
                "eta_minutes": 12,
                "expires_at_epoch": model.epoch(NOW) + 900,
            }
        ],
        {},
        {"now": NOW},
    )
    assert writer.client.hgetall(redis_keys.warning("station-1"))["kind"] == "likely_empty"
    assert 0 < writer.client.ttl(redis_keys.warning("station-1")) <= 900

    later = NOW + timedelta(minutes=16)
    writer.write([observation(bikes=4)], {"station-1": model.STATUS_OK}, {}, [], {}, {"now": later})
    assert writer.client.hgetall(redis_keys.warning("station-1")) == {}


def test_snapshot_matches_what_the_dashboard_reads(writer):
    writer.write([observation()], {"station-1": model.STATUS_EMPTY}, {}, [], {}, {"now": NOW})
    snapshot = writer.snapshot(limit=5)
    assert snapshot["stats"]["empty"] == "1"
    assert snapshot["empty_stations"][0]["station_id"] == "station-1"
