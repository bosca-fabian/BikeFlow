"""The Redis key contract (docs/SPEED_LAYER.md) as code."""

from bikeflow import config
from bikeflow.streaming import model, redis_keys


def test_default_prefix_and_key_names():
    assert redis_keys.station("abc") == "bf:station:abc"
    assert redis_keys.stations_by_status("empty") == "bf:stations:empty"
    assert redis_keys.alert("x") == "bf:alert:x"
    assert redis_keys.alerts_active() == "bf:alerts:active"
    assert redis_keys.alerts_recent() == "bf:alerts:recent"
    assert redis_keys.alerts_events() == "bf:alerts:events"
    assert redis_keys.station_alerts("abc") == "bf:station_alerts:abc"
    assert redis_keys.warning("abc") == "bf:warning:abc"
    assert redis_keys.warnings_active() == "bf:warnings:active"
    assert redis_keys.stats_live() == "bf:stats:live"
    assert redis_keys.meta_batch() == "bf:meta:batch"
    assert redis_keys.meta_profiles() == "bf:meta:profiles"
    assert redis_keys.channel_events() == "bf:events"


def test_prefix_comes_from_the_environment(monkeypatch):
    monkeypatch.setenv("REDIS_KEY_PREFIX", "bikeflow:")
    config.reload_settings()
    try:
        assert redis_keys.station("abc") == "bikeflow:station:abc"
    finally:
        monkeypatch.delenv("REDIS_KEY_PREFIX")
        config.reload_settings()


def test_alert_id_is_deterministic():
    first = redis_keys.alert_id("citibike_nyc", "station-1", "empty", 1_793_793_600)
    second = redis_keys.alert_id("citibike_nyc", "station-1", "empty", 1_793_793_600)
    assert first == second == "citibike_nyc:station-1:empty:1793793600"
    assert redis_keys.alert_id("citibike_nyc", "station-1", "full", 1_793_793_600) != first


def test_status_labels_match_the_model():
    assert redis_keys.STATUSES == model.STATUSES
    assert redis_keys.ALERT_KINDS == model.ALERTABLE_STATUSES
