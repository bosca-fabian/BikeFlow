"""The status rules of docs/DATA_CONTRACT.md, as pure functions (no Spark, no Redis)."""

from datetime import datetime, timedelta, timezone

from bikeflow.streaming import model

NOW = datetime(2026, 11, 4, 12, 0, tzinfo=timezone.utc)


def observation(**overrides) -> model.StationObservation:
    fields = dict(
        station_id="station-1",
        system_id="citibike_nyc",
        bikes=5,
        docks=10,
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
    )
    fields.update(overrides)
    return model.StationObservation(**fields)


def test_empty_when_there_is_no_bike_to_rent():
    assert model.classify(observation(bikes=0), NOW) == model.STATUS_EMPTY


def test_disabled_bikes_do_not_count_as_available():
    # Contract: a dock holding a broken bike cannot rent, so 0 available + 3 disabled is empty.
    assert model.classify(observation(bikes=0, bikes_disabled=3), NOW) == model.STATUS_EMPTY


def test_full_when_there_is_no_free_dock():
    assert model.classify(observation(docks=0), NOW) == model.STATUS_FULL


def test_ok_when_bikes_and_docks_are_available():
    assert model.classify(observation(), NOW) == model.STATUS_OK


def test_out_of_service_when_not_installed():
    assert model.classify(observation(is_installed=False, bikes=0), NOW) == model.STATUS_OUT_OF_SERVICE


def test_out_of_service_when_not_renting_or_not_returning():
    assert model.classify(observation(is_renting=False, bikes=0), NOW) == model.STATUS_OUT_OF_SERVICE
    assert model.classify(observation(is_returning=False, docks=0), NOW) == model.STATUS_OUT_OF_SERVICE


def test_out_of_service_when_the_station_is_stale():
    stale = NOW - timedelta(minutes=61)
    assert model.classify(observation(bikes=0, last_reported=stale), NOW) == model.STATUS_OUT_OF_SERVICE


def test_a_report_just_under_an_hour_is_still_trusted():
    fresh = NOW - timedelta(minutes=59)
    assert model.classify(observation(bikes=0, last_reported=fresh), NOW) == model.STATUS_EMPTY


def test_a_station_that_never_reported_is_out_of_service():
    assert model.classify(observation(bikes=0, last_reported=None), NOW) == model.STATUS_OUT_OF_SERVICE


def test_staleness_uses_the_epoch_fields_so_it_cannot_be_shifted_by_a_time_zone():
    # feed_last_updated and last_reported are epoch seconds from the feed: even if
    # event_time arrives shifted (Spark session time zone), the age is exact.
    obs = observation(
        bikes=0,
        event_time=NOW + timedelta(hours=2),
        last_reported=NOW,
        feed_last_updated=model.epoch(NOW),
    )
    assert obs.age_seconds == 0
    assert model.classify(obs, obs.event_time) == model.STATUS_EMPTY

    stale = observation(
        bikes=0,
        event_time=NOW + timedelta(hours=2),
        last_reported=NOW - timedelta(hours=2),
        feed_last_updated=model.epoch(NOW),
    )
    assert stale.age_seconds == 7200
    assert model.classify(stale, stale.event_time) == model.STATUS_OUT_OF_SERVICE


def test_station_size_comes_from_the_message_not_the_capacity():
    assert observation(bikes=2, docks=3, bikes_disabled=1, docks_disabled=4).size == 10


def test_from_row_accepts_kafka_json_values():
    row = {
        "station_id": "station-9",
        "system_id": "citibike_nyc",
        "num_bikes_available": "0",
        "num_docks_available": 12,
        "num_bikes_disabled": None,
        "num_docks_disabled": None,
        "num_ebikes_available": "2",
        "is_installed": 1,
        "is_renting": True,
        "is_returning": 1,
        "last_reported": 1_793_793_600,
        "event_time": "2026-11-04T12:00:00Z",
        "local_hour": 7,
        "weekday_num": 3,
        "source": "live",
    }
    obs = model.StationObservation.from_row(row)
    assert obs is not None
    assert obs.station_id == "station-9"
    assert obs.bikes == 0 and obs.ebikes == 2
    assert obs.event_time == NOW
    assert obs.last_reported == NOW
    assert obs.in_service is True


def test_from_row_skips_unusable_rows():
    assert model.StationObservation.from_row({"station_id": None, "event_time": NOW}) is None
    assert model.StationObservation.from_row({"station_id": "station-1"}) is None
    assert model.StationObservation.from_row({"station_id": "station-1", "event_time": "not a time"}) is None


def test_event_time_comes_from_the_epoch_field_when_present():
    # PySpark returns timestamp columns as naive local time; the epoch field is exact.
    row = {
        "station_id": "station-9",
        "system_id": "citibike_nyc",
        "num_bikes_available": 3,
        "num_docks_available": 7,
        "is_installed": True,
        "is_renting": True,
        "is_returning": True,
        "last_reported": 1_793_793_600,
        "feed_last_updated": 1_793_793_600,
        "event_time": "2026-11-04T14:00:00Z",  # shifted by a non-UTC session
        "local_hour": 7,
        "weekday_num": 3,
    }
    obs = model.StationObservation.from_row(row)
    assert obs.event_time == NOW
    assert obs.age_seconds == 0


def test_latest_per_station_keeps_the_newest_row():
    older = observation(bikes=1, event_time=NOW - timedelta(minutes=1))
    newer = observation(bikes=2)
    other = observation(station_id="station-2")
    kept = {obs.station_id: obs for obs in model.latest_per_station([older, newer, other])}
    assert len(kept) == 2
    assert kept["station-1"].bikes == 2


def test_status_counts_includes_every_status():
    counts = model.status_counts({"a": model.STATUS_EMPTY, "b": model.STATUS_EMPTY, "c": model.STATUS_OK})
    assert counts == {"ok": 1, "empty": 2, "full": 0, "out_of_service": 0}


def test_timestamp_helpers_round_trip():
    assert model.iso(NOW) == "2026-11-04T12:00:00Z"
    assert model.epoch(NOW) == 1_793_793_600
    assert model.as_utc("2026-11-04T12:00:00Z") == NOW
    assert model.as_utc(1_793_793_600) == NOW
    assert model.iso(None) == ""
    assert model.as_utc(None) is None


def test_new_york_minute_follows_the_message_time_zone():
    assert model.new_york_minute(datetime(2026, 11, 4, 12, 30, tzinfo=timezone.utc)) == 30
