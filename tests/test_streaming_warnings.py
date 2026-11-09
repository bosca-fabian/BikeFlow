"""The 30-minute warnings: the forecast math and the blanks waiting on the batch side."""

from datetime import datetime, timedelta, timezone

import pytest

from bikeflow import config
from bikeflow.streaming import model, warnings

# 12:45 UTC = 07:45 in New York (EST), so 15 minutes are left in the local hour.
AT = datetime(2026, 11, 4, 12, 45, tzinfo=timezone.utc)


def observation(
    station_id: str = "station-1", bikes: int = 4, docks: int = 20, at: datetime = AT
) -> model.StationObservation:
    return model.StationObservation(
        station_id=station_id,
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


def profiles() -> dict:
    rows = [
        # station-1 drains in the morning rush: 15 bikes lost per hour, then 25.
        {"station_id": "station-1", "weekday_num": 3, "hour": 7, "departures": 20, "arrivals": 5},
        {"station_id": "station-1", "weekday_num": 3, "hour": 8, "departures": 30, "arrivals": 5},
        # station-2 fills up at the same hours.
        {"station_id": "station-2", "weekday_num": 3, "hour": 7, "departures": 5, "arrivals": 25},
        {"station_id": "station-2", "weekday_num": 3, "hour": 8, "departures": 5, "arrivals": 25},
    ]
    return warnings.profiles_from_rows(rows)


def test_profiles_from_rows_uses_the_documented_columns():
    table = profiles()
    assert table[("station-1", 3, 7)].net_flow_per_hour == -15.0
    assert table[("station-2", 3, 7)].net_flow_per_hour == 20.0


def test_profiles_from_rows_accepts_long_column_names():
    table = warnings.profiles_from_rows(
        [{"station_id": "s", "weekday_num": 1, "hour": 9, "departures_per_hour": 4, "arrivals_per_hour": 9}]
    )
    assert table[("s", 1, 9)].net_flow_per_hour == 5.0


def test_net_flow_splits_the_horizon_between_two_hours():
    # 15 min left of hour 7 (-15/h) plus 15 min of hour 8 (-25/h) = -10 bikes.
    assert warnings.net_flow_over_horizon(profiles(), "station-1", 3, 7, minute=45) == pytest.approx(-10.0)


def test_net_flow_is_none_without_a_profile():
    assert warnings.net_flow_over_horizon(profiles(), "station-unknown", 3, 7, minute=45) is None


def test_net_flow_needs_the_next_hour_when_the_horizon_crosses_it():
    # At 08:45 the second half of the horizon is 09:00, which does not exist in the profile.
    assert warnings.net_flow_over_horizon(profiles(), "station-1", 3, 8, minute=45) is None


def test_likely_empty_warning():
    warning = warnings.build_warning(observation(bikes=4), model.STATUS_OK, profiles(), AT, 30)
    assert warning is not None
    assert warning["kind"] == warnings.LIKELY_EMPTY
    assert warning["predicted_bikes"] == pytest.approx(-6.0)
    assert warning["eta_minutes"] == 24  # 4 bikes at 10 bikes/hour
    assert warning["confidence"] == "high"
    assert warning["horizon_minutes"] == 30
    assert warning["expires_at_epoch"] == model.epoch(AT) + config.settings.warning_ttl_seconds


def test_likely_full_warning():
    warning = warnings.build_warning(
        observation(station_id="station-2", bikes=10, docks=3), model.STATUS_OK, profiles(), AT, 30
    )
    assert warning is not None
    assert warning["kind"] == warnings.LIKELY_FULL
    assert warning["predicted_docks"] == pytest.approx(-7.0)
    assert warning["eta_minutes"] == 18  # 3 docks at 10 docks/hour


def test_no_warning_when_the_usual_flow_is_weak():
    table = warnings.profiles_from_rows(
        [
            {"station_id": "station-1", "weekday_num": 3, "hour": 7, "departures": 5.2, "arrivals": 5},
            {"station_id": "station-1", "weekday_num": 3, "hour": 8, "departures": 5.2, "arrivals": 5},
        ]
    )
    assert warnings.build_warning(observation(bikes=1), model.STATUS_OK, table, AT, 30) is None


def test_no_warning_when_the_station_is_healthy():
    # 100 bikes will not run out in 30 minutes at 10 bikes/hour.
    assert warnings.build_warning(observation(bikes=100), model.STATUS_OK, profiles(), AT, 30) is None


def test_no_warning_for_stations_that_already_have_an_alert():
    assert warnings.build_warning(observation(bikes=0), model.STATUS_EMPTY, profiles(), AT, 30) is None
    assert warnings.build_warning(observation(), model.STATUS_OUT_OF_SERVICE, profiles(), AT, 30) is None


def test_no_warning_without_profiles():
    assert warnings.build_warning(observation(), model.STATUS_OK, None, AT, 30) is None
    assert warnings.build_warning(observation(), model.STATUS_OK, {}, AT, 30) is None


def test_next_slot_wraps_sunday_night():
    assert warnings.next_slot(3, 7) == (3, 8)
    assert warnings.next_slot(7, 23) == (1, 0)


def test_minutes_until_zero():
    assert warnings.minutes_until_zero(4, -10) == 24
    assert warnings.minutes_until_zero(0, -10) == 0
    assert warnings.minutes_until_zero(4, 10) is None


def test_confidence_follows_the_strength_of_the_flow():
    assert warnings.confidence_for(-12) == "high"
    assert warnings.confidence_for(3) == "medium"
    assert warnings.confidence_for(-1) == "low"


def test_demand_profiles_are_a_blank_function_for_now():
    assert warnings.demand_profiles_available() is False
    with pytest.raises(NotImplementedError):
        warnings.load_demand_profiles()
    with pytest.raises(NotImplementedError):
        warnings.write_warnings_to_lake([{"station_id": "station-1"}])


def test_horizon_is_thirty_minutes():
    assert warnings.HORIZON_MINUTES == 30
    warning = warnings.build_warning(observation(), model.STATUS_OK, profiles(), AT)
    assert warning["horizon_minutes"] == 30
    assert warning["created_at"] == model.iso(AT)
    assert model.as_utc(warning["expires_at"]) == AT + timedelta(seconds=config.settings.warning_ttl_seconds)
