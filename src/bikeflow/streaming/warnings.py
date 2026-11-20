"""30-minute early warnings from the batch demand profiles.

The rule (Deliverable 1, section 4): warn about stations likely to empty or fill
**within the next 30 minutes**, based on their usual demand **at that hour and
weekday**. The demand comes from ``demand_profiles``, the aggregated table built by
the batch layer; the live message only says how many bikes and docks a station has now.

:func:`write_warnings_to_lake` remains a blank function (raising ``NotImplementedError``)
because warnings live in Redis only until a warning history table owner is agreed.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from bikeflow import config
from bikeflow.lake import table_path
from bikeflow.streaming import model

HORIZON_MINUTES = 30
# Below this the profile says nothing useful (fewer than one bike per two hours), in bikes per hour.
MIN_ABS_NET_FLOW = 0.5
# Confidence thresholds, in bikes per hour.
CONFIDENCE_HIGH = 5.0
CONFIDENCE_MEDIUM = 2.0

LIKELY_EMPTY = "likely_empty"
LIKELY_FULL = "likely_full"

DEMAND_PROFILE_COLUMNS = (
    "station_id",
    "weekday_num",
    "hour",
    "departures",
    "arrivals",
)


@dataclass(frozen=True)
class DemandProfile:
    """Usual demand of one station at one hour of one weekday (a 30-day average)."""

    station_id: str
    weekday_num: int
    hour: int
    departures_per_hour: float
    arrivals_per_hour: float

    @property
    def net_flow_per_hour(self) -> float:
        """Bikes gained per hour (arrivals - departures). Negative = the station drains."""
        return self.arrivals_per_hour - self.departures_per_hour

    @property
    def key(self) -> tuple[str, int, int]:
        return (self.station_id, self.weekday_num, self.hour)


def demand_profiles_available(table: str | None = None) -> bool:
    """``True`` once the batch side produces ``demand_profiles``."""
    table_name = table or config.settings.demand_profiles_table
    path = table_path(table_name)
    if "://" in path:
        return True
    return (Path(path) / "_delta_log").is_dir()


def load_demand_profiles(spark=None, table: str | None = None) -> dict[tuple[str, int, int], DemandProfile]:
    """Load expected hourly departures and arrivals from the batch Delta table."""
    table_name = table or config.settings.demand_profiles_table
    path = table_path(table_name)
    if spark is not None:
        df = spark.read.format("delta").load(path)
        return profiles_from_rows(df.collect())

    from deltalake import DeltaTable

    rows = DeltaTable(path).to_pyarrow_table().to_pylist()
    return profiles_from_rows(rows)


def write_warnings_to_lake(warnings: list[dict], spark=None) -> None:
    """BLANK: warnings live in Redis only for now.

    Writing a Delta table is not this module's call: the lake has one writer per
    table and no owner has been agreed for a warning history (``station_warnings``).
    """
    raise NotImplementedError("no warning history table has been agreed with the batch leads yet")


def profiles_from_rows(rows) -> dict[tuple[str, int, int], DemandProfile]:
    """Build the lookup from rows shaped like ``DEMAND_PROFILE_COLUMNS``.

    Accepts dicts or Spark rows, so ``load_demand_profiles`` and the tests share it.
    """
    profiles: dict[tuple[str, int, int], DemandProfile] = {}
    for row in rows:
        data = row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)
        station_id = data.get("station_id")
        if not station_id:
            continue
        profile = DemandProfile(
            station_id=str(station_id),
            weekday_num=model.as_int(data.get("weekday_num"), 1),
            hour=model.as_int(data.get("hour")),
            departures_per_hour=float(data.get("departures") or data.get("departures_per_hour") or 0.0),
            arrivals_per_hour=float(data.get("arrivals") or data.get("arrivals_per_hour") or 0.0),
        )
        profiles[profile.key] = profile
    return profiles


def next_slot(weekday_num: int, hour: int) -> tuple[int, int]:
    """The ``(weekday, hour)`` after this one; after Sunday 23:00 comes Monday 00:00."""
    if hour < 23:
        return weekday_num, hour + 1
    return (weekday_num % 7) + 1, 0


def net_flow_over_horizon(
    profiles: dict[tuple[str, int, int], DemandProfile],
    station_id: str,
    weekday_num: int,
    hour: int,
    minute: int,
    horizon_minutes: int = HORIZON_MINUTES,
) -> float | None:
    """Bikes the station gains over the horizon, or ``None`` when a profile is missing.

    The horizon is split between the current hour (what is left of it) and the next
    one, so a warning at 07:50 looks mostly at the 08:00 profile.
    """
    first = profiles.get((station_id, weekday_num, hour))
    if first is None:
        return None
    horizon_hours = horizon_minutes / 60
    left_in_hour = max(0.0, (60 - minute) / 60)
    first_part = min(horizon_hours, left_in_hour)
    second_part = horizon_hours - first_part
    flow = first.net_flow_per_hour * first_part
    if second_part > 0:
        following = profiles.get((station_id, *next_slot(weekday_num, hour)))
        if following is None:
            return None
        flow += following.net_flow_per_hour * second_part
    return flow


def minutes_until_zero(value: float, change_per_hour: float) -> int | None:
    """Minutes until ``value`` reaches 0 at ``change_per_hour`` (``None`` if it grows)."""
    if change_per_hour >= 0:
        return None
    minutes = value / (-change_per_hour) * 60
    return max(0, int(round(minutes)))


def confidence_for(net_flow_per_hour: float) -> str:
    """How much we trust the forecast: a strong usual flow is a stronger signal."""
    magnitude = abs(net_flow_per_hour)
    if magnitude >= CONFIDENCE_HIGH:
        return "high"
    if magnitude >= CONFIDENCE_MEDIUM:
        return "medium"
    return "low"


def build_warning(
    observation: model.StationObservation,
    status: str,
    profiles: dict[tuple[str, int, int], DemandProfile] | None,
    now: datetime,
    horizon_minutes: int = HORIZON_MINUTES,
    ttl_seconds: int | None = None,
) -> dict | None:
    """The 30-minute warning for one station, or ``None`` when there is nothing to warn about.

    No warning for stations that are out of service or already empty/full (those are
    alerts, not warnings), for stations whose profile is missing, or when the usual
    flow is too weak to say anything.
    """
    if status != model.STATUS_OK or not profiles:
        return None
    # The minute of the hour in New York, like local_hour in the message.
    minute = model.new_york_minute(observation.event_time)
    net_flow = net_flow_over_horizon(
        profiles, observation.station_id, observation.weekday_num, observation.local_hour, minute, horizon_minutes
    )
    if net_flow is None:
        return None
    # net_flow is the bikes gained over the whole horizon; the ETA, the minimum flow and
    # the confidence levels are in bikes per hour, so turn it into an hourly rate.
    flow_per_hour = net_flow / (horizon_minutes / 60)
    if abs(flow_per_hour) < MIN_ABS_NET_FLOW:
        return None

    predicted_bikes = observation.bikes + net_flow
    predicted_docks = observation.docks - net_flow
    if predicted_bikes <= 0:
        kind = LIKELY_EMPTY
        eta = minutes_until_zero(observation.bikes, flow_per_hour)
    elif predicted_docks <= 0:
        kind = LIKELY_FULL
        eta = minutes_until_zero(observation.docks, -flow_per_hour)
    else:
        return None

    ttl = config.settings.warning_ttl_seconds if ttl_seconds is None else ttl_seconds
    return {
        "station_id": observation.station_id,
        "kind": kind,
        "horizon_minutes": horizon_minutes,
        "bikes": observation.bikes,
        "docks": observation.docks,
        "station_size": observation.size,
        "predicted_bikes": round(predicted_bikes, 1),
        "predicted_docks": round(predicted_docks, 1),
        "net_flow_per_hour": round(flow_per_hour, 2),
        "eta_minutes": eta,
        "confidence": confidence_for(flow_per_hour),
        "local_hour": observation.local_hour,
        "weekday_num": observation.weekday_num,
        "basis": "demand_profiles",
        "created_at": model.iso(now),
        "expires_at": model.iso(now + timedelta(seconds=ttl)),
        "expires_at_epoch": model.epoch(now + timedelta(seconds=ttl)),
    }
