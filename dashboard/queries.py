"""Data access for the dashboard.

Live page: read from the speed layer (Redis), which the streaming job keeps up to date:
station state, open alerts and 30-minute warnings. Keys and fields are documented in
docs/SPEED_LAYER.md; keys are built with `bikeflow.streaming.redis_keys`, so they match
the job. The dashboard only reads Redis, never writes it.

History page: Delta tables read with `deltalake` (no Spark). The aggregates are computed
here from `trips_clean` for the months loaded in the local lake. On the full 2024-2026
data they will come from the batch-processing tables instead; only the functions in the
"History" section change, not the page.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.dataset as ds
import redis
from deltalake import DeltaTable

from bikeflow.config import settings
from bikeflow.lake import table_path
from bikeflow.streaming import redis_keys

NY_TZ = "America/New_York"
WEEKDAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
# Redis status words -> labels shown on the page.
STATUS_LABELS = {"ok": "OK", "empty": "Empty", "full": "Full", "out_of_service": "Out of service"}


def _table(name: str) -> DeltaTable | None:
    path = table_path(name)
    return DeltaTable(path) if Path(path, "_delta_log").exists() else None


def _dataset(name: str) -> ds.Dataset | None:
    t = _table(name)
    return t.to_pyarrow_dataset() if t is not None else None


# ---------------------------------------------------------------- Live


def _hashes(r: redis.Redis, keys: list[str]) -> list[dict]:
    pipe = r.pipeline(transaction=False)
    for key in keys:
        pipe.hgetall(key)
    return [h for h in pipe.execute() if h]


def _numbers(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def _times(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df:
            df[c] = pd.to_datetime(df[c].replace("", None), utc=True, errors="coerce")
    return df


def live_state() -> dict | None:
    """Everything the live page shows, from Redis, or None when the speed layer is empty.

    Returns a dict with:
      stats     bf:stats:live (counters, newest event time, lag behind the feed)
      stations  one row per station (bf:station:{id}), status relabelled for the page
      alerts    open alerts (bf:alerts:active -> bf:alert:{id}), with the station name
      warnings  30-minute warnings (bf:warnings:active -> bf:warning:{id})
      profiles  bf:meta:profiles ("missing" until the demand profiles exist)
    """
    r = redis.Redis.from_url(settings.redis_url, decode_responses=True, socket_timeout=5)
    try:
        stats = r.hgetall(redis_keys.stats_live())
    except redis.ConnectionError:
        return None
    if not stats or not int(stats.get("total") or 0):
        return None

    ids: set[str] = set()
    for status in redis_keys.STATUSES:
        ids |= r.smembers(redis_keys.stations_by_status(status))
    stations = pd.DataFrame(_hashes(r, [redis_keys.station(i) for i in sorted(ids)]))
    stations = _numbers(stations, ["bikes", "ebikes", "bikes_disabled", "docks", "docks_disabled", "lat", "lon"])
    stations = _times(stations, ["event_time", "condition_since", "alert_opened_at"])
    stations["status"] = stations["status"].map(STATUS_LABELS).fillna("Out of service")

    alert_ids = r.zrevrange(redis_keys.alerts_active(), 0, -1)
    alerts = pd.DataFrame(_hashes(r, [redis_keys.alert(a) for a in alert_ids]))
    if not alerts.empty:
        alerts = _numbers(alerts, ["bikes", "docks"])
        alerts = _times(alerts, ["opened_at", "condition_since"])
        alerts = alerts.merge(stations[["station_id", "name"]], on="station_id", how="left")

    warning_ids = r.zrange(redis_keys.warnings_active(), 0, -1)
    warnings = pd.DataFrame(_hashes(r, [redis_keys.warning(w) for w in warning_ids]))
    if not warnings.empty:
        warnings = _numbers(warnings, ["bikes", "docks", "eta_minutes", "net_flow_per_hour"])
        warnings = warnings.merge(stations[["station_id", "name"]], on="station_id", how="left")

    return {
        "stats": stats,
        "stations": stations,
        "alerts": alerts,
        "warnings": warnings,
        "profiles": r.hgetall(redis_keys.meta_profiles()),
    }


# ---------------------------------------------------------------- History


def months_loaded() -> list[str]:
    """Months present in trips_clean, e.g. ['2025-09']."""
    t = _table("trips_clean")
    if t is None:
        return []
    parts = {(int(p["year"]), int(p["month"])) for p in t.partitions()}
    return [f"{y}-{m:02d}" for y, m in sorted(parts)]


def _trips(columns: list[str]):
    return _dataset("trips_clean").to_table(columns=columns)


def _in_loaded_months(dates: pd.Series) -> pd.Series:
    """True for dates inside the loaded months. A monthly trip file also holds a few
    trips that started late on the last day of the previous month (they ended in this
    month), e.g. 847 trips on 31 Aug 2025 in the September file; such partial days
    would distort per-day figures."""
    return pd.to_datetime(dates).dt.strftime("%Y-%m").isin(months_loaded())


def hour_weekday() -> pd.DataFrame:
    """Average trips started per hour, by weekday and hour of day (New York time).

    Averaged over the number of days of each weekday in the loaded months, so
    weekdays that occur 4 or 5 times in a month are comparable.
    """
    t = _trips(["start_date", "weekday_num", "start_hour"])
    g = t.group_by(["start_date", "weekday_num", "start_hour"]).aggregate([([], "count_all")]).to_pandas()
    g = g[_in_loaded_months(g["start_date"])]
    days = g.groupby("weekday_num")["start_date"].nunique()
    df = g.groupby(["weekday_num", "start_hour"])["count_all"].sum().reset_index()
    df["avg_trips"] = df["count_all"] / df["weekday_num"].map(days)
    df["weekday"] = df["weekday_num"].map(lambda d: WEEKDAYS[d - 1])
    return df.rename(columns={"start_hour": "hour"})[["weekday_num", "weekday", "hour", "avg_trips"]]


def monthly() -> pd.DataFrame:
    """Trips per month with the share of members and of e-bikes (in %)."""
    t = _trips(["year", "month", "member_casual", "rideable_type"])
    g = t.group_by(["year", "month", "member_casual", "rideable_type"]).aggregate([([], "count_all")]).to_pandas()
    g["month_label"] = g["year"].astype(int).astype(str) + "-" + g["month"].astype(int).map("{:02d}".format)
    out = g.groupby("month_label").agg(trips=("count_all", "sum")).reset_index()
    members = g[g["member_casual"] == "member"].groupby("month_label")["count_all"].sum()
    ebikes = g[g["rideable_type"] == "electric_bike"].groupby("month_label")["count_all"].sum()
    out["member_pct"] = 100 * out["month_label"].map(members).fillna(0) / out["trips"]
    out["ebike_pct"] = 100 * out["month_label"].map(ebikes).fillna(0) / out["trips"]
    return out


def busiest_stations(top: int = 15) -> pd.DataFrame:
    """Stations with the most trips started. Uses the names in the trip files, because
    some busy stations from the history are no longer in the live feed."""
    t = _trips(["start_station_name"])
    g = t.group_by(["start_station_name"]).aggregate([([], "count_all")]).to_pandas()
    g = g.rename(columns={"start_station_name": "station", "count_all": "trips"})
    return g.nlargest(top, "trips")


def daily_weather() -> pd.DataFrame:
    """Trips per day next to that day's precipitation (mm) and mean temperature (°C).

    Rain classes follow the usual climate-index thresholds: a wet day has at least
    1 mm, a heavy-rain day at least 10 mm.
    """
    trips = _trips(["start_date"]).group_by(["start_date"]).aggregate([([], "count_all")]).to_pandas()
    trips = trips.rename(columns={"start_date": "date", "count_all": "trips"})
    trips = trips[_in_loaded_months(trips["date"])]
    w = _dataset("weather_hourly").to_table(columns=["date", "precipitation", "temperature_2m"]).to_pandas()
    w = w.groupby("date").agg(precip_mm=("precipitation", "sum"), temp_c=("temperature_2m", "mean")).reset_index()
    df = trips.merge(w, on="date", how="inner")
    df["rain"] = pd.cut(
        df["precip_mm"], bins=[-np.inf, 1, 10, np.inf], right=False,
        labels=["Dry (< 1 mm)", "Wet (1–10 mm)", "Heavy rain (≥ 10 mm)"],
    )
    df["weekday"] = pd.to_datetime(df["date"]).dt.dayofweek.map(lambda d: WEEKDAYS[d])
    return df
