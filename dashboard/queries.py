"""Data access for the dashboard (reads Delta tables with `deltalake`, no Spark).

Live page: the latest state of each station, from `station_status_raw` (written by
lake-sink, at most ~2 minutes behind the feed). It moves to Redis once the streaming
job writes the live state there.

History page: aggregates computed here from `trips_clean` for the months loaded in the
local lake. On the full 2024-2026 data they will come from the batch-processing tables
instead; only the functions in the "History" section change, not the page.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.dataset as ds
from deltalake import DeltaTable

from bikeflow.lake import STATION_STATUS_RAW, table_path

NY_TZ = "America/New_York"
# Stations whose last report is older than this are treated as out of service
# (docs/DATA_CONTRACT.md, "Stations to leave out of alerts").
STALE_AFTER = timedelta(hours=1)
WEEKDAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def _table(name: str) -> DeltaTable | None:
    path = table_path(name)
    return DeltaTable(path) if Path(path, "_delta_log").exists() else None


def _dataset(name: str) -> ds.Dataset | None:
    t = _table(name)
    return t.to_pyarrow_dataset() if t is not None else None


# ---------------------------------------------------------------- Live


def live_stations() -> pd.DataFrame | None:
    """One row per station: its latest status, location and a status label.

    status is one of: Empty, Full, OK, Out of service (not installed, not renting or
    returning, or no report for over an hour). Empty/full follow the data contract:
    empty = no bike to rent, full = no free dock.
    """
    status_ds, info_ds = _dataset(STATION_STATUS_RAW), _dataset("stations")
    if status_ds is None or info_ds is None:
        return None

    since = (datetime.now(timezone.utc) - timedelta(days=1)).date()
    cols = [
        "station_id", "num_bikes_available", "num_ebikes_available", "num_bikes_disabled",
        "num_docks_available", "num_docks_disabled", "is_installed", "is_renting", "is_returning",
        "last_reported", "event_time",
    ]
    status = status_ds.to_table(columns=cols, filter=ds.field("event_date") >= since).to_pandas()
    if status.empty:
        return None
    # Latest message per station = its current state (heartbeats keep quiet stations fresh).
    status = status.sort_values("event_time").drop_duplicates("station_id", keep="last")

    info = info_ds.to_table(columns=["station_id", "name", "lat", "lon", "region_id"]).to_pandas()
    df = status.merge(info, on="station_id", how="inner")

    reported = pd.to_datetime(df["last_reported"], unit="s", utc=True)
    stale = reported.isna() | (pd.Timestamp.now(tz="UTC") - reported > STALE_AFTER)
    in_service = df["is_installed"] & df["is_renting"] & df["is_returning"] & ~stale
    df["status"] = np.select(
        [~in_service, df["num_bikes_available"].eq(0), df["num_docks_available"].eq(0)],
        ["Out of service", "Empty", "Full"],
        default="OK",
    )
    df["last_change_ny"] = df["event_time"].dt.tz_convert(NY_TZ)
    return df


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
