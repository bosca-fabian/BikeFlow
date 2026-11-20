"""Data access for the dashboard.

Live page: read from the speed layer (Redis), which the streaming job keeps up to date:
station state, open alerts and 30-minute warnings. Keys and fields are documented in
docs/SPEED_LAYER.md; keys are built with `bikeflow.streaming.redis_keys`, so they match
the job. The dashboard only reads Redis, never writes it.

History page: the gold tables built by batch processing (`batch/09_dashboard_aggregates.py`:
`agg_hourly`, `agg_monthly`, `agg_weather`), read with `deltalake` (no Spark). They are small,
so the page never touches the 125 million rows of `trips_clean`.
"""

from __future__ import annotations

from pathlib import Path

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
# Rain classes as written by batch/09_dashboard_aggregates.py, in display order.
RAIN_CLASSES = ["Dry (< 1 mm)", "Wet (1–10 mm)", "Heavy rain (≥ 10 mm)"]
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
    """Months in the aggregated tables, e.g. ['2025-09']."""
    mo = _dataset("agg_monthly")
    if mo is None:
        return []
    labels = mo.to_table(columns=["month_label"]).column("month_label").unique().to_pylist()
    return sorted(labels)


def hour_weekday() -> pd.DataFrame:
    """Average trips started per hour, by weekday and hour of day (New York time).

    Summed over stations from agg_hourly, then divided by the number of days of each
    weekday in the loaded months, so weekdays that occur 4 or 5 times are comparable.
    """
    t = _dataset("agg_hourly").to_table(columns=["weekday_num", "hour", "trips"])
    df = t.group_by(["weekday_num", "hour"]).aggregate([("trips", "sum")]).to_pandas()
    days = pd.Series(0, index=range(1, 8))
    for month in months_loaded():
        dates = pd.date_range(f"{month}-01", periods=pd.Period(month).days_in_month)
        days = days.add((dates.dayofweek + 1).value_counts(), fill_value=0)
    df["avg_trips"] = df["trips_sum"] / df["weekday_num"].map(days)
    df["weekday"] = df["weekday_num"].map(lambda d: WEEKDAYS[d - 1])
    return df[["weekday_num", "weekday", "hour", "avg_trips"]]


def monthly() -> pd.DataFrame:
    """Trips per month with the share of members and of e-bikes (in %), from agg_monthly."""
    mo = _dataset("agg_monthly").to_table(columns=["month_label", "trips", "member_trips", "ebike_trips"])
    g = mo.to_pandas().groupby("month_label")[["trips", "member_trips", "ebike_trips"]].sum().reset_index()
    g["member_pct"] = 100 * g["member_trips"] / g["trips"]
    g["ebike_pct"] = 100 * g["ebike_trips"] / g["trips"]
    return g[["month_label", "trips", "member_pct", "ebike_pct"]]


def busiest_stations(top: int = 15) -> pd.DataFrame:
    """Stations with the most trips started, from agg_monthly. Mapped stations carry
    their live-feed name; stations no longer in the live feed keep their trip-file name."""
    mo = _dataset("agg_monthly").to_table(columns=["station_name", "trips"])
    g = mo.group_by(["station_name"]).aggregate([("trips", "sum")]).to_pandas()
    g = g.rename(columns={"station_name": "station", "trips_sum": "trips"})
    return g.nlargest(top, "trips")


def daily_weather() -> pd.DataFrame:
    """Trips per day next to that day's precipitation (mm) and mean temperature (°C),
    from agg_weather. Rain classes follow the usual climate-index thresholds: a wet day
    has at least 1 mm, a heavy-rain day at least 10 mm.

    A month's trip file also holds a few hundred trips that started on the last day of
    the month before. agg_weather keeps them as a second, tiny row for that date; they
    are dropped here, as agg_monthly drops them, so each date appears once."""
    cols = ["date", "year", "month", "weekday_num", "trips", "precip_mm", "temp_c", "rain_category"]
    df = _dataset("agg_weather").to_table(columns=cols).to_pandas()
    day = pd.to_datetime(df["date"])
    df = df[(day.dt.year == df.pop("year")) & (day.dt.month == df.pop("month"))].sort_values("date")
    df["rain"] = pd.Categorical(df.pop("rain_category"), categories=RAIN_CLASSES, ordered=True)
    df["weekday"] = df.pop("weekday_num").map(lambda d: WEEKDAYS[d - 1])
    return df
