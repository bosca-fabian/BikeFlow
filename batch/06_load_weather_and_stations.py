"""Load the downloaded weather and station snapshots into Delta:
    lake/weather_hourly     one row per New York hour
    lake/station_snapshots  every station from every saved snapshot
    lake/stations           the latest snapshot only (for the dashboard map)

Usage:
    python batch/06_load_weather_and_stations.py
"""
import json
import re

import pandas as pd
from pyspark.sql import functions as F

from common import RAW, get_spark, table_path


def weather_frame() -> pd.DataFrame:
    frames = []
    for f in sorted((RAW / "weather").glob("weather_*.json")):
        hourly = json.loads(f.read_text())["hourly"]
        frames.append(pd.DataFrame(hourly))
    if not frames:
        raise FileNotFoundError("no weather files; run 02_download_weather.py first")
    w = pd.concat(frames, ignore_index=True)
    w["hour_ts"] = pd.to_datetime(w.pop("time"))
    # On the autumn clock change one local hour appears twice; keep the first.
    return w.drop_duplicates("hour_ts").sort_values("hour_ts")


def stations_frame() -> pd.DataFrame:
    frames = []
    for f in sorted((RAW / "station_information").glob("station_information_*.json")):
        snap_date = re.search(r"(\d{4}-\d{2}-\d{2})", f.name)[1]
        st = pd.DataFrame(json.loads(f.read_text())["data"]["stations"])
        st["snapshot_date"] = pd.to_datetime(snap_date).date()
        frames.append(st)
    if not frames:
        raise FileNotFoundError("no station snapshots; run 03_download_station_info.py first")
    st = pd.concat(frames, ignore_index=True)
    keep = ["station_id", "short_name", "name", "lat", "lon", "capacity", "region_id", "snapshot_date"]
    st = st[[c for c in keep if c in st.columns]].copy()
    for c in ["station_id", "short_name", "name", "region_id"]:
        if c in st.columns:
            st[c] = st[c].astype("string")
    st["capacity"] = pd.to_numeric(st["capacity"], errors="coerce").astype("Int64")
    return st


def to_spark(spark, pdf: pd.DataFrame, name: str):
    """Hand a pandas table to Spark through a CSV file, read with Spark's own reader.
    (spark.createDataFrame(pandas) needs Python worker processes, which crash on
    some Windows setups; reading a CSV does not use them.)"""
    tmp = RAW / "_staging"
    tmp.mkdir(parents=True, exist_ok=True)
    path = tmp / f"{name}.csv"
    pdf.to_csv(path, index=False)
    return (spark.read.option("header", True).option("escape", '"')
            .option("inferSchema", False).csv(str(path)))


def main():
    spark = get_spark("bikeflow-weather-stations")

    wp = weather_frame()
    wp["hour_ts"] = wp["hour_ts"].dt.strftime("%Y-%m-%d %H:%M:%S")
    w = to_spark(spark, wp, "weather_hourly")
    for c in w.columns:
        if c != "hour_ts":
            w = w.withColumn(c, F.col(c).cast("double"))
    w = (w.withColumn("hour_ts", F.to_timestamp("hour_ts"))
          .withColumn("date", F.to_date("hour_ts"))
          .withColumn("hour", F.hour("hour_ts"))
          .withColumn("year", F.year("hour_ts")))
    w.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(table_path("weather_hourly"))
    print(f"weather_hourly: {w.count():,} rows")

    sp = stations_frame()
    sp["snapshot_date"] = sp["snapshot_date"].astype(str)
    snaps = to_spark(spark, sp, "station_snapshots")
    snaps = (snaps.withColumn("lat", F.col("lat").cast("double"))
                  .withColumn("lon", F.col("lon").cast("double"))
                  .withColumn("capacity", F.col("capacity").cast("int"))
                  .withColumn("snapshot_date", F.to_date("snapshot_date")))
    snaps.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(table_path("station_snapshots"))
    latest = snaps.agg(F.max("snapshot_date")).first()[0]
    current = snaps.where(F.col("snapshot_date") == F.lit(latest))
    current.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(table_path("stations"))
    print(f"station_snapshots: {snaps.count():,} rows; stations (latest {latest}): {current.count():,}")
    spark.stop()


if __name__ == "__main__":
    main()
