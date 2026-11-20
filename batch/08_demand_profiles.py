"""Build lake/demand_profiles for the speed layer's 30-minute warnings.

Reads:
    lake/trips_clean
    lake/station_id_map

Writes:
    lake/demand_profiles  (station_id [UUID], weekday_num, hour, departures, arrivals)

Usage:
    python batch/08_demand_profiles.py
    python batch/08_demand_profiles.py --month 2025-09
    python batch/08_demand_profiles.py --first 2025-01 --last 2025-12
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable

from common import get_spark, month_args, table_path


def _filter_months(df: DataFrame, months: list[tuple[int, int]]) -> DataFrame:
    """Keep rows belonging to the requested (year, month) partitions and drop
    spillover trips whose start_date falls outside those months."""
    ym_filter = F.lit(False)
    for y, m in months:
        cond = (F.col("year") == y) & (F.col("month") == m)
        ym_filter = ym_filter | cond
    valid_ym = {f"{y}-{m:02d}" for y, m in months}
    return df.where(ym_filter).where(F.date_format("start_date", "yyyy-MM").isin(list(valid_ym)))


def build_profiles(trips: DataFrame, station_map: DataFrame) -> DataFrame:
    """Compute average hourly departures and arrivals per (station_uuid, weekday_num, hour)."""
    # Broadcast the small station mapping table (2,727 rows) to avoid shuffle joins
    uuid_lookup = F.broadcast(
        station_map.where(F.col("station_uuid").isNotNull())
        .select(
            F.col("trip_station_code").alias("code"),
            F.col("station_uuid").cast("string").alias("station_id"),
        )
    )

    # 1. Departures: map start_station_id -> station_uuid
    departures_raw = (
        trips.join(uuid_lookup, trips["start_station_id"] == uuid_lookup["code"], "inner")
        .select(
            "station_id",
            F.col("start_date").alias("trip_date"),
            F.col("weekday_num").cast("long").alias("weekday_num"),
            F.col("start_hour").cast("long").alias("hour"),
        )
    )

    # 2. Arrivals: map end_station_id -> station_uuid (derive weekday/hour from ended_at)
    arrivals_raw = (
        trips.join(uuid_lookup, trips["end_station_id"] == uuid_lookup["code"], "inner")
        .select(
            "station_id",
            F.to_date("ended_at").alias("trip_date"),
            (((F.dayofweek("ended_at") + 5) % 7) + 1).cast("long").alias("weekday_num"),
            F.hour("ended_at").cast("long").alias("hour"),
        )
    )

    # 3. Determine each station's active date span [first_seen, last_seen]
    all_events = departures_raw.select("station_id", "trip_date").unionByName(
        arrivals_raw.select("station_id", "trip_date")
    )
    station_bounds = all_events.groupBy("station_id").agg(
        F.min("trip_date").alias("min_date"),
        F.max("trip_date").alias("max_date"),
    )

    # Distinct calendar dates present in the dataset with their weekday_num (1=Mon..7=Sun)
    calendar_dates = all_events.select("trip_date").distinct().withColumn(
        "weekday_num", (((F.dayofweek("trip_date") + 5) % 7) + 1).cast("long")
    )

    # Count how many times each weekday occurred while each station was active
    active_weekday_counts = (
        station_bounds.join(
            F.broadcast(calendar_dates),
            (calendar_dates["trip_date"] >= station_bounds["min_date"])
            & (calendar_dates["trip_date"] <= station_bounds["max_date"]),
            "inner",
        )
        .groupBy("station_id", "weekday_num")
        .agg(F.count("*").cast("double").alias("active_days"))
    )

    # 4. Total departures and arrivals per (station_id, weekday_num, hour)
    dep_totals = departures_raw.groupBy("station_id", "weekday_num", "hour").agg(
        F.count("*").cast("double").alias("total_departures")
    )
    arr_totals = arrivals_raw.groupBy("station_id", "weekday_num", "hour").agg(
        F.count("*").cast("double").alias("total_arrivals")
    )

    # 5. Combine, divide by active_days, and match Teodor's exact schema
    combined = (
        dep_totals.join(arr_totals, ["station_id", "weekday_num", "hour"], "full_outer")
        .fillna(0.0, subset=["total_departures", "total_arrivals"])
        .join(active_weekday_counts, ["station_id", "weekday_num"], "inner")
        .select(
            F.col("station_id").cast("string"),
            F.col("weekday_num").cast("long"),
            F.col("hour").cast("long"),
            F.round(F.col("total_departures") / F.col("active_days"), 4).cast("double").alias("departures"),
            F.round(F.col("total_arrivals") / F.col("active_days"), 4).cast("double").alias("arrivals"),
        )
    )
    return combined


def main():
    months = month_args(__doc__ or "")
    spark = get_spark("bikeflow-demand-profiles")

    trips = _filter_months(spark.read.format("delta").load(table_path("trips_clean")), months)
    station_map = spark.read.format("delta").load(table_path("station_id_map"))

    profiles = build_profiles(trips, station_map)

    out_path = table_path("demand_profiles")
    # Compact to 4 files (~2,500 stations * 168 slots = ~420k rows) for instant driver collect()
    profiles.coalesce(4).write.format("delta").mode("overwrite").option("overwriteSchema", True).save(out_path)
    DeltaTable.forPath(spark, out_path).optimize().executeZOrderBy("station_id")

    print(f"demand_profiles: {profiles.count():,} station-hour profiles written to {out_path}")
    spark.stop()


if __name__ == "__main__":
    main()