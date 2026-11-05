"""Clean lake/trips_raw into lake/trips_clean (silver layer), one month at a time,
and write a data-quality report to data/reports/dq_trips.csv.

Each rule below marks bad rows. A row is dropped if it fails at least one rule.
The report shows, per month, how many rows each rule caught (a row can fail
more than one rule, so the rule counts can add up to more than rows_dropped).

Usage:
    python batch/05_clean_trips.py --month 2025-09
    python batch/05_clean_trips.py
"""
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common import BBOX, get_spark, month_args, table_path, upsert_report, write_month

RIDE_TYPES = ["classic_bike", "electric_bike", "docked_bike"]
RIDER_TYPES = ["member", "casual"]


def _in_bbox(lat, lng):
    return (lat.between(BBOX["lat_min"], BBOX["lat_max"])
            & lng.between(BBOX["lng_min"], BBOX["lng_max"]))


def typed(df: DataFrame) -> DataFrame:
    """Cast the string columns of trips_raw to proper types."""
    out = df
    for c in ["ride_id", "rideable_type", "member_casual",
              "start_station_id", "end_station_id", "start_station_name", "end_station_name"]:
        out = out.withColumn(c, F.nullif(F.trim(F.col(c)), F.lit("")))
    out = (out.withColumn("rideable_type", F.lower("rideable_type"))
              .withColumn("member_casual", F.lower("member_casual"))
              .withColumn("started_at", F.to_timestamp("started_at"))
              .withColumn("ended_at", F.to_timestamp("ended_at")))
    for c in ["start_lat", "start_lng", "end_lat", "end_lng"]:
        out = out.withColumn(c, F.col(c).cast("double"))
    return out.withColumn("duration_s", F.unix_timestamp("ended_at") - F.unix_timestamp("started_at"))


def rules():
    """name -> condition that is TRUE for a bad row."""
    return {
        "bad_timestamp": F.col("started_at").isNull() | F.col("ended_at").isNull(),
        "too_short_under_60s": F.col("duration_s") < 60,
        "too_long_over_24h": F.col("duration_s") > 24 * 3600,
        "no_start_station": F.col("start_station_id").isNull(),
        "no_end_station": F.col("end_station_id").isNull(),
        "bad_start_coords": F.col("start_lat").isNull() | F.col("start_lng").isNull()
                            | ~_in_bbox(F.col("start_lat"), F.col("start_lng")),
        "bad_end_coords": F.col("end_lat").isNull() | F.col("end_lng").isNull()
                          | ~_in_bbox(F.col("end_lat"), F.col("end_lng")),
        "unknown_ride_type": ~F.col("rideable_type").isin(RIDE_TYPES) | F.col("rideable_type").isNull(),
        "unknown_rider_type": ~F.col("member_casual").isin(RIDER_TYPES) | F.col("member_casual").isNull(),
    }


def clean_month(raw: DataFrame):
    """Return (clean DataFrame, dict with data-quality counts)."""
    df = typed(raw)
    r = {k: F.coalesce(v, F.lit(False)) for k, v in rules().items()}
    any_bad = reduce(lambda a, b: a | b, r.values())

    counts = df.agg(
        F.count(F.lit(1)).alias("rows_raw"),
        F.countDistinct("ride_id").alias("distinct_ride_ids"),
        F.sum(F.when(any_bad, 1).otherwise(0)).alias("rows_failing_rules"),
        *[F.sum(F.when(c, 1).otherwise(0)).alias(k) for k, c in r.items()],
    ).first().asDict()

    clean = (
        df.filter(~any_bad)
        .dropDuplicates(["ride_id"])
        .withColumn("duration_min", F.round(F.col("duration_s") / 60, 2))
        .withColumn("start_date", F.to_date("started_at"))
        .withColumn("start_hour", F.hour("started_at"))
        .withColumn("start_hour_ts", F.date_trunc("hour", "started_at"))  # join key for weather
        .withColumn("weekday", F.date_format("started_at", "EEEE"))
        .withColumn("weekday_num", ((F.dayofweek("started_at") + 5) % 7) + 1)  # Monday=1 ... Sunday=7
        .withColumn("is_weekend", F.col("weekday_num") >= 6)
        .drop("source_file")
    )
    return clean, counts


def main():
    months = month_args(__doc__)
    spark = get_spark("bikeflow-clean-trips")
    raw_all = spark.read.format("delta").load(table_path("trips_raw"))
    report = []
    for y, m in months:
        raw = raw_all.where(f"year = {y} AND month = {m}")
        clean, dq = clean_month(raw)
        if dq["rows_raw"] == 0:
            print(f"{y}-{m:02d}: nothing in trips_raw, skipped")
            continue
        write_month(spark, clean, "trips_clean", y, m)
        kept = spark.read.format("delta").load(table_path("trips_clean")) \
            .where(f"year = {y} AND month = {m}").count()
        dq.update({
            "month": f"{y}-{m:02d}",
            "duplicate_ride_ids": dq["rows_raw"] - dq["distinct_ride_ids"],
            "rows_clean": kept,
            "rows_dropped": dq["rows_raw"] - kept,
            "pct_kept": round(100 * kept / dq["rows_raw"], 2),
        })
        print(f"{y}-{m:02d}: {dq['rows_raw']:,} raw -> {kept:,} clean ({dq['pct_kept']}% kept)")
        report.append(dq)
    if report:
        print("Report:", upsert_report(report, "dq_trips.csv", "month"))
    spark.stop()


if __name__ == "__main__":
    main()
