"""Map the short station codes in the trip files (e.g. 7740.08) to the station
UUIDs of the live feed, through short_name in station_information.

Two passes:
  1. exact match on short_name (the main method);
  2. for codes still unmatched, match on the normalized station name.
Codes that match neither are kept with station_uuid = null (usually stations
that have closed since).

Outputs:
  lake/station_id_map                 one row per trip station code
  data/reports/station_mapping.csv    match rate (by stations and by trips)
  data/reports/unmatched_stations.csv unmatched codes, busiest first

Usage:
    python batch/07_station_mapping.py
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from common import REPORTS, get_spark, table_path


def norm(c):
    c = F.lower(F.trim(c))
    c = F.regexp_replace(c, "&", " and ")
    return F.regexp_replace(c, r"\s+", " ")


def trip_codes(trips: DataFrame) -> DataFrame:
    """code, most frequent name for that code, number of trip endpoints (starts + ends)."""
    ends = (trips.select(F.col("start_station_id").alias("code"), F.col("start_station_name").alias("name"))
                 .unionByName(trips.select(F.col("end_station_id").alias("code"), F.col("end_station_name").alias("name")))
                 .groupBy("code", "name").count())
    w = Window.partitionBy("code").orderBy(F.desc("count"), F.asc("name"))
    best = ends.withColumn("rn", F.row_number().over(w)).where("rn = 1").select("code", "name")
    totals = ends.groupBy("code").agg(F.sum("count").alias("trip_endpoints"))
    return totals.join(best, "code")


def build_map(trips: DataFrame, snapshots: DataFrame) -> DataFrame:
    latest = Window.orderBy(F.desc("snapshot_date"))
    # latest record per short_name
    by_short = (snapshots.where(F.col("short_name").isNotNull())
                .withColumn("rn", F.row_number().over(latest.partitionBy("short_name")))
                .where("rn = 1")
                .select(F.col("short_name").alias("code"),
                        F.col("station_id").alias("uuid_short"),
                        F.col("name").alias("gbfs_name_short")))
    # latest record per normalized name, only names that point to a single station
    named = snapshots.withColumn("name_key", norm(F.col("name")))
    unique_names = named.groupBy("name_key").agg(F.countDistinct("station_id").alias("n")).where("n = 1")
    by_name = (named.join(unique_names, "name_key")
               .withColumn("rn", F.row_number().over(latest.partitionBy("name_key")))
               .where("rn = 1")
               .select("name_key", F.col("station_id").alias("uuid_name"), F.col("name").alias("gbfs_name_name")))

    codes = trip_codes(trips).withColumn("name_key", norm(F.col("name")))
    m = codes.join(by_short, "code", "left").join(by_name, "name_key", "left")
    return m.select(
        F.col("code").alias("trip_station_code"),
        F.col("name").alias("trip_station_name"),
        "trip_endpoints",
        F.coalesce("uuid_short", "uuid_name").alias("station_uuid"),
        F.coalesce("gbfs_name_short", "gbfs_name_name").alias("gbfs_name"),
        F.when(F.col("uuid_short").isNotNull(), "short_name")
         .when(F.col("uuid_name").isNotNull(), "name").alias("match_method"),
    )


def summary(mapping: DataFrame) -> dict:
    s = mapping.agg(
        F.count(F.lit(1)).alias("codes"),
        F.sum(F.when(F.col("match_method") == "short_name", 1).otherwise(0)).alias("codes_by_short_name"),
        F.sum(F.when(F.col("match_method") == "name", 1).otherwise(0)).alias("codes_by_name"),
        F.sum(F.when(F.col("station_uuid").isNull(), 1).otherwise(0)).alias("codes_unmatched"),
        F.sum("trip_endpoints").alias("trip_endpoints"),
        F.sum(F.when(F.col("station_uuid").isNotNull(), F.col("trip_endpoints")).otherwise(0)).alias("endpoints_matched"),
    ).first().asDict()
    s["pct_codes_matched"] = round(100 * (s["codes"] - s["codes_unmatched"]) / s["codes"], 2)
    s["pct_trip_endpoints_matched"] = round(100 * s["endpoints_matched"] / s["trip_endpoints"], 2)
    return s


def main():
    spark = get_spark("bikeflow-station-mapping")
    trips = spark.read.format("delta").load(table_path("trips_clean"))
    snaps = spark.read.format("delta").load(table_path("station_snapshots"))

    mapping = build_map(trips, snaps)
    mapping.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(table_path("station_id_map"))
    mapping = spark.read.format("delta").load(table_path("station_id_map"))

    s = summary(mapping)
    REPORTS.mkdir(parents=True, exist_ok=True)
    import pandas as pd
    pd.DataFrame([s]).to_csv(REPORTS / "station_mapping.csv", index=False)
    unmatched = mapping.where(F.col("station_uuid").isNull()).orderBy(F.desc("trip_endpoints"))
    pd.DataFrame([r.asDict() for r in unmatched.collect()], columns=unmatched.columns) \
        .to_csv(REPORTS / "unmatched_stations.csv", index=False)
    print(f"{s['codes']:,} station codes; matched {s['pct_codes_matched']}% of codes, "
          f"{s['pct_trip_endpoints_matched']}% of trip endpoints "
          f"({s['codes_by_short_name']} by short_name, {s['codes_by_name']} by name)")
    spark.stop()


if __name__ == "__main__":
    main()
