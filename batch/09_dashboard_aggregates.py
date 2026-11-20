"""Build pre-aggregated Gold Delta tables for the Streamlit dashboard:
    lake/agg_hourly     hourly demand by station and weekday (partitioned by year, month)
    lake/agg_monthly    monthly station & system KPIs (partitioned by year)
    lake/agg_weather    daily ridership joined with weather metrics

Usage:
    python batch/09_dashboard_aggregates.py
    python batch/09_dashboard_aggregates.py --month 2025-09
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable

from common import get_spark, month_args, table_path, write_month


def _prepare_trips(trips: DataFrame, station_map: DataFrame) -> DataFrame:
    """Attach mapped station_uuid (falling back to trip code if unmapped) and flags."""
    uuid_lookup = F.broadcast(
        station_map.select(
            F.col("trip_station_code").alias("code"),
            F.col("station_uuid"),
            F.col("gbfs_name"),
        )
    )
    return (
        trips.join(uuid_lookup, trips["start_station_id"] == uuid_lookup["code"], "left")
        .withColumn("station_id", F.coalesce(F.col("station_uuid"), F.col("start_station_id")))
        .withColumn("station_name", F.coalesce(F.col("gbfs_name"), F.col("start_station_name")))
        .withColumn("is_member", F.when(F.col("member_casual") == "member", 1).otherwise(0))
        .withColumn("is_ebike", F.when(F.col("rideable_type") == "electric_bike", 1).otherwise(0))
    )


def build_hourly_and_monthly(spark, trips_enriched: DataFrame, months: list[tuple[int, int]]):
    """Write partitioned agg_hourly and agg_monthly tables, then Z-Order by station_id."""
    for y, m in months:
        m_df = trips_enriched.where((F.col("year") == y) & (F.col("month") == m))
        # Exclude cross-month boundary spillover dates
        m_df = m_df.where(F.date_format("start_date", "yyyy-MM") == f"{y}-{m:02d}")
        if m_df.isEmpty():
            print(f"{y}-{m:02d}: no rows in trips_clean, skipped")
            continue

        # 1. Hourly aggregates per station + weekday + hour
        hourly = m_df.groupBy(
            "year",
            "month",
            "station_id",
            "station_name",
            "weekday_num",
            "weekday",
            F.col("start_hour").alias("hour"),
        ).agg(
            F.count("*").alias("trips"),
            F.countDistinct("start_date").alias("active_days"),
            F.round(F.avg("duration_min"), 2).alias("avg_duration_min"),
            F.sum("is_member").alias("member_trips"),
            F.sum("is_ebike").alias("ebike_trips"),
        )
        write_month(spark, hourly, "agg_hourly", y, m)

        # 2. Monthly aggregates per station
        monthly = (
            m_df.groupBy("year", "month", "station_id", "station_name")
            .agg(
                F.count("*").alias("trips"),
                F.round(F.avg("duration_min"), 2).alias("avg_duration_min"),
                F.sum("is_member").alias("member_trips"),
                F.sum("is_ebike").alias("ebike_trips"),
            )
            .withColumn("month_label", F.format_string("%04d-%02d", F.col("year"), F.col("month")))
        )

        monthly_path = table_path("agg_monthly")
        writer = monthly.write.format("delta").mode("overwrite").partitionBy("year")
        if DeltaTable.isDeltaTable(spark, monthly_path):
            writer = writer.option("replaceWhere", f"year = {y} AND month = {m}")
        writer.save(monthly_path)
        print(f"{y}-{m:02d}: wrote agg_hourly & agg_monthly")

    # Compact small files and Z-Order by station_id for fast dashboard filtering
    for tbl in ("agg_hourly", "agg_monthly"):
        path = table_path(tbl)
        if DeltaTable.isDeltaTable(spark, path):
            DeltaTable.forPath(spark, path).optimize().executeZOrderBy("station_id")


def build_weather_aggregates(spark, trips_enriched: DataFrame, weather: DataFrame, months: list[tuple[int, int]]):
    """Daily ridership joined with daily weather summaries (matches dashboard/queries.py)."""
    ym_filter = F.lit(False)
    for y, m in months:
        cond = (F.col("year") == y) & (F.col("month") == m)
        ym_filter = ym_filter | cond
    # Each trip counts in its own month only (same rule as agg_monthly): a month's file also
    # holds a few hundred trips that started on the last day of the month before, which
    # would otherwise give that date a second, tiny row.
    own_month = F.date_format("start_date", "yyyy-MM") == F.format_string("%04d-%02d", F.col("year"), F.col("month"))

    daily_trips = (
        trips_enriched.where(ym_filter)
        .where(own_month)
        .groupBy(F.col("start_date").alias("date"), "year", "month", "weekday_num", "weekday")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.avg("duration_min"), 2).alias("avg_duration_min"),
            F.sum("is_member").alias("member_trips"),
            F.sum("is_ebike").alias("ebike_trips"),
        )
    )

    daily_weather = weather.groupBy("date").agg(
        F.round(F.sum("precipitation"), 2).alias("precip_mm"),
        F.round(F.sum("rain"), 2).alias("rain_mm"),
        F.round(F.sum("snowfall"), 2).alias("snow_mm"),
        F.round(F.avg("temperature_2m"), 2).alias("temp_c"),
        F.round(F.max("wind_speed_10m"), 2).alias("wind_speed_max"),
    )

    joined = (
        daily_trips.join(F.broadcast(daily_weather), "date", "inner")
        .withColumn(
            "rain_category",
            F.when(F.col("precip_mm") < 1.0, F.lit("Dry (< 1 mm)"))
            .when(F.col("precip_mm") < 10.0, F.lit("Wet (1–10 mm)"))
            .otherwise(F.lit("Heavy rain (≥ 10 mm)")),
        )
    )

    out_path = table_path("agg_weather")
    writer = joined.coalesce(1).write.format("delta").mode("overwrite")
    if DeltaTable.isDeltaTable(spark, out_path):
        # Replace only the months that were run, so --month keeps the other months.
        months_sql = " OR ".join(f"(year = {y} AND month = {m})" for y, m in months)
        writer = writer.option("replaceWhere", months_sql)
    else:
        writer = writer.option("overwriteSchema", True)
    writer.save(out_path)
    print(f"agg_weather: {joined.count():,} daily rows written to {out_path}")


def main():
    months = month_args(__doc__ or "")
    spark = get_spark("bikeflow-dashboard-aggregates")

    trips = spark.read.format("delta").load(table_path("trips_clean"))
    station_map = spark.read.format("delta").load(table_path("station_id_map"))
    weather = spark.read.format("delta").load(table_path("weather_hourly"))

    trips_enriched = _prepare_trips(trips, station_map)

    build_hourly_and_monthly(spark, trips_enriched, months)
    build_weather_aggregates(spark, trips_enriched, weather, months)

    spark.stop()


if __name__ == "__main__":
    main()
