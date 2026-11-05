"""Load the extracted trip CSVs into the Delta table lake/trips_raw,
one month at a time, partitioned by year and month. All columns stay strings
here (bronze layer); types are fixed in 05_clean_trips.py.

Usage:
    python batch/04_load_trips_raw.py --month 2025-09
    python batch/04_load_trips_raw.py
"""
from pyspark.sql import functions as F

from common import RAW, get_spark, month_args, table_path, upsert_report, write_month

EXPECTED = [
    "ride_id", "rideable_type", "started_at", "ended_at",
    "start_station_name", "start_station_id", "end_station_name", "end_station_id",
    "start_lat", "start_lng", "end_lat", "end_lng", "member_casual",
]


def main():
    months = month_args(__doc__)
    spark = get_spark("bikeflow-load-trips-raw")
    report = []
    for y, m in months:
        src = RAW / "trips" / f"year={y}" / f"month={m:02d}"
        files = sorted(src.glob("*.csv")) if src.exists() else []
        if not files:
            print(f"{y}-{m:02d}: no CSV files, skipped (run 01_download_trips.py first)")
            continue
        # header=True and no schema: every column is read as a string, by name.
        # Station codes like 7740.10 must stay strings, otherwise they become 7740.1.
        df = spark.read.option("header", True).csv([str(f) for f in files])
        missing = [c for c in EXPECTED if c not in df.columns]
        if missing:
            raise ValueError(f"{y}-{m:02d}: missing columns {missing}; found {df.columns}")
        df = (
            df.select(*EXPECTED)
            .withColumn("source_file", F.element_at(F.split(F.input_file_name(), "/"), -1))
            .withColumn("year", F.lit(y))
            .withColumn("month", F.lit(m))
        )
        write_month(spark, df, "trips_raw", y, m)
        n = spark.read.format("delta").load(table_path("trips_raw")) \
            .where(f"year = {y} AND month = {m}").count()
        print(f"{y}-{m:02d}: {n:,} rows from {len(files)} file(s)")
        report.append({"month": f"{y}-{m:02d}", "files": len(files), "rows_raw": n})
    if report:
        print("Report:", upsert_report(report, "load_counts.csv", "month"))
    spark.stop()


if __name__ == "__main__":
    main()
