"""Shared settings and helpers for the BikeFlow batch path."""
import argparse
import os
import sys
from pathlib import Path

# Everything lives under data/ next to the repo, unless BIKEFLOW_DATA points elsewhere.
ROOT = Path(os.environ.get("BIKEFLOW_DATA", Path(__file__).resolve().parent.parent / "data"))
RAW = ROOT / "raw"
LAKE = ROOT / "lake"
REPORTS = ROOT / "reports"
JARS = Path(__file__).resolve().parent.parent / "jars"

FIRST_MONTH = "2024-01"
LAST_MONTH = "2026-09"

NYC_LAT, NYC_LON = 40.7128, -74.0060
NYC_TZ = "America/New_York"
# Rough bounding box around the Citi Bike service area (NYC + Jersey City/Hoboken).
BBOX = {"lat_min": 40.4, "lat_max": 41.0, "lng_min": -74.3, "lng_max": -73.6}


def table_path(name: str) -> str:
    return str(LAKE / name)


def month_range(first: str = FIRST_MONTH, last: str = LAST_MONTH):
    """Yield (year, month) tuples from first to last, inclusive. Format: YYYY-MM."""
    y, m = map(int, first.split("-"))
    ly, lm = map(int, last.split("-"))
    while (y, m) <= (ly, lm):
        yield y, m
        m += 1
        if m == 13:
            y, m = y + 1, 1


def month_args(description: str):
    """Common CLI: --month 2025-09 for one month, or --first/--last for a range."""
    p = argparse.ArgumentParser(description=description)
    p.add_argument("--month", help="process a single month, e.g. 2025-09")
    p.add_argument("--first", default=FIRST_MONTH)
    p.add_argument("--last", default=LAST_MONTH)
    a = p.parse_args()
    if a.month:
        return list(month_range(a.month, a.month))
    return list(month_range(a.first, a.last))


def get_spark(app_name: str):
    """Local Spark session with Delta Lake enabled. Needs Java 17 (or 11).

    If the Delta jars are in the jars/ folder of the repo, they are used directly.
    Otherwise Spark downloads them from Maven on the first run (this can hang on
    some Windows machines, which is why the jars/ folder exists)."""
    from pyspark.sql import SparkSession

    # On Windows Spark may not find the right Python for its worker processes
    # ("Python worker failed to connect back" / "Accept timed out").
    # Point it at the Python that runs this script (the one in .venv).
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.sql.session.timeZone", NYC_TZ)  # trip times are New York local time
        .config("spark.sql.shuffle.partitions", "16")
    )
    jars = sorted(JARS.glob("*.jar")) if JARS.exists() else []
    if jars:
        spark = builder.config("spark.jars", ",".join(str(j) for j in jars)).getOrCreate()
    else:
        from delta import configure_spark_with_delta_pip
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def write_month(spark, df, name: str, year: int, month: int):
    """Write one month into a Delta table partitioned by year/month.
    Re-running a month replaces only that month, so jobs can be repeated safely."""
    from delta.tables import DeltaTable

    path = table_path(name)
    writer = df.write.format("delta").mode("overwrite").partitionBy("year", "month")
    if DeltaTable.isDeltaTable(spark, path):
        writer = writer.option("replaceWhere", f"year = {year} AND month = {month}")
    writer.save(path)


def upsert_report(rows: list, filename: str, key: str):
    """Save report rows to data/reports/<filename>.csv, replacing older rows with the same key."""
    import pandas as pd

    REPORTS.mkdir(parents=True, exist_ok=True)
    out = REPORTS / filename
    new = pd.DataFrame(rows)
    if out.exists():
        old = pd.read_csv(out, dtype={key: str})
        new[key] = new[key].astype(str)
        new = pd.concat([old[~old[key].isin(new[key])], new])
    new.sort_values(key).to_csv(out, index=False)
    return out
