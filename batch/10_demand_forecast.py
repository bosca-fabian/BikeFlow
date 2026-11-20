"""Stretch Goal: Distributed Time Series STL Decomposition & Rolling Forecast.

Reads:
    lake/trips_clean
    lake/station_id_map

Writes:
    lake/demand_forecast  (station_id, station_name, date, daily_trips, trend, seasonal, resid, forecast_7d)

Usage:
    python batch/10_demand_forecast.py
    python batch/10_demand_forecast.py --first 2025-07 --last 2025-09
"""
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, LongType, StringType, StructField, StructType
from delta.tables import DeltaTable
from statsmodels.tsa.seasonal import STL

from common import get_spark, month_args, table_path


OUTPUT_SCHEMA = StructType([
    StructField("station_id", StringType(), False),
    StructField("station_name", StringType(), True),
    StructField("date", DateType(), False),
    StructField("daily_trips", LongType(), False),
    StructField("trend", DoubleType(), True),
    StructField("seasonal", DoubleType(), True),
    StructField("resid", DoubleType(), True),
    StructField("forecast_7d", DoubleType(), True),
])


def decompose_station_series(pdf: pd.DataFrame) -> pd.DataFrame:
    """Run weekly STL decomposition (period=7) per station on a continuous daily grid."""
    station_id = pdf["station_id"].iloc[0]
    station_name = pdf["station_name"].iloc[0]

    pdf["date"] = pd.to_datetime(pdf["date"]); pdf = pdf.sort_values("date")
    full_idx = pd.date_range(pdf["date"].min(), pdf["date"].max(), freq="D")
    series = (
        pdf.set_index("date")["daily_trips"]
        .reindex(full_idx, fill_value=0)
        .astype(float)
    )

    if len(series) >= 14:
        res = STL(series, period=7, robust=True).fit()
        trend = res.trend
        seasonal = res.seasonal
        resid = res.resid
        # Next-week seasonal baseline = trailing 7-day trend + weekly seasonal component
        forecast = (trend.rolling(7, min_periods=1).mean() + seasonal).clip(lower=0.0)
    else:
        trend = series.rolling(7, min_periods=1).mean()
        seasonal = pd.Series(0.0, index=full_idx)
        resid = series - trend
        forecast = trend

    return pd.DataFrame({
        "station_id": station_id,
        "station_name": station_name,
        "date": full_idx.date,
        "daily_trips": series.astype("int64").values,
        "trend": trend.round(2).values,
        "seasonal": seasonal.round(2).values,
        "resid": resid.round(2).values,
        "forecast_7d": forecast.round(2).values,
    })


def main():
    months = month_args(__doc__ or "")
    spark = get_spark("bikeflow-demand-forecast")

    ym_filter = F.lit(False)
    for y, m in months:
        cond = (F.col("year") == y) & (F.col("month") == m)
        ym_filter = ym_filter | cond
    valid_ym = [f"{y}-{m:02d}" for y, m in months]

    trips = (
        spark.read.format("delta").load(table_path("trips_clean"))
        .where(ym_filter)
        .where(F.date_format("start_date", "yyyy-MM").isin(valid_ym))
    )
    station_map = F.broadcast(
        spark.read.format("delta").load(table_path("station_id_map"))
        .where(F.col("station_uuid").isNotNull())
        .select(
            F.col("trip_station_code").alias("code"),
            F.col("station_uuid").alias("station_id"),
            F.col("gbfs_name").alias("station_name"),
        )
    )

    daily_station = (
        trips.join(station_map, trips["start_station_id"] == station_map["code"], "inner")
        .groupBy("station_id", "station_name", F.col("start_date").alias("date"))
        .agg(F.count("*").alias("daily_trips"))
    )

    forecast_df = daily_station.groupBy("station_id").applyInPandas(
        decompose_station_series, schema=OUTPUT_SCHEMA
    )

    out_path = table_path("demand_forecast")
    forecast_df.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(out_path)
    DeltaTable.forPath(spark, out_path).optimize().executeZOrderBy("station_id")

    print(f"demand_forecast: {forecast_df.count():,} station-days written to {out_path}")
    spark.stop()


if __name__ == "__main__":
    main()
