"""Ready-made readers for the data that ingestion produces.

Streaming job (Kafka -> Spark Structured Streaming)::

    from bikeflow.spark import get_spark
    from bikeflow.streams import read_station_status_stream
    spark = get_spark("alerts", kafka=True)
    status = read_station_status_stream(spark)                # live topic
    status.withWatermark("event_time", "2 minutes") ...

Batch jobs / dashboard (Delta table in the lake)::

    from bikeflow.streams import read_raw_station_status
    df = read_raw_station_status(spark)                       # every status message ever received
"""

from __future__ import annotations

from bikeflow import config
from bikeflow.lake import STATION_STATUS_RAW, table_path
from bikeflow.schemas import spark_message_ddl

# Uniquely identifies one station observation; used to drop duplicates.
DEDUPE_KEY = ["system_id", "source", "station_id", "feed_last_updated"]


def parse_station_status(df, value_col: str = "value"):
    """Parse the JSON Kafka value into typed columns.

    Works on any DataFrame (batch or streaming) that has the message in
    ``value_col`` as binary or string. Timestamps become real TimestampType.
    """
    from pyspark.sql import functions as F

    parsed = df.withColumn("_msg", F.from_json(F.col(value_col).cast("string"), spark_message_ddl()))
    keep = [c for c in df.columns if c != value_col]
    out = parsed.select(*keep, "_msg.*")
    for ts_col in ("event_time", "hour_ts", "ingested_at", "original_event_time"):
        out = out.withColumn(ts_col, F.to_timestamp(F.col(ts_col)))
    return out.withColumn("local_date", F.to_date(F.col("local_date")))


def read_station_status_stream(
    spark,
    topic: str | None = None,
    starting_offsets: str = "latest",
    max_offsets_per_trigger: int | None = None,
):
    """Streaming DataFrame of station_status messages from Kafka, already parsed.

    Columns: every field from docs/DATA_CONTRACT.md plus kafka_partition,
    kafka_offset and kafka_timestamp.
    """
    from pyspark.sql import functions as F

    s = config.settings
    topic = topic or s.topic_station_status
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", s.kafka_bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
    )
    if max_offsets_per_trigger:
        reader = reader.option("maxOffsetsPerTrigger", str(max_offsets_per_trigger))
    raw = reader.load().select(
        F.col("value"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    )
    return parse_station_status(raw)


def read_raw_station_status(spark, dedupe: bool = True):
    """Batch DataFrame of station_status_raw from the lake.

    ``dedupe`` removes repeated deliveries of the same observation.
    """

    df = spark.read.format("delta").load(table_path(STATION_STATUS_RAW))
    if dedupe:
        df = df.dropDuplicates(DEDUPE_KEY)
    return df
