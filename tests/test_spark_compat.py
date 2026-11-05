"""Checks that Spark understands what ingestion produces.

Uses plain PySpark (no Delta/Kafka jars needed): parses Kafka-style JSON
values with bikeflow.streams.parse_station_status and reads the Parquet files
of the raw Delta table to confirm column types.
"""

import shutil

import pytest

pytestmark = pytest.mark.spark

if shutil.which("java") is None:
    pytest.skip("Java not installed", allow_module_level=True)
pyspark = pytest.importorskip("pyspark")


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[1]")
        .appName("bikeflow-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()


def _value(make_station, i, ts, **kw):
    from bikeflow.ingest.messages import build_message, encode, normalize_station

    return encode(
        build_message(
            normalize_station(make_station(i, **kw)),
            feed_last_updated=ts,
            reason="changed",
            source="live",
            system_id="citibike_nyc",
            ingested_at=ts + 1,
        )
    )


def test_parse_station_status(spark, make_station):
    from bikeflow.streams import parse_station_status

    df = spark.createDataFrame(
        [(bytearray(_value(make_station, 1, 1_762_070_000, bikes=0)), 0, 5)],
        "value binary, kafka_partition int, kafka_offset long",
    )
    out = parse_station_status(df)
    types = dict(out.dtypes)
    assert types["event_time"] == "timestamp" and types["is_renting"] == "boolean"
    assert types["num_bikes_available"] == "bigint"
    assert types["hour_ts"] == "timestamp" and types["local_date"] == "date" and types["weekday_num"] == "bigint"
    row = out.collect()[0]
    assert row.num_bikes_available == 0 and row.kafka_offset == 5
    assert row.event_time.timestamp() == 1_762_070_000  # collect() returns local naive time


def test_raw_table_files_read_by_spark(spark, make_station):
    from bikeflow.ingest.lake_sink import DeltaWriter, message_to_row
    from bikeflow.lake import STATION_STATUS_RAW, table_path

    uri = table_path(STATION_STATUS_RAW)
    rows = [message_to_row(_value(make_station, i, 1_762_070_000), "station_status", 0, i, None) for i in range(3)]
    DeltaWriter(uri, "g").write(rows, {("station_status", 0): 2})
    df = spark.read.parquet(uri)  # the data files of the Delta table
    types = dict(df.dtypes)
    assert types["event_time"] == "timestamp" and types["event_date"] == "date"
    assert types["kafka_offset"] == "bigint" and types["is_installed"] == "boolean"
    assert types["hour_ts"] == "timestamp" and types["local_date"] == "date"
    assert df.count() == 3
