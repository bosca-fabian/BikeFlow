"""The ``foreachBatch`` entry point against a real Spark DataFrame.

Proves that what ``read_station_status_stream`` produces (Spark Rows with real
timestamps and booleans) goes through ``SpeedLayer.handle_batch`` unchanged.

The rows are read from JSON lines instead of ``createDataFrame``: a DataFrame created
from Python objects is a Python RDD and needs a PySpark Python worker, which some
Windows setups cannot spawn (a Kafka topic is a JVM source, so the real job is not
affected). Needs Java 17; skipped otherwise.
"""

import json
import re
import shutil
import subprocess
from datetime import datetime, timezone

import pytest

pytestmark = pytest.mark.spark


def _java_major() -> int | None:
    exe = shutil.which("java")
    if exe is None:
        return None
    output = subprocess.run([exe, "-version"], capture_output=True, text=True).stderr
    match = re.search(r'version "(\d+)', output)
    return int(match.group(1)) if match else None


_JAVA_MAJOR = _java_major()
if _JAVA_MAJOR is None or _JAVA_MAJOR < 17:
    pytest.skip(f"Spark 3.5 needs Java 17 (found {_JAVA_MAJOR})", allow_module_level=True)
pyspark = pytest.importorskip("pyspark")

from bikeflow.streaming import model, redis_keys  # noqa: E402
from bikeflow.streaming.job import SpeedLayer  # noqa: E402
from bikeflow.streaming.redis_sink import RedisStateWriter  # noqa: E402

from fake_redis import FakeRedis  # noqa: E402

NOW = datetime(2026, 11, 4, 12, 0, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("bikeflow-stream-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    # Same guarantee as get_spark(): timestamps are handed to Python in UTC.
    session.conf.set("spark.sql.session.timeZone", "UTC")
    yield session
    session.stop()


def message(station_id: str = "station-1", bikes: int = 0, docks: int = 10) -> dict:
    """A parsed station_status row, as the Kafka reader produces it (ISO strings for times)."""
    return {
        "station_id": station_id,
        "system_id": "citibike_nyc",
        "source": "live",
        "num_bikes_available": bikes,
        "num_ebikes_available": 0,
        "num_bikes_disabled": 0,
        "num_docks_available": docks,
        "num_docks_disabled": 0,
        "is_installed": True,
        "is_renting": True,
        "is_returning": True,
        "last_reported": int(NOW.timestamp()),
        "feed_last_updated": int(NOW.timestamp()),
        "event_time": "2026-11-04T12:00:00Z",
        "local_hour": 7,
        "weekday_num": 3,
    }


def batch_frame(spark, tmp_path, *messages):
    """A batch DataFrame with ``event_time`` typed, like the real stream."""
    from pyspark.sql import functions as F

    path = tmp_path / "messages.json"
    path.write_text("\n".join(json.dumps(row) for row in messages), encoding="utf-8")
    return spark.read.json(str(path)).withColumn("event_time", F.to_timestamp("event_time"))


def test_handle_batch_reads_spark_rows(spark, tmp_path):
    writer = RedisStateWriter(FakeRedis())
    result = SpeedLayer(writer).handle_batch(batch_frame(spark, tmp_path, message()), 0)

    assert result.rows == 1
    assert result.statuses["station-1"] == model.STATUS_EMPTY
    record = writer.client.hgetall(redis_keys.station("station-1"))
    assert record["status"] == model.STATUS_EMPTY
    assert record["event_time"] == "2026-11-04T12:00:00Z"


def test_handle_batch_keeps_one_row_per_station(spark, tmp_path):
    batch = batch_frame(spark, tmp_path, message(), message("station-2", bikes=4))
    writer = RedisStateWriter(FakeRedis())
    result = SpeedLayer(writer).handle_batch(batch, 1)

    assert len(result.observations) == 2
    assert result.counts[model.STATUS_EMPTY] == 1
    assert result.counts[model.STATUS_OK] == 1
