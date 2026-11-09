"""One way for every team member to get a Delta-enabled SparkSession.

    from bikeflow.spark import get_spark
    spark = get_spark("clean-trips")                # batch job
    spark = get_spark("alerts", kafka=True)         # streaming job reading Kafka

Inside Docker the Delta and Kafka connector jars are baked into PySpark's own
jars folder at build time, so nothing is downloaded at run time. Outside Docker (plain venv on a laptop) the jars are fetched from Maven
on first use via ``delta-spark``'s helper, which needs internet once.
"""

from __future__ import annotations

import glob
import os
import sys
from functools import lru_cache

from bikeflow import config

DELTA_EXTENSIONS = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
}


def scala_version() -> str:
    """Scala build of the installed PySpark: Spark 3.x ships Scala 2.12, Spark 4.x Scala 2.13."""
    import pyspark

    return "2.12" if pyspark.__version__.startswith("3.") else "2.13"


def kafka_package() -> str:
    """Maven coordinate of the Spark-Kafka connector matching the installed PySpark."""
    import pyspark

    return f"org.apache.spark:spark-sql-kafka-0-10_{scala_version()}:{pyspark.__version__}"


@lru_cache(maxsize=1)
def jars_baked_in() -> bool:
    """True when Delta and Kafka jars are already in PySpark's jars folder (Docker image)."""
    import pyspark

    jars_dir = os.path.join(os.path.dirname(pyspark.__file__), "jars")
    return bool(glob.glob(os.path.join(jars_dir, "*delta-spark_*.jar"))) and bool(
        glob.glob(os.path.join(jars_dir, "*spark-sql-kafka-0-10_*.jar"))
    )


def get_spark(app_name: str = "bikeflow", kafka: bool = False, extra_conf: dict | None = None):
    from pyspark.sql import SparkSession

    # PySpark resolves the interpreter of its Python workers from PYSPARK_PYTHON and
    # falls back to "python3", which on Windows is often the Microsoft Store alias and
    # cannot be spawned by the JVM ("Python worker exited unexpectedly"). Point it at
    # the interpreter that runs this code, unless the environment already says otherwise.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    s = config.settings
    builder = (
        SparkSession.builder.appName(app_name)
        .master(s.spark_master)
        .config("spark.driver.memory", s.spark_driver_memory)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    )
    for key, value in DELTA_EXTENSIONS.items():
        builder = builder.config(key, value)
    if "://" not in s.lake_path:
        builder = builder.config("spark.sql.warehouse.dir", os.path.join(os.path.abspath(s.lake_path), "_warehouse"))
    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, value)

    if not jars_baked_in():
        # Plain venv: let delta-spark add the Maven coordinates (downloaded once to ~/.ivy2).
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(builder, extra_packages=[kafka_package()] if kafka else None)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark
