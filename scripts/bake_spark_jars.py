"""Docker build step: download the Delta Lake and Spark-Kafka jars once and put
them in PySpark's own jars folder, so containers never download anything at
run time and every teammate runs exactly the same jars.

Versions follow the installed pip packages (delta-spark, pyspark).
"""

from __future__ import annotations

import glob
import importlib.metadata as md
import os
import re
import shutil
import sys
import tempfile

import pyspark
from pyspark.sql import SparkSession

# Spark 3.x is built for Scala 2.12, Spark 4.x for Scala 2.13 (same rule as bikeflow.spark).
SCALA = "2.12" if pyspark.__version__.startswith("3.") else "2.13"

JAR_RE = re.compile(r"^(?P<artifact>.+?)-(?P<version>\d+\.[\w.\-]*)\.jar$")


def artifact_name(jar_file: str) -> str:
    """'org.apache.kafka_kafka-clients-3.9.0.jar' -> 'kafka-clients'; 'zstd-jni-1.5.6-9.jar' -> 'zstd-jni'."""
    name = os.path.basename(jar_file)
    if "_" in name.split("-", 1)[0]:  # ivy prefixes jars with '<organisation>_'
        name = name.split("_", 1)[1]
    m = JAR_RE.match(name)
    return m.group("artifact") if m else name


def main() -> int:
    packages = [
        f"io.delta:delta-spark_{SCALA}:{md.version('delta-spark')}",
        f"org.apache.spark:spark-sql-kafka-0-10_{SCALA}:{pyspark.__version__}",
    ]
    ivy_dir = tempfile.mkdtemp(prefix="ivy-")
    print("Resolving", packages, flush=True)
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("bake-jars")
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.jars.ivy", ivy_dir)
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.stop()

    target = os.path.join(os.path.dirname(pyspark.__file__), "jars")
    present = {artifact_name(j) for j in glob.glob(os.path.join(target, "*.jar"))}
    copied, skipped = [], []
    for jar in sorted(glob.glob(os.path.join(ivy_dir, "jars", "*.jar"))):
        if artifact_name(jar) in present:
            skipped.append(os.path.basename(jar))  # Spark already ships this library
            continue
        shutil.copy2(jar, target)
        copied.append(os.path.basename(jar))
    shutil.rmtree(ivy_dir, ignore_errors=True)
    print("Added to", target, ":", *copied, sep="\n  ")
    if skipped:
        print("Skipped (already in Spark):", *skipped, sep="\n  ")
    if not any("delta-spark_" in j for j in copied) or not any("spark-sql-kafka" in j for j in copied):
        print("ERROR: Delta or Kafka connector jar missing", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
