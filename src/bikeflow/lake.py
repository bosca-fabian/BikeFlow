"""Data-lake layout shared by the whole team (same convention as the batch path).

Everything lives under one data folder, ``BIKEFLOW_DATA`` (default ``./data``,
``/app/data`` in Docker)::

    data/
      raw/          files exactly as downloaded or received
        station_status/date=YYYY-MM-DD/hour=HH.jsonl.gz   live feed recordings (producer)
      lake/         Delta tables, one folder per table, flat names
        station_status_raw    every live status message (lake-sink)
        system_alerts_raw     service alerts as change events (producer)
        system_regions        daily region snapshots (producer)
        _checkpoints/         Spark Structured Streaming checkpoints

Batch tables (trips_raw, trips_clean, weather_hourly, stations, station_id_map, ...)
sit next to these in ``data/lake/``. Build paths with :func:`table_path`::

    from bikeflow.lake import STATION_STATUS_RAW, table_path
    df = spark.read.format("delta").load(table_path(STATION_STATUS_RAW))
"""

from __future__ import annotations

from pathlib import Path

from bikeflow import config

# --- Tables written by ingestion -----------------------------------------------------
# Every message received on the `station_status` Kafka topic, one row per message.
STATION_STATUS_RAW = "station_status_raw"
# GBFS system_alerts as change events (appeared / changed / ended), partitioned by event_date.
SYSTEM_ALERTS_RAW = "system_alerts_raw"
# Daily snapshots of GBFS system_regions (region_id -> name), partitioned by snapshot_date.
SYSTEM_REGIONS = "system_regions"


def _is_remote(root: str) -> bool:
    return "://" in root


def lake_root() -> str:
    return config.settings.lake_path


def _join(*parts: str) -> str:
    root = lake_root()
    if _is_remote(root):
        return "/".join([root.rstrip("/"), *parts])
    return str(Path(root, *parts))


def table_path(name: str) -> str:
    """Location of a Delta table, e.g. ``./data/lake/station_status_raw``."""
    if not name or "/" in name or "\\" in name:
        raise ValueError(f"Table names are flat, e.g. 'station_status_raw' (got {name!r})")
    return _join(name)


def checkpoint_path(job_name: str) -> str:
    """Location for a Spark Structured Streaming checkpoint of ``job_name``."""
    return _join("_checkpoints", job_name)


def ensure_local_layout() -> None:
    """Create the lake folder for a local lake (no-op for remote lakes)."""
    root = lake_root()
    if not _is_remote(root):
        Path(root, "_checkpoints").mkdir(parents=True, exist_ok=True)
