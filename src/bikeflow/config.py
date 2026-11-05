"""Shared configuration for every BikeFlow component.

All settings come from environment variables (set in `.env` / docker-compose),
so the same code runs inside Docker and on a laptop. Import `settings` instead
of hardcoding hosts, topics or paths:

    from bikeflow.config import settings
    settings.kafka_bootstrap      # "kafka:19092" in Docker, "localhost:9092" on the host
    settings.lake_path            # "/app/data/lake" in Docker, "./data/lake" on the host
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value not in (None, "") else default


def _env_int(name: str, default: int) -> int:
    return int(_env(name, str(default)))


def _env_float(name: str, default: float) -> float:
    return float(_env(name, str(default)))


def _env_bool(name: str, default: bool) -> bool:
    return _env(name, "true" if default else "false").strip().lower() in {"1", "true", "yes", "on"}


def _data_sub(name: str) -> str:
    root = _env("BIKEFLOW_DATA", "./data")
    return f"{root.rstrip('/')}/{name}" if "://" in root else str(Path(root) / name)


@dataclass(frozen=True)
class Settings:
    # --- Kafka -------------------------------------------------------------
    kafka_bootstrap: str = field(default_factory=lambda: _env("KAFKA_BOOTSTRAP", "localhost:9092"))
    topic_station_status: str = field(default_factory=lambda: _env("TOPIC_STATION_STATUS", "station_status"))
    topic_partitions: int = field(default_factory=lambda: _env_int("TOPIC_PARTITIONS", 3))
    topic_retention_hours: int = field(default_factory=lambda: _env_int("TOPIC_RETENTION_HOURS", 72))

    # --- Redis (speed layer; data model owned by the streaming job) ---------
    redis_url: str = field(default_factory=lambda: _env("REDIS_URL", "redis://localhost:6379/0"))

    # --- Data folder (same BIKEFLOW_DATA setting as the batch path) -------------
    # data/raw = files as received, data/lake = Delta tables
    data_root: str = field(default_factory=lambda: _env("BIKEFLOW_DATA", "./data"))
    lake_path: str = field(default_factory=lambda: _env("LAKE_PATH", _data_sub("lake")))
    recordings_path: str = field(default_factory=lambda: _env("RECORDINGS_PATH", _data_sub("raw")))

    # --- Live feed --------------------------------------------------------------
    system_id: str = field(default_factory=lambda: _env("SYSTEM_ID", "citibike_nyc"))
    gbfs_discovery_url: str = field(
        default_factory=lambda: _env("GBFS_DISCOVERY_URL", "https://gbfs.citibikenyc.com/gbfs/gbfs.json")
    )
    gbfs_language: str = field(default_factory=lambda: _env("GBFS_LANGUAGE", "en"))
    poll_seconds: float = field(default_factory=lambda: _env_float("POLL_SECONDS", 60))
    heartbeat_minutes: float = field(default_factory=lambda: _env_float("HEARTBEAT_MINUTES", 15))
    record_live: bool = field(default_factory=lambda: _env_bool("RECORD_LIVE", True))
    alerts_every_minutes: float = field(default_factory=lambda: _env_float("ALERTS_EVERY_MINUTES", 5))
    regions_every_hours: float = field(default_factory=lambda: _env_float("REGIONS_EVERY_HOURS", 24))
    http_timeout_seconds: float = field(default_factory=lambda: _env_float("HTTP_TIMEOUT_SECONDS", 20))

    # --- Kafka -> lake sink -----------------------------------------------------
    sink_group_id: str = field(default_factory=lambda: _env("SINK_GROUP_ID", "bikeflow-lake-sink"))
    sink_flush_rows: int = field(default_factory=lambda: _env_int("SINK_FLUSH_ROWS", 50_000))
    sink_flush_seconds: float = field(default_factory=lambda: _env_float("SINK_FLUSH_SECONDS", 120))

    # --- Spark (used by teammates' jobs) ------------------------------------------
    spark_master: str = field(default_factory=lambda: _env("SPARK_MASTER", "local[*]"))
    spark_driver_memory: str = field(default_factory=lambda: _env("SPARK_DRIVER_MEMORY", "4g"))

    @property
    def lake_root(self) -> Path:
        return Path(self.lake_path)

    @property
    def recordings_root(self) -> Path:
        return Path(self.recordings_path)


settings = Settings()


def reload_settings() -> Settings:
    """Re-read the environment (handy in tests and notebooks)."""
    global settings
    settings = Settings()
    return settings
