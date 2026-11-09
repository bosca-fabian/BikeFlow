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
    # Recorded daytime data replayed through the same schema (source = "replay").
    topic_station_status_replay: str = field(
        default_factory=lambda: _env("TOPIC_STATION_STATUS_REPLAY", "station_status_replay")
    )
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

    # --- Speed layer: Redis data model (owner: streaming lead) -------------------
    # Every key the streaming job writes starts with this prefix (docs/SPEED_LAYER.md).
    redis_key_prefix: str = field(default_factory=lambda: _env("REDIS_KEY_PREFIX", "bf"))
    # Redis keys never outlive the process that owns them: closed alerts and warnings expire.
    alert_history_ttl_seconds: int = field(default_factory=lambda: _env_int("ALERT_HISTORY_TTL_SECONDS", 3_600))
    warning_ttl_seconds: int = field(default_factory=lambda: _env_int("WARNING_TTL_SECONDS", 900))
    # 0 keeps station state forever (stale stations are detected from `last_reported`).
    station_state_ttl_seconds: int = field(default_factory=lambda: _env_int("STATION_STATE_TTL_SECONDS", 0))

    # --- Speed layer: the Spark Structured Streaming job (owner: streaming lead) --
    stream_job_name: str = field(default_factory=lambda: _env("STREAM_JOB_NAME", "speed-layer"))
    stream_trigger_seconds: int = field(default_factory=lambda: _env_int("STREAM_TRIGGER_SECONDS", 15))
    stream_watermark_minutes: float = field(default_factory=lambda: _env_float("STREAM_WATERMARK_MINUTES", 2))
    stream_starting_offsets: str = field(default_factory=lambda: _env("STREAM_STARTING_OFFSETS", "latest"))
    # Empty/full alerts: the condition must hold this long before an alert opens
    # (one snapshot of noise must not page an operator), then it escalates.
    alert_open_after_minutes: float = field(default_factory=lambda: _env_float("ALERT_OPEN_AFTER_MINUTES", 2))
    alert_critical_after_minutes: float = field(
        default_factory=lambda: _env_float("ALERT_CRITICAL_AFTER_MINUTES", 10)
    )
    # Stations whose last report is older than this are left out of alerts
    # (docs/DATA_CONTRACT.md, "Stations to leave out of alerts").
    alert_stale_minutes: float = field(default_factory=lambda: _env_float("ALERT_STALE_MINUTES", 60))
    # 30-minute warnings: how far ahead we look, and the table with the demand profiles.
    warning_horizon_minutes: int = field(default_factory=lambda: _env_int("WARNING_HORIZON_MINUTES", 30))
    demand_profiles_table: str = field(default_factory=lambda: _env("DEMAND_PROFILES_TABLE", "demand_profiles"))

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
