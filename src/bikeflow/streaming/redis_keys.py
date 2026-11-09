"""Every Redis key of the speed layer, in one place (owner: streaming lead).

Human-readable version: ``docs/SPEED_LAYER.md``. The streaming job and the
dashboard both build keys with these functions, so they cannot drift apart.

All keys start with ``REDIS_KEY_PREFIX`` (default ``bf``) and look like::

    bf:station:66db237e-...                 HASH   current state of one station
    bf:stations:empty                       SET    station ids per status
    bf:alert:citibike_nyc:66db...:empty:1793793600   HASH one alert
    bf:alerts:active                        ZSET   open alerts, by opened_at
    bf:alerts:recent                        ZSET   last 100 closed alerts
    bf:alerts:events                        STREAM opened / escalated / closed
    bf:station_alerts:66db...               ZSET   alerts of one station
    bf:warning:66db...                      HASH   the 30-minute warning
    bf:warnings:active                      ZSET   warnings by expiry
    bf:stats:live                           HASH   counters for the dashboard
    bf:meta:batch                           HASH   last micro-batch (job health)
    bf:meta:profiles                        HASH   demand profiles: loaded or missing
"""

from __future__ import annotations

from bikeflow import config

# Status labels written to Redis (same words as the dashboard's status column).
STATUSES = ("ok", "empty", "full", "out_of_service")
# The two statuses that open an alert.
ALERT_KINDS = ("empty", "full")


def prefix() -> str:
    return config.settings.redis_key_prefix.strip(":")


def station(station_id: str) -> str:
    """Current state of one station (hash)."""
    return f"{prefix()}:station:{station_id}"


def stations_by_status(status: str) -> str:
    """Set of station ids with this status; ``SCARD`` is the live counter."""
    return f"{prefix()}:stations:{status}"


def alert(alert_id: str) -> str:
    """One alert, open or recently closed (hash)."""
    return f"{prefix()}:alert:{alert_id}"


def alerts_active() -> str:
    """Sorted set of open alerts, score = ``opened_at`` (epoch seconds)."""
    return f"{prefix()}:alerts:active"


def alerts_recent() -> str:
    """Sorted set of the last closed alerts, score = ``closed_at`` (epoch seconds)."""
    return f"{prefix()}:alerts:recent"


def alerts_events() -> str:
    """Redis stream with one entry per opened / escalated / closed alert."""
    return f"{prefix()}:alerts:events"


def station_alerts(station_id: str) -> str:
    """Sorted set of the open alert ids of one station (map popups)."""
    return f"{prefix()}:station_alerts:{station_id}"


def warning(station_id: str) -> str:
    """The 30-minute warning of one station (hash, expires)."""
    return f"{prefix()}:warning:{station_id}"


def warnings_active() -> str:
    """Sorted set of active warnings, score = expiry (epoch seconds)."""
    return f"{prefix()}:warnings:active"


def stats_live() -> str:
    """Counters of the live view (hash)."""
    return f"{prefix()}:stats:live"


def meta_batch() -> str:
    """Health of the streaming job: last micro-batch (hash)."""
    return f"{prefix()}:meta:batch"


def meta_profiles() -> str:
    """Whether the demand profiles were loaded (hash)."""
    return f"{prefix()}:meta:profiles"


def channel_events() -> str:
    """Pub/sub channel: JSON of every alert event, for instant dashboard refresh."""
    return f"{prefix()}:events"


def alert_id(system_id: str, station_id: str, kind: str, opened_at_epoch: int) -> str:
    """Deterministic id, so reprocessing a micro-batch cannot open the same alert twice."""
    return f"{system_id}:{station_id}:{kind}:{opened_at_epoch}"


def all_prefix_keys() -> list[str]:
    """The keys that hold no per-station data (used by ``bikeflow-stream dump``)."""
    return [
        alerts_active(),
        alerts_recent(),
        alerts_events(),
        warnings_active(),
        stats_live(),
        meta_batch(),
        meta_profiles(),
    ]
