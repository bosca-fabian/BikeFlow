"""Turn GBFS station_status snapshots into Kafka messages.

Only stations whose state changed since the last snapshot are sent, plus a
full "heartbeat" of every station every HEARTBEAT_MINUTES (and on start-up),
so a consumer that starts late still learns the state of all stations quickly.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from bikeflow.ingest.gbfs import to_epoch_seconds
from bikeflow.schemas import SCHEMA_VERSION

# Fields whose change makes a station worth re-sending.
STATE_FIELDS = (
    "num_bikes_available",
    "num_ebikes_available",
    "num_bikes_disabled",
    "num_docks_available",
    "num_docks_disabled",
    "is_installed",
    "is_renting",
    "is_returning",
)


def _to_int(value) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return int(value)
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _to_bool(value, default: bool = True) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "yes"}


def _ebikes(st: dict) -> int | None:
    if "num_ebikes_available" in st:
        return _to_int(st["num_ebikes_available"])
    types = st.get("num_bikes_available_types")
    if isinstance(types, dict) and "ebike" in types:
        return _to_int(types["ebike"])
    vehicles = st.get("vehicle_types_available")
    if isinstance(vehicles, list):
        counts = [
            _to_int(v.get("count")) or 0
            for v in vehicles
            if isinstance(v, dict)
            and any(k in str(v.get("vehicle_type_id", "")).lower() for k in ("ebike", "electric"))
        ]
        return sum(counts) if counts else None
    return None


YEAR_2000 = 946_684_800  # earlier "times" are placeholders (decommissioned stations report 86400)


def _real_time(value) -> int | None:
    ts = to_epoch_seconds(value)
    return ts if ts is not None and ts >= YEAR_2000 else None


def normalize_station(st: dict) -> dict | None:
    """Map one GBFS station record to the BikeFlow station fields (None if unusable)."""
    station_id = st.get("station_id")
    if station_id in (None, ""):
        return None
    legacy = st.get("legacy_id")
    return {
        "station_id": str(station_id),
        "legacy_id": str(legacy) if legacy not in (None, "") else None,
        "num_bikes_available": _to_int(st.get("num_bikes_available")) or 0,
        "num_ebikes_available": _ebikes(st),
        "num_bikes_disabled": _to_int(st.get("num_bikes_disabled")),
        "num_docks_available": _to_int(st.get("num_docks_available")) or 0,
        "num_docks_disabled": _to_int(st.get("num_docks_disabled")),
        "is_installed": _to_bool(st.get("is_installed")),
        "is_renting": _to_bool(st.get("is_renting")),
        "is_returning": _to_bool(st.get("is_returning")),
        "last_reported": _real_time(st.get("last_reported")),
    }


@dataclass
class Snapshot:
    feed_last_updated: int
    stations: list[dict]


def snapshot_from_feed(feed: dict, fallback_ts: int | None = None) -> Snapshot:
    ts = to_epoch_seconds(feed.get("last_updated"))
    if ts is None:
        if fallback_ts is None:
            raise ValueError("station_status feed has no usable last_updated")
        ts = fallback_ts
    raw = (feed.get("data") or {}).get("stations") or []
    stations = [n for n in (normalize_station(s) for s in raw if isinstance(s, dict)) if n is not None]
    return Snapshot(feed_last_updated=ts, stations=stations)


NEW_YORK = ZoneInfo("America/New_York")


def local_fields(epoch_seconds: int) -> dict:
    """New York local-time helpers for an event time (same definitions as the batch tables)."""
    local = datetime.fromtimestamp(epoch_seconds, tz=NEW_YORK)
    weekday_num = local.isoweekday()  # Monday=1 ... Sunday=7
    hour_start = local.replace(minute=0, second=0, microsecond=0)
    return {
        "hour_ts": iso_utc(hour_start.timestamp()),
        "local_date": local.date().isoformat(),
        "local_hour": local.hour,
        "weekday_num": weekday_num,
        "is_weekend": weekday_num >= 6,
    }


def iso_utc(epoch_seconds: float) -> str:
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class ChangeDetector:
    """Remembers the last state sent per station and decides what to send next."""

    heartbeat_seconds: float = 15 * 60
    _last_state: dict[str, tuple] = field(default_factory=dict)
    _last_heartbeat_ts: int | None = None

    def select(self, snapshot: Snapshot) -> list[tuple[dict, str]]:
        """Return [(station, reason)] to publish for this snapshot."""
        ts = snapshot.feed_last_updated
        if self._last_heartbeat_ts is None:
            mode = "initial"
        elif self.heartbeat_seconds > 0 and ts - self._last_heartbeat_ts >= self.heartbeat_seconds:
            mode = "heartbeat"
        else:
            mode = "delta"
        if mode != "delta":
            self._last_heartbeat_ts = ts

        out: list[tuple[dict, str]] = []
        for st in snapshot.stations:
            state = tuple(st[f] for f in STATE_FIELDS)
            changed = self._last_state.get(st["station_id"]) != state
            self._last_state[st["station_id"]] = state
            if mode == "initial":
                out.append((st, "initial"))
            elif changed:
                out.append((st, "changed"))
            elif mode == "heartbeat":
                out.append((st, "heartbeat"))
        return out


def build_message(
    station: dict,
    *,
    feed_last_updated: int,
    reason: str,
    source: str,
    system_id: str,
    ingested_at: float,
    time_shift_seconds: int = 0,
) -> dict:
    event_ts = feed_last_updated + time_shift_seconds
    msg = {
        "schema_version": SCHEMA_VERSION,
        "system_id": system_id,
        "source": source,
        "reason": reason,
        **station,
        "feed_last_updated": event_ts,
        "event_time": iso_utc(event_ts),
        **local_fields(event_ts),
        "ingested_at": iso_utc(ingested_at),
        "original_event_time": iso_utc(feed_last_updated) if time_shift_seconds else None,
    }
    if time_shift_seconds and msg["last_reported"] is not None:
        msg["last_reported"] = msg["last_reported"] + time_shift_seconds
    return msg


def encode(msg: dict) -> bytes:
    return json.dumps(msg, separators=(",", ":")).encode("utf-8")
