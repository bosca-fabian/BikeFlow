"""Write the speed layer to Redis: current state, alerts and warnings.

One pipeline for the reads (the previous state of the stations in the batch) and
one for the writes, so a micro-batch of 2,519 stations costs a few round trips
instead of thousands.

Every write is idempotent on purpose: writing the same observation twice leaves
the same state, and an alert id is derived from its station, kind and opening
time, so a Spark retry (``foreachBatch`` is at-least-once) cannot open a duplicate
alert. That is what makes the job safe to restart.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from bikeflow import config
from bikeflow.streaming import alerts as alert_rules
from bikeflow.streaming import model, redis_keys

log = logging.getLogger(__name__)

# The last N closed alerts stay in bf:alerts:recent for the dashboard.
RECENT_ALERTS_KEPT = 100
# bf:alerts:events is an audit stream, not a database: cap it.
ALERT_EVENTS_MAXLEN = 20_000
# Columns copied from the batch `stations` table into bf:station:{id}.
METADATA_FIELDS = ("name", "lat", "lon", "region_id", "capacity")


def station_hash(
    observation: model.StationObservation,
    status: str,
    condition_since: datetime | None,
    alert: dict | None,
    now: datetime,
    metadata: dict | None = None,
) -> dict:
    """The fields of ``bf:station:{station_id}``: the live state of one station."""
    record = {
        "station_id": observation.station_id,
        "system_id": observation.system_id,
        "source": observation.source,
        "status": status,
        "bikes": observation.bikes,
        "ebikes": observation.ebikes,
        "bikes_disabled": observation.bikes_disabled,
        "docks": observation.docks,
        "docks_disabled": observation.docks_disabled,
        "station_size": observation.size,
        "is_installed": int(observation.is_installed),
        "is_renting": int(observation.is_renting),
        "is_returning": int(observation.is_returning),
        "last_reported": model.iso(observation.last_reported),
        "event_time": model.iso(observation.event_time),
        "local_hour": observation.local_hour,
        "weekday_num": observation.weekday_num,
        "condition_since": model.iso(condition_since),
        "alert_id": (alert or {}).get("alert_id", ""),
        "alert_kind": (alert or {}).get("kind", ""),
        "alert_severity": (alert or {}).get("severity", ""),
        "alert_opened_at": (alert or {}).get("opened_at", ""),
        "updated_at": model.iso(now),
    }
    for field in METADATA_FIELDS:
        value = (metadata or {}).get(field)
        if value is not None:
            record[field] = value
    return {key: value for key, value in record.items() if value is not None}


class RedisStateWriter:
    """Applies one micro-batch to Redis and can read the state back (used by the CLI)."""

    def __init__(self, client, settings=None):
        self.client = client
        self.settings = settings or config.settings

    @classmethod
    def connect(cls, url: str | None = None, settings=None) -> "RedisStateWriter":
        """Client for ``REDIS_URL``; ``decode_responses`` keeps every value a ``str``."""
        import redis

        settings = settings or config.settings
        client = redis.Redis.from_url(url or settings.redis_url, decode_responses=True, health_check_interval=30)
        return cls(client, settings)

    # ------------------------------------------------------------------ reads
    def ping(self) -> bool:
        """Fail fast when Redis is not reachable (better than a silent empty dashboard)."""
        try:
            return bool(self.client.ping())
        except Exception as exc:  # noqa: BLE001 - any connection problem is "not reachable"
            log.error("Redis is not reachable: %s", exc)
            return False

    def read_previous(self, station_ids: list[str]) -> dict[str, dict]:
        """The current ``bf:station:{id}`` hashes, one ``HGETALL`` per station in one pipeline."""
        if not station_ids:
            return {}
        pipe = self.client.pipeline(transaction=False)
        for station_id in station_ids:
            pipe.hgetall(redis_keys.station(station_id))
        return dict(zip(station_ids, pipe.execute()))

    def read_warnings(self) -> dict[str, dict]:
        """Every active warning, ``{station_id: warning}`` (a few dozen at most)."""
        station_ids = self.client.zrange(redis_keys.warnings_active(), 0, -1)
        if not station_ids:
            return {}
        pipe = self.client.pipeline(transaction=False)
        for station_id in station_ids:
            pipe.hgetall(redis_keys.warning(station_id))
        return {sid: row for sid, row in zip(station_ids, pipe.execute()) if row}

    # ----------------------------------------------------------------- writes
    def write(
        self,
        observations: list[model.StationObservation],
        statuses: dict[str, str],
        outcomes: dict[str, alert_rules.AlertOutcome],
        warnings: list[dict] | None = None,
        metadata: dict[str, dict] | None = None,
        meta: dict | None = None,
    ) -> dict:
        """Apply one micro-batch and return counters (for the log and for the tests)."""
        meta = dict(meta or {})
        now = meta.get("now") or datetime.now(timezone.utc)
        metadata = metadata or {}
        warnings = warnings or []
        # The writer can always say how fresh the batch is, even if the caller did not.
        meta.setdefault("event_time_max", max((obs.event_time for obs in observations), default=None))
        stats = {"stations": 0, "alerts_opened": 0, "alerts_escalated": 0, "alerts_closed": 0, "warnings": 0}
        warnings_by_station = {warning["station_id"]: warning for warning in warnings}

        # Warnings that expired, or that no longer apply to a station in this batch.
        self._cleanup_warnings([obs.station_id for obs in observations], warnings_by_station, now)

        pipe = self.client.pipeline(transaction=False)
        for observation in observations:
            station_id = observation.station_id
            status = statuses.get(station_id, model.STATUS_OK)
            outcome = outcomes.get(station_id) or alert_rules.AlertOutcome()

            # 1. current state of the station
            pipe.hset(
                redis_keys.station(station_id),
                mapping=station_hash(
                    observation, status, outcome.condition_since, outcome.active, now, metadata.get(station_id)
                ),
            )
            if self.settings.station_state_ttl_seconds > 0:
                pipe.expire(redis_keys.station(station_id), self.settings.station_state_ttl_seconds)

            # 2. status index: a station is in exactly one set, so SCARD is the counter
            for other in model.STATUSES:
                if other != status:
                    pipe.srem(redis_keys.stations_by_status(other), station_id)
            pipe.sadd(redis_keys.stations_by_status(status), station_id)

            # 3. alerts and warnings of this station
            self._write_alerts(pipe, observation, outcome, stats)
            self._write_warning(pipe, observation, warnings_by_station.get(station_id), now, stats)
            stats["stations"] += 1

        # 4. counters, read in the same pipeline (after the SADD/SREM above)
        for status in model.STATUSES:
            pipe.scard(redis_keys.stations_by_status(status))
        pipe.zcard(redis_keys.alerts_active())
        pipe.zcard(redis_keys.warnings_active())
        results = pipe.execute()
        stats["counts"] = dict(zip(model.STATUSES, results[-6:-2]))
        stats["alerts_open"] = results[-2]
        stats["warnings_active"] = results[-1]

        self._write_stats(stats, meta, now)
        return stats

    # -------------------------------------------------------------- internals
    def _write_alerts(self, pipe, observation, outcome, stats) -> None:
        """Close, then refresh or open: the same alert id is written, never duplicated."""
        for alert in outcome.closed:
            key = redis_keys.alert(alert["alert_id"])
            pipe.hset(key, mapping=alert)
            pipe.expire(key, self.settings.alert_history_ttl_seconds)
            pipe.zrem(redis_keys.alerts_active(), alert["alert_id"])
            pipe.zrem(redis_keys.station_alerts(observation.station_id), alert["alert_id"])
            pipe.zadd(
                redis_keys.alerts_recent(),
                {alert["alert_id"]: model.epoch(model.as_utc(alert.get("closed_at")))},
            )
            pipe.zremrangebyrank(redis_keys.alerts_recent(), 0, -(RECENT_ALERTS_KEPT + 1))
            self._publish(pipe, alert_rules.EVENT_CLOSED, alert)
            stats["alerts_closed"] += 1

        if outcome.active:
            alert = outcome.active
            key = redis_keys.alert(alert["alert_id"])
            pipe.hset(key, mapping=alert)
            pipe.expire(key, self.settings.alert_history_ttl_seconds)
            opened_at = model.epoch(model.as_utc(alert.get("opened_at")))
            pipe.zadd(redis_keys.alerts_active(), {alert["alert_id"]: opened_at})
            pipe.zadd(redis_keys.station_alerts(observation.station_id), {alert["alert_id"]: opened_at})

        for alert in outcome.opened:
            self._publish(pipe, alert_rules.EVENT_OPENED, alert)
            stats["alerts_opened"] += 1
        for alert in outcome.escalated:
            self._publish(pipe, alert_rules.EVENT_ESCALATED, alert)
            stats["alerts_escalated"] += 1

    def _publish(self, pipe, event: str, alert: dict) -> None:
        """One entry in the audit stream and one pub/sub message (instant dashboard refresh)."""
        record = {
            "event": event,
            "alert_id": alert.get("alert_id", ""),
            "station_id": alert.get("station_id", ""),
            "kind": alert.get("kind", ""),
            "severity": alert.get("severity", ""),
            "opened_at": alert.get("opened_at", ""),
            "at": alert.get("closed_at") or alert.get("updated_at") or "",
            "bikes": alert.get("bikes", 0),
            "docks": alert.get("docks", 0),
        }
        pipe.xadd(redis_keys.alerts_events(), record, maxlen=ALERT_EVENTS_MAXLEN, approximate=True)
        pipe.publish(redis_keys.channel_events(), json.dumps(record))

    def _write_warning(self, pipe, observation, warning: dict | None, now: datetime, stats) -> None:
        """Warnings expire on their own; the station keeps at most one."""
        if not warning:
            return
        key = redis_keys.warning(observation.station_id)
        expires_at = warning.get("expires_at_epoch") or model.epoch(now) + self.settings.warning_ttl_seconds
        payload = {k: v for k, v in warning.items() if v is not None and k != "expires_at_epoch"}
        pipe.hset(key, mapping=payload)
        pipe.expire(key, self.settings.warning_ttl_seconds)
        pipe.zadd(redis_keys.warnings_active(), {observation.station_id: expires_at})
        stats["warnings"] += 1

    def _cleanup_warnings(self, station_ids: list[str], warnings_by_station: dict, now: datetime) -> None:
        """Drop expired warnings and warnings that no longer apply to a station in this batch."""
        pipe = self.client.pipeline(transaction=False)
        pipe.zrangebyscore(redis_keys.warnings_active(), "-inf", model.epoch(now))
        pipe.zrange(redis_keys.warnings_active(), 0, -1)
        expired, active = pipe.execute()
        in_batch = set(station_ids)
        stale = [sid for sid in active if sid in in_batch and sid not in warnings_by_station]
        to_drop = set(expired) | set(stale)
        if not to_drop:
            return
        pipe = self.client.pipeline(transaction=False)
        for station_id in to_drop:
            pipe.delete(redis_keys.warning(station_id))
            pipe.zrem(redis_keys.warnings_active(), station_id)
        pipe.execute()

    def _write_stats(self, stats: dict, meta: dict, now: datetime) -> None:
        """Counters for the live view and health of the last micro-batch."""
        counts = stats.get("counts") or {}
        live = {
            "ok": counts.get(model.STATUS_OK, 0),
            "empty": counts.get(model.STATUS_EMPTY, 0),
            "full": counts.get(model.STATUS_FULL, 0),
            "out_of_service": counts.get(model.STATUS_OUT_OF_SERVICE, 0),
            "total": sum(counts.values()),
            "alerts_open": stats.get("alerts_open", 0),
            "warnings": stats.get("warnings_active", 0),
            "updated_at": model.iso(now),
        }
        # An empty micro-batch (no new message since the last one) has no event time and
        # no lag: keep the previous values instead of blanking them (Redis rejects None).
        if meta.get("event_time_max") is not None:
            live["event_time"] = model.iso(meta["event_time_max"])
            live["lag_seconds"] = "" if meta.get("lag_seconds") is None else meta["lag_seconds"]
        pipe = self.client.pipeline(transaction=False)
        pipe.hset(redis_keys.stats_live(), mapping=live)
        batch = {
            "epoch_id": meta.get("epoch_id"),
            "rows": meta.get("rows"),
            "stations": stats.get("stations"),
            "alerts_opened": stats.get("alerts_opened"),
            "alerts_escalated": stats.get("alerts_escalated"),
            "alerts_closed": stats.get("alerts_closed"),
            "warnings": stats.get("warnings"),
            "duration_ms": meta.get("duration_ms"),
            "event_time_max": model.iso(meta.get("event_time_max")),
            "updated_at": model.iso(now),
        }
        if meta.get("wall_clock"):
            # Wall clock of the machine, next to the stream clock: shows the job is alive.
            batch["job_time"] = model.iso(meta["wall_clock"])
        pipe.hset(redis_keys.meta_batch(), mapping={k: v for k, v in batch.items() if v is not None})
        pipe.execute()

    def write_profiles_status(self, loaded: bool, rows: int = 0, table: str = "") -> None:
        """``bf:meta:profiles``: the dashboard (and we) can see why there are no warnings."""
        self.client.hset(
            redis_keys.meta_profiles(),
            mapping={
                "status": "loaded" if loaded else "missing",
                "rows": rows,
                "table": table,
                "updated_at": model.iso(datetime.now(timezone.utc)),
            },
        )

    # --------------------------------------------------------------- utilities
    def snapshot(self, limit: int = 10) -> dict:
        """What the dashboard sees: counters, open alerts, warnings and a few empty stations."""
        stats = self.client.hgetall(redis_keys.stats_live())
        alerts = []
        for alert_id in self.client.zrevrange(redis_keys.alerts_active(), 0, max(0, limit - 1)) or []:
            record = self.client.hgetall(redis_keys.alert(alert_id))
            if record:
                alerts.append(record)
        station_ids = sorted(self.client.smembers(redis_keys.stations_by_status(model.STATUS_EMPTY)))[:limit]
        stations = [record for record in (self.client.hgetall(redis_keys.station(s)) for s in station_ids) if record]
        return {
            "stats": stats,
            "alerts": alerts,
            "warnings": list(self.read_warnings().values()),
            "profiles": self.client.hgetall(redis_keys.meta_profiles()),
            "empty_stations": stations,
        }

    def reset(self) -> int:
        """Delete every key of the speed layer (development and demos only)."""
        deleted = 0
        cursor = 0
        pattern = f"{redis_keys.prefix()}:*"
        while True:
            cursor, keys = self.client.scan(cursor=cursor, match=pattern, count=1000)
            if keys:
                deleted += int(self.client.delete(*keys) or 0)
            if cursor == 0:
                break
        log.warning("reset: deleted %d keys matching %s", deleted, pattern)
        return deleted



