"""GBFS ``system_alerts`` -> ``data/lake/system_alerts_raw`` as change events.

Polled by the producer every ALERTS_EVERY_MINUTES (default 5). Instead of
storing the same alert again on every poll, a row is written only when
something happens to an alert:

    appeared  first time we see this alert_id
    changed   its content changed (times, stations, text, ...)
    ended     it is no longer in the feed

Currently active alerts = for each alert_id, its latest event, if that event
is not 'ended'. On restart the poller rebuilds that state from the table, so
nothing is reported twice.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from datetime import datetime, timezone

import pyarrow as pa

from bikeflow.ingest.gbfs import to_epoch_seconds

log = logging.getLogger(__name__)

ALERTS_SCHEMA = pa.schema(
    [
        ("alert_id", pa.string()),
        ("event", pa.string()),  # appeared | changed | ended
        ("type", pa.string()),  # SYSTEM_CLOSURE, STATION_CLOSURE, STATION_MOVE, OTHER, ...
        ("summary", pa.string()),
        ("description", pa.string()),
        ("url", pa.string()),
        ("station_ids", pa.list_(pa.string())),
        ("region_ids", pa.list_(pa.string())),
        ("start_time", pa.timestamp("us", tz="UTC")),  # earliest start in `times`
        ("end_time", pa.timestamp("us", tz="UTC")),  # latest end in `times` (null = open-ended)
        ("alert_last_updated", pa.timestamp("us", tz="UTC")),
        ("content_hash", pa.string()),
        ("raw_json", pa.string()),
        ("system_id", pa.string()),
        ("observed_at", pa.timestamp("us", tz="UTC")),
        ("event_date", pa.date32()),  # partition column (UTC date of observed_at)
    ]
)


def _ts(value) -> datetime | None:
    s = to_epoch_seconds(value)
    return datetime.fromtimestamp(s, tz=timezone.utc) if s else None


def _text(v):
    if isinstance(v, list):  # GBFS v3 localized strings
        for item in v:
            if isinstance(item, dict) and str(item.get("language", "")).startswith("en"):
                return item.get("text")
        return v[0].get("text") if v and isinstance(v[0], dict) else None
    return None if v is None else str(v)


def _content_hash(alert: dict) -> str:
    return hashlib.sha1(json.dumps(alert, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def alert_row(alert: dict, event: str, system_id: str, now: datetime) -> dict:
    times = [t for t in alert.get("times") or [] if isinstance(t, dict)]
    starts = [s for s in (_ts(t.get("start")) for t in times) if s]
    ends = [e for e in (_ts(t.get("end")) for t in times) if e]
    return {
        "alert_id": str(alert.get("alert_id")),
        "event": event,
        "type": alert.get("type"),
        "summary": _text(alert.get("summary")),
        "description": _text(alert.get("description")),
        "url": _text(alert.get("url")),
        "station_ids": [str(x) for x in alert.get("station_ids") or []],
        "region_ids": [str(x) for x in alert.get("region_ids") or []],
        "start_time": min(starts) if starts else None,
        "end_time": max(ends) if ends and len(ends) == len(times) else None,
        "alert_last_updated": _ts(alert.get("last_updated")),
        "content_hash": _content_hash(alert),
        "raw_json": json.dumps(alert, separators=(",", ":")),
        "system_id": system_id,
        "observed_at": now,
        "event_date": now.date(),
    }


def diff(previous: dict[str, dict], alerts: list[dict], system_id: str, now: datetime) -> list[dict]:
    """Rows to write, given the last known state {alert_id: row} and the alerts in the feed now."""
    current = {str(a.get("alert_id")): a for a in alerts if isinstance(a, dict) and a.get("alert_id") is not None}
    rows = []
    for aid, alert in current.items():
        old = previous.get(aid)
        if old is None:
            rows.append(alert_row(alert, "appeared", system_id, now))
        elif old["content_hash"] != _content_hash(alert):
            rows.append(alert_row(alert, "changed", system_id, now))
    for aid, old in previous.items():
        if aid not in current:
            ended = json.loads(old["raw_json"])
            rows.append(alert_row(ended, "ended", system_id, now))
    return rows


def load_active(table_uri: str) -> dict[str, dict]:
    """Active alerts according to the table (latest event per alert, not ended)."""
    from bikeflow.ingest.deltautil import open_table

    dt = open_table(table_uri)
    if dt is None:
        return {}
    tbl = dt.to_pyarrow_table(columns=["alert_id", "event", "content_hash", "raw_json", "observed_at"])
    latest: dict[str, dict] = {}
    for r in sorted(tbl.to_pylist(), key=lambda r: r["observed_at"]):
        latest[r["alert_id"]] = r
    return {aid: r for aid, r in latest.items() if r["event"] != "ended"}


class AlertsPoller:
    """Call ``maybe_poll()`` often; it fetches and writes at most every ``every_minutes``."""

    def __init__(self, client, table_uri: str, system_id: str, every_minutes: float = 5.0):
        self.client = client
        self.table_uri = table_uri
        self.system_id = system_id
        self.every_s = every_minutes * 60
        self._next = 0.0
        self._state: dict[str, dict] | None = None
        self.disabled = False

    def maybe_poll(self, now_monotonic: float | None = None) -> int:
        """Returns the number of events written (0 if not due, unchanged, or failed)."""
        mono = time.monotonic() if now_monotonic is None else now_monotonic
        if self.disabled or self.every_s <= 0 or mono < self._next:
            return 0
        self._next = mono + self.every_s
        try:
            return self.poll(datetime.now(timezone.utc))
        except KeyError:
            log.warning("This GBFS system publishes no system_alerts feed; alerts polling disabled")
            self.disabled = True
        except Exception:  # noqa: BLE001 - alerts must never break the live producer
            log.exception("Polling system_alerts failed; retrying next interval")
        return 0

    def poll(self, now: datetime) -> int:
        from deltalake import write_deltalake

        if self._state is None:
            self._state = load_active(self.table_uri)
        doc = self.client.fetch("system_alerts")
        alerts = (doc.get("data") or {}).get("alerts") or []
        rows = diff(self._state, alerts, self.system_id, now)
        if rows:
            write_deltalake(
                self.table_uri,
                pa.Table.from_pylist(rows, schema=ALERTS_SCHEMA),
                mode="append",
                partition_by=["event_date"],
            )
            for r in rows:
                if r["event"] == "ended":
                    self._state.pop(r["alert_id"], None)
                else:
                    self._state[r["alert_id"]] = r
            log.info(
                "System alerts: %s (%d active)",
                ", ".join(f"{r['event']} {r['alert_id']}" for r in rows),
                len(self._state),
            )
        return len(rows)
