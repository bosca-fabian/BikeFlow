"""GBFS ``system_regions`` (region_id -> name) -> a daily snapshot table in the lake.

Polled by the producer once a day. Stations carry a ``region_id``; this table
turns it into a name, which is how New York City stations are told apart from
Jersey City / Hoboken ones (the NYC trip files leave Jersey City out).
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone

import pyarrow as pa

from bikeflow.ingest.gbfs import to_epoch_seconds

log = logging.getLogger(__name__)


def _text(v):
    if v is None:
        return None
    if isinstance(v, list):  # GBFS v3 localized strings
        for item in v:
            if isinstance(item, dict) and item.get("language", "").startswith("en"):
                return item.get("text")
        return v[0].get("text") if v and isinstance(v[0], dict) else None
    return str(v)


REGIONS_SCHEMA = pa.schema(
    [
        ("region_id", pa.string()),
        ("name", pa.string()),
        ("raw_json", pa.string()),
        ("system_id", pa.string()),
        ("feed_last_updated", pa.timestamp("us", tz="UTC")),
        ("snapshot_time", pa.timestamp("us", tz="UTC")),
        ("snapshot_date", pa.date32()),  # partition column
    ]
)


def regions_to_table(doc: dict, system_id: str, now: datetime) -> pa.Table:
    feed_ts = to_epoch_seconds(doc.get("last_updated"))
    feed_dt = datetime.fromtimestamp(feed_ts, tz=timezone.utc) if feed_ts else None
    rows = [
        {
            "region_id": str(r["region_id"]),
            "name": _text(r.get("name")),
            "raw_json": json.dumps(r, separators=(",", ":")),
            "system_id": system_id,
            "feed_last_updated": feed_dt,
            "snapshot_time": now,
            "snapshot_date": now.date(),
        }
        for r in (doc.get("data") or {}).get("regions") or []
        if isinstance(r, dict) and r.get("region_id") not in (None, "")
    ]
    return pa.Table.from_pylist(rows, schema=REGIONS_SCHEMA)


def snapshot_regions(client, table_uri: str, system_id: str, now: datetime | None = None) -> int:
    from deltalake import write_deltalake

    now = now or datetime.now(timezone.utc)
    table = regions_to_table(client.fetch("system_regions"), system_id, now)
    if table.num_rows == 0:
        raise RuntimeError("system_regions returned no regions")
    write_deltalake(
        table_uri,
        table,
        mode="overwrite",
        predicate=f"snapshot_date = '{now.date().isoformat()}'",
        partition_by=["snapshot_date"],
    )
    log.info("System regions: %d regions written for %s", table.num_rows, now.date())
    return table.num_rows


class RegionsPoller:
    """Call ``maybe_poll()`` after every live poll; it snapshots regions at most every ``every_hours``."""

    def __init__(self, client, table_uri: str, system_id: str, every_hours: float = 24.0):
        self.client = client
        self.table_uri = table_uri
        self.system_id = system_id
        self.every_s = every_hours * 3600
        self._next = 0.0
        self.disabled = False

    def maybe_poll(self, now_monotonic: float | None = None) -> int:
        mono = time.monotonic() if now_monotonic is None else now_monotonic
        if self.disabled or mono < self._next:
            return 0
        try:
            n = snapshot_regions(self.client, self.table_uri, self.system_id)
            self._next = mono + self.every_s
            return n
        except KeyError:
            log.warning("This GBFS system publishes no system_regions feed; regions snapshots disabled")
            self.disabled = True
        except Exception:  # noqa: BLE001 - must never break the live producer
            log.exception("Snapshotting system_regions failed; retrying in 15 minutes")
            self._next = mono + 15 * 60
        return 0
