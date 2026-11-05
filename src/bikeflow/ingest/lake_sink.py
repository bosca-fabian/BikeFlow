"""Kafka -> Delta Lake sink for the station_status_raw table.

Reads every message on the station_status topic and appends it to the Delta
table ``<BIKEFLOW_DATA>/lake/station_status_raw`` (partitioned by event_date).

Exactly-once into the lake: each Delta commit also stores, per Kafka
partition, the last offset it contains (Delta "app transactions", the same
mechanism Spark Structured Streaming uses). On restart the sink resumes from
those offsets, so a crash between writing and committing never duplicates rows.

Written with the `deltalake` (delta-rs) library, so it needs no JVM; the
table is a standard Delta table that Spark reads as usual.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime, timezone

from bikeflow.schemas import MESSAGE_FIELD_NAMES, arrow_raw_schema

log = logging.getLogger(__name__)

TIMESTAMP_FIELDS = ("event_time", "hour_ts", "ingested_at", "original_event_time")
PARTITION_COL = "event_date"


def _parse_ts(value) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def _parse_date(value):
    try:
        return datetime.strptime(str(value), "%Y-%m-%d").date() if value else None
    except ValueError:
        return None


def message_to_row(value: bytes | None, topic: str, partition: int, offset: int, kafka_ts_ms: int | None) -> dict:
    """Convert one Kafka record to a raw-table row (never raises)."""
    text = value.decode("utf-8", errors="replace") if value is not None else ""
    try:
        msg = json.loads(text)
        if not isinstance(msg, dict):
            raise ValueError("not an object")
    except ValueError:
        log.warning("Unparseable message at %s[%d]@%d, stored raw only", topic, partition, offset)
        msg = {}
    row = {name: msg.get(name) for name in MESSAGE_FIELD_NAMES}
    for f in TIMESTAMP_FIELDS:
        row[f] = _parse_ts(row[f])
    row["local_date"] = _parse_date(row["local_date"])
    kafka_ts = datetime.fromtimestamp(kafka_ts_ms / 1000, tz=timezone.utc) if kafka_ts_ms and kafka_ts_ms > 0 else None
    event = row["event_time"] or kafka_ts or datetime.now(timezone.utc)
    row.update(
        event_date=event.date(),
        kafka_topic=topic,
        kafka_partition=partition,
        kafka_offset=offset,
        kafka_timestamp=kafka_ts,
        raw_value=text,
    )
    return row


def app_id(group_id: str, topic: str, partition: int) -> str:
    return f"{group_id}:{topic}:{partition}"


class DeltaWriter:
    """Appends batches to the raw Delta table and remembers Kafka offsets inside it."""

    def __init__(self, table_uri: str, group_id: str, storage_options: dict | None = None):
        self.table_uri = table_uri
        self.group_id = group_id
        self.storage_options = storage_options
        self.schema = arrow_raw_schema()

    def _table(self):
        from deltalake import DeltaTable

        if DeltaTable.is_deltatable(self.table_uri, storage_options=self.storage_options):
            return DeltaTable(self.table_uri, storage_options=self.storage_options)
        return None

    def stored_offset(self, topic: str, partition: int) -> int | None:
        """Last Kafka offset already in the table for this partition, or None."""
        dt = self._table()
        if dt is None:
            return None
        return dt.transaction_version(app_id(self.group_id, topic, partition))

    def write(self, rows: list[dict], offsets: dict[tuple[str, int], int]) -> None:
        import pyarrow as pa
        from deltalake import CommitProperties, Transaction, write_deltalake

        table = pa.Table.from_pylist(rows, schema=self.schema)
        txns = [Transaction(app_id(self.group_id, t, p), off) for (t, p), off in sorted(offsets.items())]
        write_deltalake(
            self.table_uri,
            table,
            mode="append",
            partition_by=[PARTITION_COL],
            storage_options=self.storage_options,
            commit_properties=CommitProperties(app_transactions=txns),
        )

    def compact_and_vacuum(self, retention_hours: int = 168) -> dict:
        dt = self._table()
        if dt is None:
            return {}
        metrics = dt.optimize.compact()
        removed = dt.vacuum(retention_hours=retention_hours, dry_run=False, enforce_retention_duration=False)
        log.info(
            "Compaction: %s files removed, %s added; vacuum deleted %d old files",
            metrics.get("numFilesRemoved"),
            metrics.get("numFilesAdded"),
            len(removed),
        )
        return {"compact": metrics, "vacuumed": len(removed)}


class LakeSink:
    def __init__(
        self,
        consumer,
        writer: DeltaWriter,
        topics: list[str],
        flush_rows: int = 50_000,
        flush_seconds: float = 120.0,
        compact_every_hours: float = 6.0,
        stop: threading.Event | None = None,
    ):
        self.consumer = consumer
        self.writer = writer
        self.topics = topics
        self.flush_rows = flush_rows
        self.flush_seconds = flush_seconds
        self.compact_every_s = compact_every_hours * 3600
        self.stop = stop or threading.Event()
        self._rows: list[dict] = []
        self._offsets: dict[tuple[str, int], int] = {}
        self._last_flush = time.monotonic()
        self._last_compact = time.monotonic()
        self.stats = {"messages": 0, "flushes": 0, "rows_written": 0, "skipped_already_in_lake": 0}

    # Called by the consumer when partitions are assigned: resume from the lake's offsets.
    def on_assign(self, consumer, partitions):
        for tp in partitions:
            stored = self.writer.stored_offset(tp.topic, tp.partition)
            if stored is not None:
                tp.offset = stored + 1
                self._offsets[(tp.topic, tp.partition)] = stored
                log.info("Resuming %s[%d] at offset %d (from Delta log)", tp.topic, tp.partition, tp.offset)
        consumer.assign(partitions)

    def on_revoke(self, consumer, partitions):
        self.flush()

    def _maybe_skip(self, topic: str, partition: int, offset: int) -> bool:
        # Defensive: never write an offset the lake already holds.
        known = self._offsets.get((topic, partition))
        if known is not None and offset <= known:
            self.stats["skipped_already_in_lake"] += 1
            return True
        return False

    def handle(self, msg) -> None:
        topic, partition, offset = msg.topic(), msg.partition(), msg.offset()
        if self._maybe_skip(topic, partition, offset):
            return
        ts_type, ts_ms = msg.timestamp()
        self._rows.append(message_to_row(msg.value(), topic, partition, offset, ts_ms if ts_type else None))
        self._offsets[(topic, partition)] = offset
        self.stats["messages"] += 1

    def flush(self) -> None:
        if not self._rows:
            self._last_flush = time.monotonic()
            return
        from confluent_kafka import TopicPartition

        rows, offsets = self._rows, dict(self._offsets)
        self.writer.write(rows, offsets)
        self.stats["flushes"] += 1
        self.stats["rows_written"] += len(rows)
        log.info("Wrote %d rows to %s (offsets %s)", len(rows), self.writer.table_uri, offsets)
        self._rows = []
        self._last_flush = time.monotonic()
        try:  # informational only: the Delta log is the source of truth
            self.consumer.commit(
                offsets=[TopicPartition(t, p, o + 1) for (t, p), o in offsets.items()], asynchronous=False
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("Kafka offset commit failed (harmless, lake has the offsets): %s", exc)

    def run(self, max_idle_polls: int | None = None) -> dict:
        self.consumer.subscribe(self.topics, on_assign=self.on_assign, on_revoke=self.on_revoke)
        idle = 0
        try:
            while not self.stop.is_set():
                batch = self.consumer.consume(num_messages=1000, timeout=1.0)
                got = 0
                for msg in batch:
                    if msg.error():
                        log.warning("Kafka error: %s", msg.error())
                        continue
                    self.handle(msg)
                    got += 1
                idle = 0 if got else idle + 1
                if len(self._rows) >= self.flush_rows or time.monotonic() - self._last_flush >= self.flush_seconds:
                    self.flush()
                if self.compact_every_s > 0 and time.monotonic() - self._last_compact >= self.compact_every_s:
                    self.flush()
                    self.writer.compact_and_vacuum()
                    self._last_compact = time.monotonic()
                if max_idle_polls is not None and idle >= max_idle_polls:
                    break
        finally:
            self.flush()
            self.consumer.close()
        return self.stats


def consumer_config(bootstrap: str, group_id: str) -> dict:
    return {
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "partition.assignment.strategy": "range",
    }
