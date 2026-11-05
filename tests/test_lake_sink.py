"""Kafka -> Delta sink with a fake consumer and a real Delta table."""

from confluent_kafka import TopicPartition
from deltalake import DeltaTable

from bikeflow.ingest.lake_sink import DeltaWriter, LakeSink, message_to_row
from bikeflow.ingest.messages import build_message, encode, normalize_station
from bikeflow.lake import STATION_STATUS_RAW, table_path

TOPIC = "station_status"


class FakeMsg:
    def __init__(self, partition, offset, value):
        self._p, self._o, self._v = partition, offset, value

    def topic(self):
        return TOPIC

    def partition(self):
        return self._p

    def offset(self):
        return self._o

    def value(self):
        return self._v

    def key(self):
        return None

    def timestamp(self):
        return (1, 1_762_070_000_000)

    def error(self):
        return None


class FakeConsumer:
    """Two partitions; honours offsets set in on_assign, ignores Kafka commits (worst case)."""

    def __init__(self, log, batch=1000):
        self.log = log  # {partition: [bytes, ...]}
        self.batch = batch
        self.pos = {}
        self.commits = []

    def subscribe(self, topics, on_assign=None, on_revoke=None):
        parts = [TopicPartition(TOPIC, p) for p in sorted(self.log)]
        on_assign(self, parts)

    def assign(self, partitions):
        for tp in partitions:
            self.pos[tp.partition] = tp.offset if tp.offset >= 0 else 0

    def consume(self, num_messages=1000, timeout=1.0):
        out = []
        for p, msgs in self.log.items():
            while self.pos[p] < len(msgs) and len(out) < min(num_messages, self.batch):
                out.append(FakeMsg(p, self.pos[p], msgs[self.pos[p]]))
                self.pos[p] += 1
        return out

    def commit(self, offsets, asynchronous=False):
        self.commits.append(offsets)

    def close(self):
        pass


def make_value(make_station, i, ts):
    st = normalize_station(make_station(i, bikes=i))
    return encode(
        build_message(
            st, feed_last_updated=ts, reason="changed", source="live", system_id="citibike_nyc", ingested_at=ts
        )
    )


def run_sink(log, flush_rows=4, batch=1000):
    writer = DeltaWriter(table_path(STATION_STATUS_RAW), "test-sink")
    sink = LakeSink(
        FakeConsumer(log, batch), writer, [TOPIC], flush_rows=flush_rows, flush_seconds=999, compact_every_hours=0
    )
    return sink.run(max_idle_polls=1)


def test_sink_writes_partitioned_table_exactly_once(make_station):
    day1, day2 = 1_762_070_000, 1_762_070_000 + 86_400
    log = {
        0: [make_value(make_station, i, day1) for i in range(5)],
        1: [make_value(make_station, i, day2) for i in range(3)] + [b"not json"],
    }
    stats = run_sink(log)
    assert stats["rows_written"] == 9

    uri = table_path(STATION_STATUS_RAW)
    dt = DeltaTable(uri)
    tbl = dt.to_pyarrow_table()
    assert tbl.num_rows == 9
    assert sorted(set(tbl.column("event_date").to_pylist()))[0].isoformat() == "2025-11-02"
    assert dt.metadata().partition_columns == ["event_date"]
    bad = [r for r in tbl.to_pylist() if r["raw_value"] == "not json"]
    assert bad and bad[0]["station_id"] is None
    assert dt.transaction_version("test-sink:station_status:0") == 4
    assert dt.transaction_version("test-sink:station_status:1") == 3

    # Restart with Kafka offsets "lost": nothing is duplicated, only new data is added.
    log[0].append(make_value(make_station, 42, day1))
    stats2 = run_sink(log)
    assert stats2["rows_written"] == 1
    assert DeltaTable(uri).to_pyarrow_table().num_rows == 10


def test_compaction_keeps_rows(make_station):
    log = {0: [make_value(make_station, i, 1_762_070_000) for i in range(12)]}
    run_sink(log, flush_rows=2, batch=2)  # many small files
    writer = DeltaWriter(table_path(STATION_STATUS_RAW), "test-sink")
    before = len(DeltaTable(writer.table_uri).file_uris())
    writer.compact_and_vacuum(retention_hours=0)
    dt = DeltaTable(writer.table_uri)
    assert len(dt.file_uris()) < before
    assert dt.to_pyarrow_table().num_rows == 12
    # offsets survive compaction
    assert dt.transaction_version("test-sink:station_status:0") == 11


def test_message_to_row_types(make_station):
    row = message_to_row(make_value(make_station, 3, 1_762_070_000), TOPIC, 0, 7, 1_762_070_001_000)
    assert row["event_time"].isoformat() == "2025-11-02T07:53:20+00:00"
    assert row["num_bikes_available"] == 3 and row["kafka_offset"] == 7
