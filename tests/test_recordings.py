import gzip
from datetime import datetime, timezone

from bikeflow.ingest.recordings import Recorder, iter_snapshots, recording_file, summarize

T0 = int(datetime(2026, 11, 4, 12, 58, tzinfo=timezone.utc).timestamp())


def utc(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def test_roundtrip_across_hours_and_window(tmp_path, make_feed, make_station):
    rec = Recorder(tmp_path)
    for k in range(5):  # 12:58 .. 13:02 -> two hour files
        rec.write(make_feed(T0 + 60 * k, [make_station(k)]), T0 + 60 * k, T0 + 60 * k + 2)
    assert recording_file(tmp_path, T0).name == "hour=12.jsonl.gz"
    assert recording_file(tmp_path, T0 + 180).name == "hour=13.jsonl.gz"

    all_ts = [ts for ts, _ in iter_snapshots(tmp_path, utc(T0), utc(T0 + 3600))]
    assert all_ts == [T0 + 60 * k for k in range(5)]
    window = [ts for ts, _ in iter_snapshots(tmp_path, utc(T0 + 60), utc(T0 + 180))]
    assert window == [T0 + 60, T0 + 120]


def test_damaged_tail_is_tolerated(tmp_path, make_feed, make_station):
    rec = Recorder(tmp_path)
    rec.write(make_feed(T0, [make_station(1)]), T0, T0)
    path = rec.write(make_feed(T0 + 60, [make_station(1)]), T0 + 60, T0 + 60)
    with open(path, "ab") as fh:  # simulate a crash in the middle of a write
        fh.write(gzip.compress(b'{"feed_last_updated": 1}\n')[:15])
    got = [ts for ts, _ in iter_snapshots(tmp_path, utc(T0), utc(T0 + 3600))]
    assert got == [T0, T0 + 60]


def test_summarize(tmp_path, make_feed, make_station):
    rec = Recorder(tmp_path)
    for k in range(3):
        rec.write(make_feed(T0 + 60 * k, [make_station(1)]), T0 + 60 * k, T0)
    (day,) = summarize(tmp_path)
    assert day.snapshots == 3 and day.hours == [12, 13] and day.first_ts == T0
