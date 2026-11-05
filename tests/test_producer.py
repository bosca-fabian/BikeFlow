from datetime import datetime, timezone

from bikeflow.ingest.producer import run_live
from bikeflow.ingest.recordings import Recorder, iter_snapshots


class FakePublisher:
    def __init__(self, stop_after=None, stop=None):
        self.sent = []
        self.stop_after, self.stop = stop_after, stop

    def publish(self, topic, messages):
        self.sent.append((topic, messages))
        if self.stop_after and len(self.sent) >= self.stop_after:
            self.stop.set()
        return len(messages)


class FakeClient:
    def __init__(self, feeds):
        self.feeds = list(feeds)

    def fetch_station_status(self):
        item = self.feeds.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def test_live_sends_changes_skips_stale_feed_and_survives_errors(tmp_path, make_feed, make_station):
    s = [make_station(i) for i in range(4)]
    feeds = [
        make_feed(1000, s),
        make_feed(1000, s),  # feed not refreshed -> nothing sent, not recorded
        ConnectionError("network down"),  # poll fails -> keep going
        make_feed(1060, [make_station(0, bikes=0), *s[1:]]),
    ]
    pub = FakePublisher()
    stats = run_live(
        client=FakeClient(feeds),
        publisher=pub,
        topic="station_status",
        system_id="citibike_nyc",
        poll_seconds=0,
        heartbeat_seconds=900,
        recorder=Recorder(tmp_path),
        max_polls=4,
        clock=lambda: 2000.0,
    )
    assert stats == {"polls": 4, "snapshots": 2, "messages": 5, "errors": 1, "unchanged_feed": 1}
    assert [len(m) for _, m in pub.sent] == [4, 1]
    changed = pub.sent[1][1][0]
    assert changed["reason"] == "changed" and changed["num_bikes_available"] == 0 and changed["source"] == "live"
    recorded = list(
        iter_snapshots(tmp_path, datetime(1970, 1, 1, tzinfo=timezone.utc), datetime(1970, 1, 2, tzinfo=timezone.utc))
    )
    assert [ts for ts, _ in recorded] == [1000, 1060]
