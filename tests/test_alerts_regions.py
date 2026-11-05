"""system_alerts change events and system_regions snapshots."""

from datetime import datetime, timezone

from deltalake import DeltaTable

from bikeflow.ingest.alerts import AlertsPoller, diff, load_active
from bikeflow.ingest.regions import RegionsPoller, regions_to_table, snapshot_regions
from bikeflow.ingest.deltautil import partition_row_counts
from bikeflow.lake import SYSTEM_ALERTS_RAW, SYSTEM_REGIONS, table_path

T0 = datetime(2026, 11, 2, 12, 0, tzinfo=timezone.utc)


def alert(aid, summary="Station closed for maintenance", stations=("s1",), end=1_793_800_000):
    return {
        "alert_id": aid,
        "type": "STATION_CLOSURE",
        "times": [{"start": 1_793_600_000, "end": end}],
        "station_ids": list(stations),
        "summary": summary,
        "description": "Use nearby stations.",
        "last_updated": 1_793_600_000,
    }


class FeedSeq:
    """Returns the given system_alerts documents one after another."""

    def __init__(self, *alert_lists, regions=None):
        self.docs = [{"last_updated": 1_793_600_000, "data": {"alerts": a}} for a in alert_lists]
        self.regions = regions

    def fetch(self, name):
        if name == "system_regions":
            return self.regions
        assert name == "system_alerts"
        return self.docs.pop(0)


def test_diff_events():
    a1, a2 = alert("a1"), alert("a2", stations=("s2", "s3"))
    rows = diff({}, [a1, a2], "citibike_nyc", T0)
    assert [(r["alert_id"], r["event"]) for r in rows] == [("a1", "appeared"), ("a2", "appeared")]
    assert rows[1]["station_ids"] == ["s2", "s3"] and rows[0]["type"] == "STATION_CLOSURE"
    assert rows[0]["end_time"] == datetime.fromtimestamp(1_793_800_000, tz=timezone.utc)
    state = {r["alert_id"]: r for r in rows}

    assert diff(state, [a1, a2], "c", T0) == []  # nothing new -> nothing written
    changed = diff(state, [alert("a1", summary="Reopens Friday"), a2], "c", T0)
    assert [(r["alert_id"], r["event"]) for r in changed] == [("a1", "changed")]
    ended = diff(state, [a2], "c", T0)
    assert [(r["alert_id"], r["event"], r["summary"]) for r in ended] == [
        ("a1", "ended", "Station closed for maintenance")
    ]


def test_poller_writes_events_and_survives_restart():
    uri = table_path(SYSTEM_ALERTS_RAW)
    feed = FeedSeq([alert("a1")], [alert("a1"), alert("a2")], [alert("a2")], [alert("a2")])
    p = AlertsPoller(feed, uri, "citibike_nyc", every_minutes=5)
    assert p.maybe_poll(0) == 1  # a1 appeared
    assert p.maybe_poll(10) == 0  # not due yet: feed not even fetched
    assert p.maybe_poll(301) == 1  # a2 appeared
    assert p.maybe_poll(602) == 1  # a1 ended

    assert set(load_active(uri)) == {"a2"}
    restarted = AlertsPoller(feed, uri, "citibike_nyc", every_minutes=5)
    assert restarted.maybe_poll(0) == 0  # a2 already known: not reported again
    events = sorted((r["alert_id"], r["event"]) for r in DeltaTable(uri).to_pyarrow_table().to_pylist())
    assert events == [("a1", "appeared"), ("a1", "ended"), ("a2", "appeared")]


def test_poller_never_breaks_the_producer():
    class Broken:
        def fetch(self, name):
            raise ConnectionError("down")

    class NoFeed:
        def fetch(self, name):
            raise KeyError(name)

    assert AlertsPoller(Broken(), table_path(SYSTEM_ALERTS_RAW), "c").maybe_poll(0) == 0
    p = AlertsPoller(NoFeed(), table_path(SYSTEM_ALERTS_RAW), "c")
    assert p.maybe_poll(0) == 0 and p.disabled


def test_regions_snapshot():
    doc = {
        "last_updated": 1_793_616_256,
        "data": {
            "regions": [
                {"region_id": "71", "name": "NYC District"},
                {"region_id": "70", "name": [{"text": "JC District", "language": "en"}]},  # GBFS v3 style
            ]
        },
    }
    t = regions_to_table(doc, "citibike_nyc", T0)
    assert t.column("name").to_pylist() == ["NYC District", "JC District"]

    uri = table_path(SYSTEM_REGIONS)
    feed = FeedSeq(regions=doc)
    snapshot_regions(feed, uri, "citibike_nyc", T0)
    snapshot_regions(feed, uri, "citibike_nyc", T0)  # same day: replaced, not duplicated
    assert partition_row_counts(uri, "snapshot_date") == {"2026-11-02": 2}


def test_regions_poller_daily_and_failure_tolerant():
    doc = {"last_updated": 1, "data": {"regions": [{"region_id": "71", "name": "NYC District"}]}}
    calls = []

    class Feed:
        def fetch(self, name):
            calls.append(name)
            if len(calls) == 2:
                raise ConnectionError("down")
            return doc

    p = RegionsPoller(Feed(), table_path(SYSTEM_REGIONS), "c", every_hours=24)
    assert p.maybe_poll(0) == 1
    assert p.maybe_poll(3600) == 0 and len(calls) == 1  # not due until tomorrow
    assert p.maybe_poll(86_400) == 0  # fails -> retried 15 min later, producer unaffected
    assert p.maybe_poll(86_400 + 900) == 1
