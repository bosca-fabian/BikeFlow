import json

from bikeflow.ingest.gbfs import parse_discovery, to_epoch_seconds
from bikeflow.ingest.messages import ChangeDetector, build_message, encode, normalize_station, snapshot_from_feed
from bikeflow.schemas import MESSAGE_FIELD_NAMES


def test_normalize_handles_int_flags_and_millisecond_timestamps(make_station):
    st = make_station(1, renting=0)
    st["last_reported"] = 1_762_070_000_123  # milliseconds
    n = normalize_station(st)
    assert n["is_renting"] is False and n["is_installed"] is True
    assert n["last_reported"] == 1_762_070_000
    assert n["legacy_id"] == "1001"
    assert normalize_station({**st, "last_reported": 86400})["last_reported"] is None  # placeholder time


def test_normalize_derives_ebikes_from_vehicle_types():
    n = normalize_station(
        {
            "station_id": "x",
            "num_bikes_available": 5,
            "num_docks_available": 3,
            "is_installed": True,
            "is_renting": True,
            "is_returning": True,
            "vehicle_types_available": [
                {"vehicle_type_id": "cb:vehicle:ebike", "count": 4},
                {"vehicle_type_id": "cb:vehicle:bike", "count": 1},
            ],
        }
    )
    assert n["num_ebikes_available"] == 4
    assert normalize_station({"num_bikes_available": 1}) is None  # no station_id


def test_change_detector_initial_delta_heartbeat(make_feed, make_station):
    det = ChangeDetector(heartbeat_seconds=600)
    s = [make_station(i) for i in range(3)]
    first = det.select(snapshot_from_feed(make_feed(1000, s)))
    assert [r for _, r in first] == ["initial"] * 3

    s2 = [make_station(0, bikes=4), make_station(1), make_station(2)]
    second = det.select(snapshot_from_feed(make_feed(1060, s2)))
    assert [(x["station_id"][-1], r) for x, r in second] == [("0", "changed")]

    # last_reported alone changing is not a state change
    s3 = [dict(x, last_reported=x["last_reported"] + 60) for x in s2]
    assert det.select(snapshot_from_feed(make_feed(1120, s3))) == []

    # after 10 minutes every station is re-sent
    hb = det.select(snapshot_from_feed(make_feed(1600, s3)))
    assert sorted(r for _, r in hb) == ["heartbeat"] * 3


def test_build_message_contract_and_time_shift(make_station):
    st = normalize_station(make_station(7))
    m = build_message(
        st, feed_last_updated=1000, reason="changed", source="live", system_id="citibike_nyc", ingested_at=1001
    )
    assert list(m) == MESSAGE_FIELD_NAMES
    assert m["event_time"] == "1970-01-01T00:16:40Z" and m["original_event_time"] is None
    shifted = build_message(
        st,
        feed_last_updated=1000,
        reason="changed",
        source="replay",
        system_id="c",
        ingested_at=0,
        time_shift_seconds=500,
    )
    assert shifted["feed_last_updated"] == 1500
    assert shifted["original_event_time"] == "1970-01-01T00:16:40Z"
    assert json.loads(encode(m)) == m


def test_discovery_v2_and_v3():
    v2 = {"data": {"en": {"feeds": [{"name": "station_status", "url": "https://x/ss.json"}]}}}
    v3 = {"data": {"feeds": [{"name": "station_status", "url": "https://y/ss.json"}]}}
    assert parse_discovery(v2) == {"station_status": "https://x/ss.json"}
    assert parse_discovery(v3) == {"station_status": "https://y/ss.json"}
    assert parse_discovery(v2, language="fr") == {"station_status": "https://x/ss.json"}


def test_epoch_parsing():
    assert to_epoch_seconds(1_700_000_000) == 1_700_000_000
    assert to_epoch_seconds(1_700_000_000_000) == 1_700_000_000
    assert to_epoch_seconds("2023-11-14T22:13:20Z") == 1_700_000_000
    assert to_epoch_seconds(None) is None


def test_new_york_local_fields_match_batch_definitions(make_station):
    from datetime import datetime, timezone

    st = normalize_station(make_station(1))
    # Wednesday 4 Nov 2026, 13:17:05 UTC = 08:17:05 in New York (EST, UTC-5)
    ts = int(datetime(2026, 11, 4, 13, 17, 5, tzinfo=timezone.utc).timestamp())
    m = build_message(st, feed_last_updated=ts, reason="changed", source="live", system_id="c", ingested_at=ts)
    assert (m["local_date"], m["local_hour"], m["weekday_num"], m["is_weekend"]) == ("2026-11-04", 8, 3, False)
    assert m["hour_ts"] == "2026-11-04T13:00:00Z"  # 08:00 New York as an instant
    # Sunday 1 Nov 2026 01:30 EST happens after the clocks went back (second 01:xx hour)
    ts2 = int(datetime(2026, 11, 1, 6, 30, tzinfo=timezone.utc).timestamp())
    m2 = build_message(st, feed_last_updated=ts2, reason="changed", source="live", system_id="c", ingested_at=ts2)
    assert (m2["local_hour"], m2["weekday_num"], m2["is_weekend"], m2["hour_ts"]) == (
        1,
        7,
        True,
        "2026-11-01T06:00:00Z",
    )
