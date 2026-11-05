# Ingestion data contract

What the ingestion layer produces, where it lives, and what downstream code can rely on.
The source of truth in code is `src/bikeflow/schemas.py`; change both together.

**Owner:** ingestion lead (Fabian). Ask before changing anything here, because the streaming job,
batch jobs and dashboard all depend on it.

## 1. Kafka topics

| Topic | Written by | Content | Partitions | Retention |
|---|---|---|---|---|
| `station_status` | `producer` (live feed) | One message per station that changed in a snapshot, plus every station on start-up and every 15 min (heartbeat) | 3 | 72 h |
| `station_status_replay` | `replay` | The same messages rebuilt from recordings (`source = "replay"`) | 3 | 72 h |

- **Key:** `station_id` (UTF-8). All updates of one station are in one partition, in order.
- **Value:** compact JSON, UTF-8, one object per message (fields below).
- **Inside Docker:** `kafka:19092`. **From your laptop:** `localhost:9092`.
- **Delivery:** the producer is idempotent (`acks=all`), so it creates no duplicates when it retries.
  A producer *restart* re-sends every station (`reason = "initial"`). Consumers should treat each message as
  "this is the current state of station X at `event_time`", which is naturally idempotent.
- **Kafka timestamps** are send times, not event times. Always use `event_time`.

### Message fields (`schema_version = 1`)

| Field | Type | Nullable | Meaning |
|---|---|---|---|
| `schema_version` | long | no | Message format version (currently 1) |
| `system_id` | string | no | Bike-share system, e.g. `citibike_nyc` |
| `source` | string | no | `live` (real feed) or `replay` (recorded data) |
| `reason` | string | no | `initial`, `changed` or `heartbeat` |
| `station_id` | string | no | GBFS station_id (UUID for Citi Bike); also the Kafka key |
| `legacy_id` | string | yes | GBFS legacy_id if the feed provides it |
| `num_bikes_available` | long | no | Bikes available to rent (classic + e-bikes) |
| `num_ebikes_available` | long | yes | E-bikes available to rent |
| `num_bikes_disabled` | long | yes | Broken/reserved bikes docked at the station |
| `num_docks_available` | long | no | Free docks to return a bike |
| `num_docks_disabled` | long | yes | Docks out of service |
| `is_installed` | bool | no | Station is installed |
| `is_renting` | bool | no | Station allows renting |
| `is_returning` | bool | no | Station accepts returns |
| `last_reported` | long | yes | Epoch seconds when the station last reported (from the feed); null when the feed gives a placeholder (decommissioned stations report 1970-01-02) |
| `feed_last_updated` | long | no | Epoch seconds of the feed snapshot (event time) |
| `event_time` | timestamp | no | `feed_last_updated` as an ISO-8601 UTC string (`2026-11-04T12:00:05Z`); **use this for windows and watermarks** |
| `hour_ts` | timestamp | no | Start of the hour containing `event_time` (ISO-8601 UTC string). Same instant as the batch tables' `weather_hourly.hour_ts` / `trips_clean.start_hour_ts` for that New York hour |
| `local_date` | date | no | New York date of `event_time` (`YYYY-MM-DD`) |
| `local_hour` | long | no | New York hour, 0–23 (= `trips_clean.start_hour`) |
| `weekday_num` | long | no | New York weekday, Monday = 1 … Sunday = 7 (= `trips_clean.weekday_num`) |
| `is_weekend` | bool | no | `weekday_num >= 6` (= `trips_clean.is_weekend`) |
| `ingested_at` | timestamp | no | When the producer built the message (UTC, ISO-8601) |
| `original_event_time` | timestamp | yes | Time-shifted replay only: the event time as recorded |

Example:

```json
{"schema_version":1,"system_id":"citibike_nyc","source":"live","reason":"changed",
 "station_id":"66db237e-0aca-11e7-82f6-3863bb44ef7c","legacy_id":"3255",
 "num_bikes_available":0,"num_ebikes_available":0,"num_bikes_disabled":1,
 "num_docks_available":30,"num_docks_disabled":0,"is_installed":true,"is_renting":true,
 "is_returning":true,"last_reported":1793793541,"feed_last_updated":1793793600,
 "event_time":"2026-11-04T12:00:00Z","hour_ts":"2026-11-04T12:00:00Z","local_date":"2026-11-04",
 "local_hour":7,"weekday_num":3,"is_weekend":false,
 "ingested_at":"2026-11-04T12:00:02Z","original_event_time":null}
```

### Semantics worth knowing

- **Only changes are sent.** A station is re-sent when any of these change: bikes, e-bikes, disabled bikes,
  docks, disabled docks, installed/renting/returning. A change in `last_reported` alone does not count.
  To know a station's state at time *t*, take its latest message with `event_time <= t`.
  Heartbeats guarantee that state is at most 15 min old even for quiet stations.
- **For fill levels, use the message, not the published capacity.** Station size =
  `num_bikes_available + num_bikes_disabled + num_docks_available + num_docks_disabled`.
  `station_information.capacity` is often off: in a real snapshot (2 Nov 2026) that sum was lower than
  `capacity` for 687 of 2,466 active stations, and 37 stations have capacity 0.
- **Empty / full:** empty = `num_bikes_available == 0`; full = `num_docks_available == 0`.
  Disabled bikes are common (≈9,900 across 2,265 stations in that snapshot): a station can hold bikes
  and still have none to rent, and a dock holding a broken bike cannot take a return.
- **Stations to leave out of alerts:** `is_installed = false` (≈50, decommissioned), installed but
  `is_renting = false` / `is_returning = false` (≈90, temporarily closed; `system_alerts_raw` may say why),
  and stations whose `last_reported` is more than about an hour old (stale; ≈40, of which ≈20 over a day).
- **NYC vs. Jersey City / Hoboken:** the live feed covers all three (regions in `system_regions`;
  most stations are region `71`), while the NYC trip files do not include Jersey City. Filter by region
  when comparing live data with trip history.
- **Joining with the batch tables** (they live in the same `data/lake/` folder):
  - station: `station_id` = `station_id_map.station_uuid` (the trip files use short codes; the
    batch path maps them to these UUIDs);
  - hour: `hour_ts` = `weather_hourly.hour_ts` = `trips_clean.start_hour_ts`;
  - demand profiles by weekday and hour: `weekday_num` and `local_hour` use the same definitions
    as `trips_clean` (New York time, Monday = 1).
  Compare instants (`hour_ts`, `event_time`), not formatted strings: the batch scripts run Spark in
  New York time, `get_spark()` in UTC; instants are equal either way.
- **Feed not refreshed:** if a poll returns the same `last_updated` as the previous one, nothing is sent.

### Replay topic

- Same schema, `source = "replay"`. Never mixed into the live topic.
- `--time-shift none` keeps recorded event times. `weeks` (the default for the looping demo replay) moves
  events by whole weeks, keeping the **weekday and hour** (so hour-of-week demand profiles still match),
  and adds one more week per loop so event time keeps increasing. `now` makes the replay start at the current time.
- Point the streaming job at it with `read_station_status_stream(spark, replay=True)`.

## 2. Lake table `station_status_raw`

Location: `table_path(STATION_STATUS_RAW)` → `data/lake/station_status_raw` (Delta Lake, written by the `lake-sink` service).

- One row per Kafka message from `station_status` (live only), every field above with real
  timestamp types, plus:

| Column | Type | Meaning |
|---|---|---|
| `event_date` | date | UTC date of `event_time` (**partition column**) |
| `kafka_topic` | string | Topic the message was read from |
| `kafka_partition` | long | Kafka partition |
| `kafka_offset` | long | Kafka offset; (`kafka_topic`, `kafka_partition`, `kafka_offset`) is unique |
| `kafka_timestamp` | timestamp | Kafka timestamp (send time) |
| `raw_value` | string | The original message text (kept even if parsing fails) |

- **Exactly once:** every Delta commit records the last Kafka offset per partition (Delta app transactions,
  app id `bikeflow-lake-sink:<topic>:<partition>`). A restarted sink resumes from there, so no message is
  written twice or skipped.
- **Freshness:** flushed every 120 s or 50,000 rows. Small files are compacted every 6 h and files no longer
  referenced for 7 days are vacuumed. Time travel works within those 7 days.
- **Duplicated observations** (producer restarts) can be removed with
  `dropDuplicates(["system_id", "source", "station_id", "feed_last_updated"])`, which `read_raw_station_status()` does by default.
- **Single writer:** only `lake-sink` writes this table. Read it from anywhere; write your own tables elsewhere.

```python
from bikeflow.spark import get_spark
from bikeflow.streams import read_raw_station_status
spark = get_spark("explore")
df = read_raw_station_status(spark)          # live rows, de-duplicated
df.groupBy("event_date").count().show()
```

## 3. Other lake tables written by ingestion

The batch tables (`trips_raw`, `trips_clean`, `weather_hourly`, `station_snapshots`, `stations`,
`station_id_map`) are documented by the batch path (`README_batch.md`). Remember `capacity` there is
unreliable for fill levels (see section 1).

### `system_regions`: daily snapshots of GBFS `system_regions`

Location `data/lake/system_regions`, partitioned by `snapshot_date` (UTC), written once a day by the `producer`.

| Column | Type | Meaning |
|---|---|---|
| `region_id` | string | Matches `region_id` in the station tables |
| `name` | string | Region name as published by Citi Bike |
| `raw_json`, `system_id`, `feed_last_updated`, `snapshot_time`, `snapshot_date` | | Provenance |

### `system_alerts_raw`: service alerts as change events

Location `data/lake/system_alerts_raw`, written by the `producer`, which checks GBFS `system_alerts` every 5 minutes. A row is written only when
something happens to an alert, so the table stays small. Partitioned by `event_date` (UTC).

| Column | Type | Meaning |
|---|---|---|
| `alert_id` | string | The feed's alert ID |
| `event` | string | `appeared` (first seen), `changed` (content changed) or `ended` (gone from the feed) |
| `type` | string | `SYSTEM_CLOSURE`, `STATION_CLOSURE`, `STATION_MOVE` or `OTHER` |
| `summary`, `description`, `url` | string | Text of the alert |
| `station_ids`, `region_ids` | array of string | Affected stations / regions (station IDs = live-feed UUIDs) |
| `start_time`, `end_time` | timestamp | Earliest start and latest end of the alert's time windows (`end_time` null = open-ended) |
| `alert_last_updated` | timestamp | When the operator last edited the alert |
| `content_hash`, `raw_json`, `system_id`, `observed_at`, `event_date` | | Change detection and provenance |

**Currently active alerts** = for each `alert_id`, its latest row by `observed_at`, if that row is not `ended`.
Times are when *we noticed* a change, so with a 5-minute poll they are accurate to about 5 minutes.

## 4. Recordings (`data/raw/station_status/`)

Raw, unmodified GBFS `station_status` documents, used for replay and as a backup of the live feed.

```
data/raw/station_status/date=YYYY-MM-DD/hour=HH.jsonl.gz     (UTC)
  one line per snapshot: {"recorded_at": "...Z", "feed_last_updated": 1793793600, "feed": {...GBFS...}}
```

- Roughly 50–120 MB per day. Not in git: share whole `date=...` folders through the team drive.
- `data/sample_recordings/` holds a small **synthetic** sample (100 invented stations, Wednesday 07:00–09:00
  New York, `"_synthetic": true` in every snapshot) for development and tests. It is not real data.

## 5. Changing the contract

- Adding a nullable field is allowed: add it to `schemas.py`, mention it here, and keep `schema_version`.
- Renaming, removing or retyping a field: bump `schema_version`, tell the team, and update `streams.py`.
