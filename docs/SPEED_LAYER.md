# Speed layer data model (Redis)

What the streaming job writes to Redis, what the dashboard can rely on, and how
alerts and 30-minute warnings behave. The source of truth in code is
`src/bikeflow/streaming/redis_keys.py`; change both together.

**Owner:** streaming lead (Teodor). Everything here is read by the dashboard, so
agree on changes with the analytics and dashboard lead before renaming a key.

The job that writes all of this is `bikeflow-stream run`
(`src/bikeflow/streaming/job.py`). It reads the Kafka topic `station_status`
(written by the producer, see `docs/DATA_CONTRACT.md`).

## 1. One batch, one pipeline

Every micro-batch (default every 15 s) does:

1. Spark parses the Kafka JSON (`bikeflow.streams.read_station_status_stream`),
   applies a 2-minute watermark and `dropDuplicates` on
   `(system_id, source, station_id, feed_last_updated)`.
2. `SpeedLayer.process` classifies every station (empty / full / ok / out of service),
   reads the previous state from Redis (one pipeline of `HGETALL`), runs the alert
   state machine and the 30-minute forecast.
3. One pipeline writes everything back (station hashes, status index, alerts,
   warnings, counters).

Consequences worth knowing:

- **Idempotent.** Writing the same observation twice leaves the same state, and an
  alert id is derived from station + kind + opening time, so a retried batch cannot
  open a duplicate alert.
- **The clock is the data.** All times written here are the feed's `event_time`
  (the newest one in the batch), not the wall clock: replaying recorded daytime data
  behaves exactly like the live feed. `bf:meta:batch.job_time` is the only wall clock.
- **At-least-once.** `foreachBatch` may repeat a batch after a crash; see above why
  that is harmless.

## 2. Keys

`<prefix>` is `REDIS_KEY_PREFIX` (default `bf`). Nothing else writes these keys, and
no key is ever written by the dashboard.

| Key | Type | TTL | Content |
|---|---|---|---|
| `bf:station:{station_id}` | hash | none | current state of one station (§3) |
| `bf:stations:ok` / `:empty` / `:full` / `:out_of_service` | set | none | station ids per status; a station is in exactly one of them, so `SCARD` is the live counter |
| `bf:alert:{alert_id}` | hash | `ALERT_HISTORY_TTL_SECONDS` (1 h) | one alert, open or recently closed (§4) |
| `bf:alerts:active` | sorted set | none | open alerts, score = `opened_at` (epoch s) |
| `bf:alerts:recent` | sorted set | none | last 100 closed alerts, score = `closed_at` |
| `bf:alerts:events` | stream | capped | `opened` / `escalated` / `closed`, `MAXLEN ~20000` (audit and demo) |
| `bf:station_alerts:{station_id}` | sorted set | none | the open alert ids of one station (map popups) |
| `bf:warning:{station_id}` | hash | `WARNING_TTL_SECONDS` (15 min) | the 30-minute warning of one station (§5) |
| `bf:warnings:active` | sorted set | none | warnings, score = expiry (epoch s) |
| `bf:stats:live` | hash | none | counters of the live view (§6) |
| `bf:meta:batch` | hash | none | health of the last micro-batch (§6) |
| `bf:meta:profiles` | hash | none | `status` = `loaded` or `missing` |
| `bf:events` | pub/sub | — | JSON of every alert event, for instant dashboard refresh |

## 3. `bf:station:{station_id}` — the current state

One hash per station, overwritten on every change (and every heartbeat, so the map
never shows a state older than 15 minutes for a quiet station).

| Field | Meaning |
|---|---|
| `station_id`, `system_id`, `source` | identity; `source` is `live` or `replay` |
| `status` | `ok`, `empty`, `full` or `out_of_service` (rules in §4) |
| `bikes`, `ebikes`, `bikes_disabled`, `docks`, `docks_disabled` | the feed's counts |
| `station_size` | `bikes + bikes_disabled + docks + docks_disabled` (the contract's size; `capacity` is unreliable) |
| `is_installed`, `is_renting`, `is_returning` | `1` / `0` |
| `last_reported`, `event_time` | ISO-8601 UTC (`...Z`) |
| `local_hour`, `weekday_num` | New York hour (0–23) and weekday (1 = Monday), as in the batch tables |
| `condition_since` | when the current empty/full condition started (empty when the station is fine) |
| `alert_id`, `alert_kind`, `alert_severity`, `alert_opened_at` | the open alert of this station, or empty strings |
| `updated_at` | when the job last wrote this hash (= stream clock) |
| `name`, `lat`, `lon`, `region_id`, `capacity` | copied from the batch `stations` table when it exists, so the map needs no join |

## 4. Alerts

### Status rules (identical to the dashboard and the contract)

- **out of service**: `is_installed = 0`, or installed but `is_renting = 0` or
  `is_returning = 0`, or `last_reported` older than `ALERT_STALE_MINUTES` (60 min).
  These stations are left out of the alerts.
- **empty**: `bikes = 0` (no bike to rent).
- **full**: `docks = 0` (no free dock to return a bike).
- **ok**: anything else.

### Lifecycle

| Step | Rule |
|---|---|
| debounce | the condition must hold for `ALERT_OPEN_AFTER_MINUTES` (2 min) before an alert opens — one noisy snapshot must not page anyone |
| open | `warning`, `alert_id = {system_id}:{station_id}:{kind}:{opened_at epoch}` |
| escalate | `critical` after `ALERT_CRITICAL_AFTER_MINUTES` (10 min) |
| close | on recovery (`recovered`), on leaving the alerts (`out_of_service`), or when the condition flips to the other kind (`superseded`) |

`bf:alert:{alert_id}` fields: `alert_id`, `station_id`, `system_id`, `kind`
(`empty`/`full`), `severity` (`warning`/`critical`), `status` (`open`/`closed`),
`opened_at`, `condition_since`, `updated_at`, `event_time`, `bikes`, `docks`,
`station_size`, `local_hour`, `weekday_num`, `closed_at`, `close_reason`.

A closed alert stays readable for an hour (`ALERT_HISTORY_TTL_SECONDS`) and in
`bf:alerts:recent`; it leaves `bf:alerts:active` and `bf:station_alerts:{id}` at once.

## 5. 30-minute warnings

A warning says "this station is likely to empty or fill within 30 minutes, given its
usual demand at this hour and weekday". It is computed from the batch
`demand_profiles` table (departures and arrivals per station, weekday and hour):

```
net30 = net(this hour) × min(0.5 h, time left in the hour) + net(next hour) × the rest
predicted_bikes = bikes + net30        predicted_docks = docks - net30
```

- `likely_empty` when `predicted_bikes <= 0`, `likely_full` when `predicted_docks <= 0`,
  `eta_minutes` = when the count reaches zero at this flow.
- Only for stations that are `ok` (empty/full stations already have an alert) and only
  when the usual flow is strong enough (`|net| >= 0.5` bikes/hour); otherwise silence.
- `confidence` is `high` / `medium` / `low` for `|net| >= 5`, `>= 2`, below.

`bf:warning:{station_id}` fields: `station_id`, `kind` (`likely_empty`/`likely_full`),
`horizon_minutes`, `bikes`, `docks`, `station_size`, `predicted_bikes`,
`predicted_docks`, `net_flow_per_hour`, `eta_minutes`, `confidence`, `local_hour`,
`weekday_num`, `basis`, `created_at`, `expires_at`.

**The input does not exist yet.** `demand_profiles` is the batch-processing lead's
table and has not been produced, so `bikeflow.streaming.warnings.load_demand_profiles`
is a blank function raising `NotImplementedError` with the expected columns. Until it
lands, the job runs without warnings and `bf:meta:profiles.status` is `missing`
(the dashboard can say "warnings need the demand profiles" instead of showing nothing).

## 6. Counters and health

`bf:stats:live`: `ok`, `empty`, `full`, `out_of_service`, `total`, `alerts_open`,
`warnings`, `event_time` (newest event written), `lag_seconds` (wall clock minus that
event), `updated_at`.

`bf:meta:batch`: `epoch_id`, `rows`, `stations`, `alerts_opened`, `alerts_escalated`,
`alerts_closed`, `warnings`, `duration_ms`, `event_time_max`, `updated_at`, `job_time`.

`bf:meta:profiles`: `status` (`loaded`/`missing`), `rows`, `table`, `updated_at`.

## 7. Reading it from the dashboard

```python
import redis

r = redis.Redis.from_url("redis://localhost:6379/0", decode_responses=True)

r.hgetall("bf:stats:live")                       # the header of the live page
r.smembers("bf:stations:empty")                  # ids; then HGETALL bf:station:{id}
r.zrevrange("bf:alerts:active", 0, 19)           # newest 20 alerts, then HGETALL bf:alert:{id}
r.hgetall("bf:station:66db237e-...")             # one station: status, counts, name, lat, lon
r.zrange("bf:alerts:events", 0, -1)              # history of this run (audit)
```

To follow alerts live, subscribe to `bf:events`:

```python
pubsub = r.pubsub()
pubsub.subscribe("bf:events")
for message in pubsub.listen():
    ...                                          # {"event": "opened", "station_id": ..., ...}
```

## 8. Freshness

- Live: the producer polls every 60 s, the job runs every 15 s, so the state in Redis
  is normally under ~20 s older than the feed. `bf:stats:live.lag_seconds` says it
  exactly; if it grows, the job is behind (check `bf:meta:batch.job_time`).
- Heartbeats every 15 min keep quiet stations fresh, so a station hash that is an hour
  old really means the station stopped reporting (and it becomes `out_of_service`).
- Nothing in Redis is permanent except station state: alerts expire after an hour,
  warnings after 15 minutes, so Redis stays small (≈2.5k hashes + a few dozen alerts).

