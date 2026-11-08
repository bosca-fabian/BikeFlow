# BikeFlow – speed layer (streaming path)

Owner: Teodor (streaming lead).

Kafka `station_status` → **Spark Structured Streaming** → **Redis**, where the live
dashboard reads it: the current state of every station, the empty/full alerts and the
30-minute warnings. The Redis model is documented in `docs/SPEED_LAYER.md`; the input
it consumes is documented in `docs/DATA_CONTRACT.md`.

```
producer ──▶ Kafka `station_status` ──▶ bikeflow-stream (Spark) ──▶ Redis ──▶ Streamlit
             (ingestion lead)            status, alerts, warnings    (dashboard)
```

| Piece | Code |
|---|---|
| status rules (empty / full / out of service) | `src/bikeflow/streaming/model.py` |
| alert lifecycle (open, escalate, close) | `src/bikeflow/streaming/alerts.py` |
| 30-minute warnings | `src/bikeflow/streaming/warnings.py` |
| Redis model and writer | `src/bikeflow/streaming/redis_keys.py`, `redis_sink.py` |
| the Structured Streaming job | `src/bikeflow/streaming/job.py` |
| command line | `src/bikeflow/streaming/cli.py` (`bikeflow-stream`) |

## How to run

### In Docker (the team environment)

```bash
docker compose up -d --build            # kafka, redis, producer, lake-sink, stream
docker compose logs -f stream           # what the job is doing
docker compose exec redis redis-cli hgetall bf:stats:live
```

The `stream` service is defined in `docker-compose.yml` and restarts with the stack.

### On a laptop

Needs Python 3.11/3.12, Java 17, Kafka and Redis reachable from the machine.

```bash
pip install -r requirements-dev.txt
pip install -e .
bikeflow-stream run                     # blocks; Ctrl+C stops it cleanly
bikeflow-stream dump                    # what the dashboard sees, in the terminal
bikeflow-stream reset --yes             # delete every speed-layer key (dev only)
```

Useful variants:

```bash
bikeflow-stream run --starting-offsets earliest      # re-read the topic from the start
bikeflow-stream run --once                           # process what is in Kafka, then stop
bikeflow-stream run --replay                         # read `station_status_replay` (demo)
bikeflow-stream run --trigger-seconds 5              # faster micro-batches for a demo
```

A replay uses the same checkpoint folder as the live run (`STREAM_JOB_NAME`), so give
the demo its own name and keep the live checkpoint intact:

```bash
bikeflow-stream run --replay --job-name speed-layer-replay
```

## Settings (environment variables)

All of them have a working default; `src/bikeflow/config.py` is the source of truth.

| Variable | Default | Meaning |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `localhost:9092` (`kafka:19092` in Docker) | Kafka brokers |
| `TOPIC_STATION_STATUS` | `station_status` | live topic |
| `TOPIC_STATION_STATUS_REPLAY` | `station_status_replay` | replay topic (`--replay`) |
| `REDIS_URL` | `redis://localhost:6379/0` | speed storage |
| `REDIS_KEY_PREFIX` | `bf` | prefix of every key the job writes |
| `STREAM_JOB_NAME` | `speed-layer` | also the checkpoint folder under `data/lake/_checkpoints/` |
| `STREAM_TRIGGER_SECONDS` | `15` | micro-batch interval |
| `STREAM_WATERMARK_MINUTES` | `2` | lateness allowed before a row is dropped |
| `STREAM_STARTING_OFFSETS` | `latest` | `earliest` to replay the retained topic (72 h) |
| `ALERT_OPEN_AFTER_MINUTES` | `2` | how long empty/full must hold before an alert opens |
| `ALERT_CRITICAL_AFTER_MINUTES` | `10` | when an open alert escalates to `critical` |
| `ALERT_STALE_MINUTES` | `60` | stations silent for longer are `out_of_service` |
| `ALERT_HISTORY_TTL_SECONDS` | `3600` | how long a closed alert stays readable |
| `WARNING_HORIZON_MINUTES` | `30` | look-ahead of the early warnings |
| `WARNING_TTL_SECONDS` | `900` | how long a warning stays in Redis |
| `DEMAND_PROFILES_TABLE` | `demand_profiles` | batch table with the usual demand |
| `STATION_STATE_TTL_SECONDS` | `0` | `0` keeps station state forever |

## What is not implemented yet (on purpose)

These are blank functions in the code, with the exact contract in their docstring, so
the missing inputs do not block the streaming job:

| Blank | File | Waits for |
|---|---|---|
| `load_demand_profiles()` / `demand_profiles_available()` | `warnings.py` | the `demand_profiles` table (batch processing lead). Without it the job runs, `bf:meta:profiles.status = missing` and there are no 30-minute warnings |
| `write_warnings_to_lake()` | `warnings.py` | an agreed Delta table for warning history (one writer per table) |
| `send_alert_notification()` | `notify.py` | notifications outside the dashboard (not in Deliverable 1) |
| replay entry point in `bikeflow-ingest` | `src/bikeflow/ingest/cli.py` | the ingestion lead (`read_station_status_stream(replay=True)` already works) |

When the demand profiles land, `load_demand_profiles` is one `spark.read` plus
`profiles_from_rows(rows)`; `SpeedLayer` picks them up automatically at start-up.

## Tests

```bash
pytest tests/test_streaming_*.py -q
```

- `test_streaming_model.py` – the status rules of the contract.
- `test_streaming_alerts.py` – debounce, escalation, closing, idempotency.
- `test_streaming_warnings.py` – the forecast math and the blanks.
- `test_streaming_redis_keys.py` – the key contract of `docs/SPEED_LAYER.md`.
- `test_streaming_sink.py`, `test_streaming_job.py` – the writer and the whole batch
  against an in-memory Redis (`tests/fake_redis.py`), so no server is needed.
- `test_streaming_job_spark.py` – the `foreachBatch` path with a real Spark DataFrame
  (needs Java 17; skipped otherwise).
- `test_streaming_redis.py` – the same writes against a real Redis, skipped when
  `REDIS_URL` is not reachable.

## Troubleshooting

**`java.lang.UnsupportedClassVersionError` / Spark refuses to start.** Spark 3.5 needs
**Java 17**. Check `java -version` (a Java 8 elsewhere on the PATH is a common trap) and
point `JAVA_HOME` at the 17 installation:
`C:\Program Files\Eclipse Adoptium\jre-17.0.21.9-hotspot`.

**`Failed to locate the winutils binary in the hadoop binary path` (Windows).** Spark
writes files through Hadoop: put `winutils.exe` and `hadoop.dll` (Hadoop 3.3.x) in
`C:\hadoop\bin`, set `HADOOP_HOME=C:\hadoop` and add `C:\hadoop\bin` to the PATH.

**`Python worker exited unexpectedly (crashed)`.** PySpark needs a Python it can spawn
for DataFrames **created in Python** (`createDataFrame`, `parallelize`). The streaming
job reads Kafka (a JVM source), so the job itself is not affected, but tests such as
`tests/test_spark_compat.py` are. `get_spark()` and `tests/conftest.py` set
`PYSPARK_PYTHON` to the interpreter in use, which covers the usual cause (Spark
defaults to `python3`, which on Windows is often the Microsoft Store alias). If it
still happens, run those tests in the Docker image, where the team environment is.

**`Redis is not reachable at redis://localhost:6379/0`.** Start it:
`docker compose up -d redis` (or `redis-server`), then `bikeflow-stream run` again.
`bikeflow-stream dump` tells you whether the job is writing anything.

**`KafkaTimeoutError` / the stream starts but no batch arrives.** The topic only gets
messages while the producer runs (`docker compose up -d producer`). With
`--starting-offsets earliest` the job also reads the last 72 h of the topic.

**Nothing appears in Redis during a replay.** The job is reading the live topic; use
`--replay --job-name speed-layer-replay` (the replay topic must be written by the
replay command of the ingestion layer).

