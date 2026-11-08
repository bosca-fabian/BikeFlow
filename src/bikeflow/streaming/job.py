"""The Spark Structured Streaming job of the speed layer (owner: streaming lead).

::

    Kafka `station_status`
      -> read_station_status_stream()        parsed messages (bikeflow.streams)
      -> watermark + dropDuplicates          one row per observation
      -> foreachBatch -> SpeedLayer.process  status, alerts, warnings
      -> Redis                               state, alerts, warnings (redis_sink)

Run it with ``bikeflow-stream run`` (see README_streaming.md).

Two decisions worth knowing:

* **The clock is the data.** Statuses, the debounce and the alerts use the newest
  ``event_time`` in the batch, not the wall clock. Live, that is a few seconds
  behind real time; during a replay of recorded daytime data (the demo) it keeps
  the same behaviour instead of calling every station stale.
* **``foreachBatch`` is at-least-once.** A retried batch rewrites the same state
  and cannot open a duplicate alert, because the alert id is derived from the
  station, the kind and the opening time (see ``alerts.make_alert_id``).

Nothing here needs Spark at import time, and ``SpeedLayer.process`` takes plain
rows, so the whole pipeline is unit-tested without a cluster.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from bikeflow import config
from bikeflow.lake import checkpoint_path
from bikeflow.streaming import alerts as alert_rules
from bikeflow.streaming import lake_extras, model, warnings as warnings_rules
from bikeflow.streaming.redis_sink import RedisStateWriter
from bikeflow.streams import DEDUPE_KEY, read_station_status_stream

log = logging.getLogger(__name__)


@dataclass
class BatchResult:
    """What one micro-batch did (returned by ``SpeedLayer.process``)."""

    epoch_id: int | None = None
    rows: int = 0
    observations: list = field(default_factory=list)
    statuses: dict = field(default_factory=dict)
    outcomes: dict = field(default_factory=dict)
    warnings: list = field(default_factory=list)
    event_time_max: datetime | None = None
    clock: datetime | None = None
    stats: dict = field(default_factory=dict)

    @property
    def counts(self) -> dict:
        return model.status_counts(self.statuses)


class SpeedLayer:
    """Turns parsed ``station_status`` rows into what Redis should hold."""

    def __init__(
        self,
        writer: RedisStateWriter,
        settings=None,
        profiles: dict | None = None,
        station_metadata: dict | None = None,
        thresholds: alert_rules.Thresholds | None = None,
    ):
        self.writer = writer
        self.settings = settings or config.settings
        self.profiles = profiles
        self.station_metadata = station_metadata or {}
        self.thresholds = thresholds or alert_rules.Thresholds.from_settings(self.settings)
        self.stale_after = timedelta(minutes=self.settings.alert_stale_minutes)

    def process(self, rows: list, epoch_id: int | None = None, now: datetime | None = None) -> BatchResult:
        """Status, alerts and warnings of one micro-batch, written to Redis."""
        started = time.monotonic()
        observations = [obs for obs in (model.StationObservation.from_row(row) for row in rows) if obs is not None]
        observations = model.latest_per_station(observations)
        event_time_max = max((obs.event_time for obs in observations), default=None)
        # Stream clock: the newest event of the batch (see the module docstring).
        clock = event_time_max or now or datetime.now(timezone.utc)

        statuses = {obs.station_id: model.classify(obs, clock, self.stale_after) for obs in observations}

        previous = self.writer.read_previous([obs.station_id for obs in observations])
        outcomes: dict[str, alert_rules.AlertOutcome] = {}
        for obs in observations:
            stored = previous.get(obs.station_id) or {}
            outcomes[obs.station_id] = alert_rules.evaluate(
                obs,
                statuses[obs.station_id],
                alert_rules.open_alert_from_hash(stored),
                model.as_utc(stored.get("condition_since")),
                clock,
                self.thresholds,
            )

        warnings_list: list[dict] = []
        if self.profiles:
            for obs in observations:
                warning = warnings_rules.build_warning(
                    obs,
                    statuses[obs.station_id],
                    self.profiles,
                    clock,
                    horizon_minutes=self.settings.warning_horizon_minutes,
                )
                if warning:
                    warnings_list.append(warning)

        wall_clock = now or datetime.now(timezone.utc)
        lag = int((wall_clock - event_time_max).total_seconds()) if event_time_max else None
        stats = self.writer.write(
            observations,
            statuses,
            outcomes,
            warnings_list,
            self.station_metadata,
            {
                "epoch_id": epoch_id,
                "rows": len(rows),
                "now": clock,
                "wall_clock": wall_clock,
                "event_time_max": event_time_max,
                "lag_seconds": lag,
                "duration_ms": int((time.monotonic() - started) * 1000),
            },
        )
        result = BatchResult(
            epoch_id=epoch_id,
            rows=len(rows),
            observations=observations,
            statuses=statuses,
            outcomes=outcomes,
            warnings=warnings_list,
            event_time_max=event_time_max,
            clock=clock,
            stats=stats,
        )
        log.info(
            "batch %s: %d rows, %d stations, %s, %d alert(s) opened, %d closed, %d warning(s)",
            epoch_id,
            result.rows,
            len(observations),
            result.counts,
            stats.get("alerts_opened", 0),
            stats.get("alerts_closed", 0),
            stats.get("warnings", 0),
        )
        return result

    def handle_batch(self, df, epoch_id: int | None = None) -> BatchResult:
        """``foreachBatch`` entry point: collect the micro-batch and apply it."""
        rows = [row.asDict(recursive=True) for row in df.collect()]
        return self.process(rows, epoch_id)


def build_stream(spark, settings=None, replay: bool = False, starting_offsets: str | None = None):
    """The deduplicated stream of parsed messages: one row per station observation.

    ``event_time`` comes from the feed (``feed_last_updated``), never from Kafka:
    the data contract is explicit about that. The watermark drops observations that
    arrive more than ``STREAM_WATERMARK_MINUTES`` late, and ``dropDuplicates`` keeps
    one row per ``(system_id, source, station_id, feed_last_updated)``.
    """
    s = settings or config.settings
    status = read_station_status_stream(
        spark,
        starting_offsets=starting_offsets or s.stream_starting_offsets,
        replay=replay,
    )
    watermark = f"{float(s.stream_watermark_minutes):g} minutes"
    return status.withWatermark("event_time", watermark).dropDuplicates(DEDUPE_KEY)


def build_layer(writer: RedisStateWriter, spark, settings=None) -> SpeedLayer:
    """Load the optional inputs (station metadata, demand profiles) once, at start-up."""
    s = settings or config.settings
    metadata = lake_extras.load_station_metadata(spark)

    profiles = None
    if warnings_rules.demand_profiles_available():
        profiles = warnings_rules.load_demand_profiles(spark, s.demand_profiles_table)
        writer.write_profiles_status(True, len(profiles), s.demand_profiles_table)
        log.info("demand profiles: %d loaded from %s", len(profiles), s.demand_profiles_table)
    else:
        writer.write_profiles_status(False, 0, s.demand_profiles_table)
        log.warning(
            "demand profiles: missing -> the 30-minute warnings are off until the batch table exists "
            "(blank function in bikeflow.streaming.warnings)"
        )
    return SpeedLayer(writer, s, profiles=profiles, station_metadata=metadata)


def run(
    replay: bool = False,
    once: bool = False,
    trigger_seconds: int | None = None,
    starting_offsets: str | None = None,
    job_name: str | None = None,
) -> int:
    """Start the streaming job and block until it stops."""
    from bikeflow.spark import get_spark

    s = config.settings
    job_name = job_name or s.stream_job_name
    writer = RedisStateWriter.connect()
    if not writer.ping():
        raise SystemExit(f"Redis is not reachable at {s.redis_url} (is `docker compose up -d redis` running?)")

    spark = get_spark(job_name, kafka=True)
    layer = build_layer(writer, spark, s)
    stream = build_stream(spark, s, replay=replay, starting_offsets=starting_offsets)

    query = (
        stream.writeStream.foreachBatch(layer.handle_batch)
        .option("checkpointLocation", checkpoint_path(job_name))
        .queryName(job_name)
        .outputMode("append")
    )
    if once:
        # Process what is already in Kafka and stop (demos and tests).
        query = query.trigger(availableNow=True)
    else:
        query = query.trigger(processingTime=f"{trigger_seconds or s.stream_trigger_seconds} seconds")

    query = query.start()
    log.info(
        "%s started: %s -> Redis %s (checkpoint %s, %s)",
        job_name,
        s.topic_station_status_replay if replay else s.topic_station_status,
        s.redis_url,
        checkpoint_path(job_name),
        "availableNow" if once else f"every {trigger_seconds or s.stream_trigger_seconds}s",
    )
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        log.info("stopping %s ...", job_name)
        query.stop()
    return 0


