"""`bikeflow-ingest`: the four ingestion services started by docker compose.

    produce   live Citi Bike feed -> Kafka `station_status` (+ raw recordings,
              + system_alerts every 5 min, + system_regions daily)
    sink      Kafka `station_status` -> data/lake/station_status_raw

Settings come from environment variables (see .env.example).
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import threading

from bikeflow import config
from bikeflow.lake import STATION_STATUS_RAW, ensure_local_layout, table_path

log = logging.getLogger("bikeflow.ingest")


def _setup_logging() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stdout,
    )


def _stop_event() -> threading.Event:
    stop = threading.Event()

    def handler(signum, _frame):
        log.info("Received signal %s, shutting down cleanly...", signum)
        stop.set()

    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)
    return stop


# --------------------------------------------------------------------------- services
def cmd_produce(args) -> int:
    from bikeflow.ingest.gbfs import GbfsClient
    from bikeflow.ingest.kafka_io import KafkaPublisher, ensure_topics, wait_for_kafka
    from bikeflow.ingest.producer import run_live
    from bikeflow.ingest.recordings import Recorder

    s = config.settings
    wait_for_kafka(s.kafka_bootstrap)
    ensure_topics(
        s.kafka_bootstrap,
        [s.topic_station_status],
        s.topic_partitions,
        s.topic_retention_hours,
    )
    record = s.record_live and not args.no_record
    recorder = Recorder(s.recordings_root) if record else None
    log.info(
        "Live producer: %s -> topic %s every %ss (recording %s)",
        s.gbfs_discovery_url,
        s.topic_station_status,
        s.poll_seconds,
        f"to {s.recordings_root}" if record else "off",
    )
    from bikeflow.ingest.alerts import AlertsPoller
    from bikeflow.ingest.regions import RegionsPoller
    from bikeflow.lake import SYSTEM_ALERTS_RAW, SYSTEM_REGIONS

    ensure_local_layout()
    client = GbfsClient(s.gbfs_discovery_url, s.gbfs_language, s.http_timeout_seconds)
    alerts = AlertsPoller(client, table_path(SYSTEM_ALERTS_RAW), s.system_id, s.alerts_every_minutes)
    regions = RegionsPoller(client, table_path(SYSTEM_REGIONS), s.system_id, s.regions_every_hours)
    publisher = KafkaPublisher(s.kafka_bootstrap, "bikeflow-producer")
    try:
        stats = run_live(
            client=client,
            publisher=publisher,
            topic=s.topic_station_status,
            system_id=s.system_id,
            poll_seconds=s.poll_seconds,
            heartbeat_seconds=s.heartbeat_minutes * 60,
            recorder=recorder,
            stop=_stop_event(),
            max_polls=args.max_polls,
            side_tasks=[alerts.maybe_poll, regions.maybe_poll],
        )
    finally:
        publisher.close()
    log.info("Producer stopped: %s", stats)
    return 0


def cmd_sink(args) -> int:
    from confluent_kafka import Consumer

    from bikeflow.ingest.kafka_io import ensure_topics, wait_for_kafka
    from bikeflow.ingest.lake_sink import DeltaWriter, LakeSink, consumer_config

    s = config.settings
    topic = s.topic_station_status
    table = table_path(STATION_STATUS_RAW)
    group = s.sink_group_id
    ensure_local_layout()
    wait_for_kafka(s.kafka_bootstrap)
    ensure_topics(s.kafka_bootstrap, [topic], s.topic_partitions, s.topic_retention_hours)
    log.info("Lake sink: topic %s -> %s (group %s)", topic, table, group)
    sink = LakeSink(
        Consumer(consumer_config(s.kafka_bootstrap, group)),
        DeltaWriter(table, group),
        [topic],
        flush_rows=s.sink_flush_rows,
        flush_seconds=s.sink_flush_seconds,
        compact_every_hours=float(os.getenv("SINK_COMPACT_EVERY_HOURS", "6")),
        stop=_stop_event(),
    )
    stats = sink.run()
    log.info("Sink stopped: %s", stats)
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="bikeflow-ingest", description="BikeFlow ingestion services")
    sub = p.add_subparsers(dest="command", required=True)

    sp = sub.add_parser("produce", help="poll the live feed and publish to Kafka")
    sp.add_argument("--no-record", action="store_true", help=argparse.SUPPRESS)
    sp.add_argument("--max-polls", type=int, default=None, help=argparse.SUPPRESS)
    sp.set_defaults(func=cmd_produce)

    sp = sub.add_parser("sink", help="copy the Kafka topic into data/lake/station_status_raw")
    sp.set_defaults(func=cmd_sink)

    return p


def main(argv: list[str] | None = None) -> int:
    _setup_logging()
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
