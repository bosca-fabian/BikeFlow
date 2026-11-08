"""``bikeflow-stream``: the speed layer of BikeFlow (owner: streaming lead).

::

    run     Kafka `station_status` -> Spark Structured Streaming -> Redis
            (station state, empty/full alerts, 30-minute warnings)
    dump    what the dashboard sees, printed in the terminal (no dashboard needed)
    reset   delete every key of the speed layer (development only)

Settings come from environment variables (see README_streaming.md); the names are
the same inside Docker and on a laptop.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path


def _setup_logging(log_file: str | None = None) -> None:
    """Log to stdout and, when asked, to a file (``logs/stream.log`` and similar)."""
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_file:
        path = Path(log_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(path, encoding="utf-8"))
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        handlers=handlers,
    )


def cmd_run(args) -> int:
    from bikeflow.streaming.job import run

    return run(
        replay=args.replay,
        once=args.once,
        trigger_seconds=args.trigger_seconds,
        starting_offsets=args.starting_offsets,
        job_name=args.job_name,
    )


def cmd_dump(args) -> int:
    from bikeflow.streaming.redis_sink import RedisStateWriter

    writer = RedisStateWriter.connect()
    if not writer.ping():
        print(f"Redis is not reachable at {writer.settings.redis_url}", file=sys.stderr)
        return 2
    snapshot = writer.snapshot(limit=args.limit)
    if args.json:
        print(json.dumps(snapshot, indent=2, ensure_ascii=False))
        return 0

    stats = snapshot["stats"] or {}
    if not stats:
        print("nothing in Redis yet: is `bikeflow-stream run` running?")
        return 0
    print(
        "stations: total={total} ok={ok} empty={empty} full={full} out_of_service={out_of_service}".format(**{
            **{key: stats.get(key, "?") for key in ("total", "ok", "empty", "full", "out_of_service")}
        })
    )
    print(f"as of {stats.get('event_time', '?')} (written {stats.get('updated_at', '?')}, lag {stats.get('lag_seconds', '?')}s)")
    profiles = snapshot["profiles"] or {}
    print(f"demand profiles: {profiles.get('status', 'missing')} ({profiles.get('rows', 0)} rows from {profiles.get('table', '-')})")

    alerts = snapshot["alerts"]
    print(f"\nopen alerts ({stats.get('alerts_open', len(alerts))}):")
    for alert in alerts:
        print(
            f"  [{alert.get('severity', '?')}] {alert.get('kind', '?')} {alert.get('station_id', '?')} "
            f"bikes={alert.get('bikes', '?')} docks={alert.get('docks', '?')} "
            f"opened {alert.get('opened_at', '?')}"
        )

    warnings = snapshot["warnings"]
    print(f"\n30-minute warnings ({len(warnings)}):")
    for warning in warnings:
        print(
            f"  {warning.get('station_id', '?')} {warning.get('kind', '?')} "
            f"eta={warning.get('eta_minutes', '?')}min confidence={warning.get('confidence', '?')} "
            f"bikes={warning.get('bikes', '?')}->{warning.get('predicted_bikes', '?')}"
        )

    print(f"\nempty stations (first {args.limit}):")
    for station in snapshot["empty_stations"]:
        print(
            f"  {station.get('station_id', '?')} bikes={station.get('bikes', '?')} docks={station.get('docks', '?')} "
            f"{station.get('name', '')}".rstrip()
        )
    return 0


def cmd_reset(args) -> int:
    from bikeflow.streaming.redis_sink import RedisStateWriter

    writer = RedisStateWriter.connect()
    if not writer.ping():
        print(f"Redis is not reachable at {writer.settings.redis_url}", file=sys.stderr)
        return 2
    if not args.yes:
        print("This deletes every key of the speed layer. Re-run with --yes to confirm.")
        return 1
    print(f"deleted {writer.reset()} keys")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="bikeflow-stream", description="BikeFlow speed layer (Kafka -> Spark -> Redis)")
    sub = p.add_subparsers(dest="command", required=True)

    sp = sub.add_parser("run", help="start the Structured Streaming job")
    sp.add_argument("--replay", action="store_true", help="read the replay topic instead of the live one")
    sp.add_argument("--once", action="store_true", help="process what is in Kafka and stop")
    sp.add_argument("--trigger-seconds", type=int, default=None, help="micro-batch interval (default STREAM_TRIGGER_SECONDS)")
    sp.add_argument("--starting-offsets", choices=["earliest", "latest"], default=None)
    sp.add_argument("--job-name", default=None, help="also the checkpoint folder (default STREAM_JOB_NAME)")
    sp.add_argument(
        "--log-file",
        default=os.getenv("LOG_FILE") or None,
        help="also write the log to this file, e.g. logs/stream.log (default LOG_FILE)",
    )
    sp.set_defaults(func=cmd_run)

    sp = sub.add_parser("dump", help="print the live state kept in Redis")
    sp.add_argument("--limit", type=int, default=10)
    sp.add_argument("--json", action="store_true", help="raw JSON instead of a summary")
    sp.set_defaults(func=cmd_dump)

    sp = sub.add_parser("reset", help="delete every key of the speed layer")
    sp.add_argument("--yes", action="store_true", help="do it (development only)")
    sp.set_defaults(func=cmd_reset)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    _setup_logging(getattr(args, "log_file", None))
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
