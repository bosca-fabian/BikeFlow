"""Live producer for station_status.

Poll GBFS every POLL_SECONDS -> record raw snapshot -> send changed stations to Kafka.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from typing import Protocol

from bikeflow.ingest.messages import ChangeDetector, build_message, snapshot_from_feed

log = logging.getLogger(__name__)


class Publisher(Protocol):
    def publish(self, topic: str, messages: list[dict]) -> int: ...


def snapshot_to_messages(
    feed: dict,
    detector: ChangeDetector,
    *,
    source: str,
    system_id: str,
    now: float,
    time_shift_seconds: int = 0,
    fallback_ts: int | None = None,
) -> tuple[int, list[dict]]:
    snap = snapshot_from_feed(feed, fallback_ts=fallback_ts)
    selected = detector.select(snap)
    msgs = [
        build_message(
            st,
            feed_last_updated=snap.feed_last_updated,
            reason=reason,
            source=source,
            system_id=system_id,
            ingested_at=now,
            time_shift_seconds=time_shift_seconds,
        )
        for st, reason in selected
    ]
    return snap.feed_last_updated, msgs


def run_live(
    *,
    client,
    publisher: Publisher,
    topic: str,
    system_id: str,
    poll_seconds: float,
    heartbeat_seconds: float,
    recorder=None,
    stop: threading.Event | None = None,
    max_polls: int | None = None,
    clock: Callable[[], float] = time.time,
    side_tasks: list[Callable[[], object]] | None = None,
) -> dict:
    """Poll the live feed until ``stop`` is set (or ``max_polls`` reached).

    ``side_tasks`` run after every poll (e.g. the system_alerts poller, which
    decides itself whether it is due); they must not raise.
    """
    stop = stop or threading.Event()
    detector = ChangeDetector(heartbeat_seconds=heartbeat_seconds)
    stats = {"polls": 0, "snapshots": 0, "messages": 0, "errors": 0, "unchanged_feed": 0}
    last_feed_ts = None
    while not stop.is_set():
        started = time.monotonic()
        stats["polls"] += 1
        try:
            feed = client.fetch_station_status()
            now = clock()
            feed_ts, msgs = snapshot_to_messages(
                feed, detector, source="live", system_id=system_id, now=now, fallback_ts=int(now)
            )
            if feed_ts == last_feed_ts:
                stats["unchanged_feed"] += 1
                log.info("Feed not refreshed since last poll (last_updated=%s), nothing sent", feed_ts)
            else:
                last_feed_ts = feed_ts
                if recorder is not None:
                    recorder.write(feed, feed_ts, now)
                publisher.publish(topic, msgs)
                stats["snapshots"] += 1
                stats["messages"] += len(msgs)
                reasons = {}
                for m in msgs:
                    reasons[m["reason"]] = reasons.get(m["reason"], 0) + 1
                log.info(
                    "Snapshot %s: %d stations in feed, sent %d %s",
                    feed_ts,
                    len(feed.get("data", {}).get("stations", [])),
                    len(msgs),
                    reasons or "",
                )
        except Exception:  # noqa: BLE001 - keep polling whatever happens
            stats["errors"] += 1
            log.exception("Poll failed; will retry next interval")
        for task in side_tasks or []:
            task()
        if max_polls is not None and stats["polls"] >= max_polls:
            break
        stop.wait(max(0.0, poll_seconds - (time.monotonic() - started)))
    return stats
