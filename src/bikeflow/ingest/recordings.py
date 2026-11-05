"""Record raw station_status snapshots to disk (and read them back).

Layout (UTC, one gzip-compressed JSON line per snapshot)::

    <BIKEFLOW_DATA>/raw/station_status/date=2026-11-04/hour=13.jsonl.gz

Each line is ``{"recorded_at": "...Z", "feed_last_updated": 1730728800, "feed": {...}}``
where ``feed`` is the untouched GBFS document. Files are appended one gzip
member per snapshot, so a crash never corrupts earlier snapshots.
A full day is roughly 50-120 MB.
"""

from __future__ import annotations

import gzip
import json
import logging
import zlib
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

log = logging.getLogger(__name__)

FEED_DIR = "station_status"


def _utc(ts: int) -> datetime:
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def recording_file(root: Path, feed_ts: int) -> Path:
    dt = _utc(feed_ts)
    return Path(root) / FEED_DIR / f"date={dt:%Y-%m-%d}" / f"hour={dt:%H}.jsonl.gz"


class Recorder:
    def __init__(self, root: str | Path):
        self.root = Path(root)

    def write(self, feed: dict, feed_ts: int, recorded_at: float) -> Path:
        path = recording_file(self.root, feed_ts)
        path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(
            {
                "recorded_at": datetime.fromtimestamp(recorded_at, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "feed_last_updated": feed_ts,
                "feed": feed,
            },
            separators=(",", ":"),
        )
        with gzip.open(path, "at", encoding="utf-8") as fh:
            fh.write(line + "\n")
        return path


def _read_lines(path: Path) -> Iterator[str]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            for line in fh:
                if line.strip():
                    yield line
    except (EOFError, gzip.BadGzipFile, zlib.error, OSError) as exc:
        # A snapshot cut off by a crash: keep everything before it.
        log.warning("Stopped reading %s at a damaged snapshot (%s)", path, exc)


def _hour_files(root: Path, start: datetime, end: datetime) -> list[Path]:
    files = []
    hour = start.replace(minute=0, second=0, microsecond=0)
    while hour <= end:
        p = Path(root) / FEED_DIR / f"date={hour:%Y-%m-%d}" / f"hour={hour:%H}.jsonl.gz"
        if p.exists():
            files.append(p)
        hour += timedelta(hours=1)
    return files


def iter_snapshots(root: str | Path, start: datetime, end: datetime) -> Iterator[tuple[int, dict]]:
    """Yield (feed_last_updated, feed) for recorded snapshots in [start, end), oldest first."""
    start_ts, end_ts = int(start.timestamp()), int(end.timestamp())
    last_ts = None
    for path in _hour_files(Path(root), start.astimezone(timezone.utc), end.astimezone(timezone.utc)):
        batch = []
        for line in _read_lines(path):
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                log.warning("Skipping unreadable line in %s", path)
                continue
            ts = int(rec["feed_last_updated"])
            if start_ts <= ts < end_ts:
                batch.append((ts, rec["feed"]))
        batch.sort(key=lambda r: r[0])
        for ts, feed in batch:
            if last_ts is not None and ts <= last_ts:
                continue  # duplicate or out-of-order snapshot
            last_ts = ts
            yield ts, feed


@dataclass
class RecordedDay:
    day: date
    hours: list[int]
    snapshots: int
    first_ts: int | None
    last_ts: int | None
    size_bytes: int


def summarize(root: str | Path) -> list[RecordedDay]:
    """List recorded days with snapshot counts."""
    base = Path(root) / FEED_DIR
    days = []
    for day_dir in sorted(base.glob("date=*")):
        day = date.fromisoformat(day_dir.name.split("=", 1)[1])
        hours, count, first, last, size = [], 0, None, None, 0
        for f in sorted(day_dir.glob("hour=*.jsonl.gz")):
            hours.append(int(f.name.split("=", 1)[1].split(".", 1)[0]))
            size += f.stat().st_size
            for line in _read_lines(f):
                try:
                    ts = int(json.loads(line)["feed_last_updated"])
                except (json.JSONDecodeError, KeyError, ValueError):
                    continue
                count += 1
                first = ts if first is None else min(first, ts)
                last = ts if last is None else max(last, ts)
        days.append(RecordedDay(day, hours, count, first, last, size))
    return days
