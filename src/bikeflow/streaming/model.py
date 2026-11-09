"""What one ``station_status`` message means: the status rules of the contract.

Pure functions and plain dataclasses (no Spark, no Redis) so they are easy to test
and cheap to reason about. The rules are the ones already used by the dashboard
(``dashboard/queries.py``) and written in ``docs/DATA_CONTRACT.md``:

* **empty** = ``num_bikes_available == 0``
* **full** = ``num_docks_available == 0``
* **out of service** = not installed, installed but not renting or not returning,
  or a ``last_reported`` older than :data:`DEFAULT_STALE_AFTER` (stale station)
* otherwise **ok**

Two time-zone traps this module avoids:

* PySpark hands a timestamp column to Python as a **naive local** datetime
  (``datetime.fromtimestamp``), so ``event_time`` depends on the machine's time
  zone. Every instant used here is therefore taken from the message's epoch fields
  (``feed_last_updated``, ``last_reported``), which are time-zone free.

Disabled bikes are common: a station can hold bikes and still have none to rent,
and a dock holding a broken bike cannot take a return. That is why the checks are
on ``num_bikes_available`` / ``num_docks_available`` and not on the capacity.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

STATUS_OK = "ok"
STATUS_EMPTY = "empty"
STATUS_FULL = "full"
STATUS_OUT_OF_SERVICE = "out_of_service"
STATUSES = (STATUS_OK, STATUS_EMPTY, STATUS_FULL, STATUS_OUT_OF_SERVICE)
# The statuses that open an alert.
ALERTABLE_STATUSES = (STATUS_EMPTY, STATUS_FULL)

# "more than about an hour old" - docs/DATA_CONTRACT.md, "Stations to leave out of alerts".
DEFAULT_STALE_AFTER = timedelta(hours=1)


def as_int(value, default: int = 0) -> int:
    """Tolerant int conversion (Kafka JSON, Spark rows and text all arrive here)."""
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return int(value)
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return default


def as_bool(value, default: bool = True) -> bool:
    """Tolerant bool conversion; ``None`` means "the feed did not say" (assume good)."""
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def as_utc(value) -> datetime | None:
    """Timestamp from a ``datetime`` (Spark), an epoch number or an ISO-8601 string."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    text = str(value).strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def iso(value: datetime | None) -> str:
    """UTC ISO-8601 with a ``Z``, the format used in every Redis hash."""
    if value is None:
        return ""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def epoch(value: datetime | None) -> int:
    """Epoch seconds, for sorted-set scores."""
    if value is None:
        return 0
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp())


NEW_YORK = ZoneInfo("America/New_York")


def new_york_minute(value: datetime) -> int:
    """Minute of the hour in New York (``local_hour`` in the message is the New York hour)."""
    return value.astimezone(NEW_YORK).minute


@dataclass(frozen=True)
class StationObservation:
    """One parsed ``station_status`` message (the fields the speed layer uses)."""

    station_id: str
    system_id: str
    bikes: int
    docks: int
    bikes_disabled: int
    docks_disabled: int
    ebikes: int
    is_installed: bool
    is_renting: bool
    is_returning: bool
    event_time: datetime
    last_reported: datetime | None = None
    local_hour: int = 0
    weekday_num: int = 1
    source: str = "live"
    # Epoch seconds of the feed snapshot that produced this message. Kept next to
    # `last_reported` (also epoch seconds) because comparing epochs cannot go wrong
    # with the session time zone, while comparing timestamps can.
    feed_last_updated: int | None = None

    @classmethod
    def from_row(cls, row) -> "StationObservation | None":
        """Build from a Spark Row, a dict or anything with ``asDict()``.

        ``event_time`` is taken from ``feed_last_updated`` (epoch seconds) when the
        message has it, not from the timestamp column: PySpark turns a timestamp into
        a **naive local** datetime (``datetime.fromtimestamp``), so the timestamp
        column depends on the machine's time zone while the epoch does not.
        """
        data = row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)
        station_id = data.get("station_id")
        feed_last_updated = data.get("feed_last_updated")
        event_time = as_utc(feed_last_updated) if feed_last_updated not in (None, "") else as_utc(data.get("event_time"))
        if not station_id or event_time is None:
            return None
        return cls(
            station_id=str(station_id),
            system_id=str(data.get("system_id") or ""),
            bikes=as_int(data.get("num_bikes_available")),
            docks=as_int(data.get("num_docks_available")),
            bikes_disabled=as_int(data.get("num_bikes_disabled")),
            docks_disabled=as_int(data.get("num_docks_disabled")),
            ebikes=as_int(data.get("num_ebikes_available")),
            is_installed=as_bool(data.get("is_installed")),
            is_renting=as_bool(data.get("is_renting")),
            is_returning=as_bool(data.get("is_returning")),
            event_time=event_time,
            last_reported=as_utc(data.get("last_reported")),
            local_hour=as_int(data.get("local_hour")),
            weekday_num=as_int(data.get("weekday_num"), 1),
            source=str(data.get("source") or "live"),
            feed_last_updated=as_int(data["feed_last_updated"]) if data.get("feed_last_updated") else None,
        )

    @property
    def size(self) -> int:
        """Station size from the message, not ``station_information.capacity``."""
        return self.bikes + self.bikes_disabled + self.docks + self.docks_disabled

    @property
    def in_service(self) -> bool:
        """Installed and able to rent and return (the feed's own flags)."""
        return self.is_installed and self.is_renting and self.is_returning

    def is_stale(self, now: datetime, stale_after: timedelta = DEFAULT_STALE_AFTER) -> bool:
        """True when the station has not reported for ``stale_after`` (or never did).

        Uses ``feed_last_updated - last_reported`` when both are known: both are epoch
        seconds straight from the feed, so the answer does not depend on the Spark
        session time zone (``event_time`` is a timestamp and may be shifted by it).
        """
        if self.last_reported is None:
            return True
        age = self.age_seconds
        if age is not None:
            return age > stale_after.total_seconds()
        return now - self.last_reported > stale_after

    @property
    def age_seconds(self) -> int | None:
        """Seconds between the feed snapshot and the station's last report, or ``None``."""
        if self.last_reported is None or self.feed_last_updated is None:
            return None
        # `last_reported` is built from an epoch number, so its timestamp is exact
        # even when the session time zone shifted `event_time`.
        return int(self.feed_last_updated - int(self.last_reported.timestamp()))


def classify(
    observation: StationObservation,
    now: datetime | None = None,
    stale_after: timedelta = DEFAULT_STALE_AFTER,
) -> str:
    """Status of one station: ``ok``, ``empty``, ``full`` or ``out_of_service``."""
    now = now or observation.event_time
    if not observation.in_service or observation.is_stale(now, stale_after):
        return STATUS_OUT_OF_SERVICE
    if observation.bikes <= 0:
        return STATUS_EMPTY
    if observation.docks <= 0:
        return STATUS_FULL
    return STATUS_OK


def latest_per_station(observations: list[StationObservation]) -> list[StationObservation]:
    """Keep the newest observation per station (a micro-batch can hold several)."""
    newest: dict[str, StationObservation] = {}
    for obs in observations:
        current = newest.get(obs.station_id)
        if current is None or obs.event_time >= current.event_time:
            newest[obs.station_id] = obs
    return list(newest.values())


def status_counts(statuses: dict[str, str]) -> dict[str, int]:
    """``{status: count}`` for a mapping of ``{station_id: status}``."""
    counts = {status: 0 for status in STATUSES}
    for status in statuses.values():
        counts[status] = counts.get(status, 0) + 1
    return counts

