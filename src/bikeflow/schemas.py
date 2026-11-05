"""Single source of truth for the `station_status` message and raw-table schemas.

See docs/DATA_CONTRACT.md for the human-readable version.
"""

from __future__ import annotations

SCHEMA_VERSION = 1

# (field name, logical type, nullable, description)
# Logical types: string, long, bool, timestamp (ISO-8601 UTC string in Kafka, real timestamp in the lake),
# date ('YYYY-MM-DD' string in Kafka, real date in the lake)
MESSAGE_FIELDS: list[tuple[str, str, bool, str]] = [
    ("schema_version", "long", False, "Message format version (currently 1)"),
    ("system_id", "string", False, "Bike-share system, e.g. citibike_nyc"),
    ("source", "string", False, "'live' (real feed) or 'replay' (recorded data)"),
    ("reason", "string", False, "'initial', 'changed' or 'heartbeat'"),
    ("station_id", "string", False, "GBFS station_id (UUID for Citi Bike); also the Kafka key"),
    ("legacy_id", "string", True, "GBFS legacy_id if the feed provides it"),
    ("num_bikes_available", "long", False, "Bikes available to rent (classic + e-bikes)"),
    ("num_ebikes_available", "long", True, "E-bikes available to rent"),
    ("num_bikes_disabled", "long", True, "Broken/reserved bikes docked at the station"),
    ("num_docks_available", "long", False, "Free docks to return a bike"),
    ("num_docks_disabled", "long", True, "Docks out of service"),
    ("is_installed", "bool", False, "Station is installed"),
    ("is_renting", "bool", False, "Station allows renting"),
    ("is_returning", "bool", False, "Station accepts returns"),
    ("last_reported", "long", True, "Epoch seconds when the station last reported (from the feed)"),
    ("feed_last_updated", "long", False, "Epoch seconds of the feed snapshot (event time)"),
    ("event_time", "timestamp", False, "feed_last_updated as UTC timestamp: use this for windows/watermarks"),
    # New York local time of event_time, defined exactly like the batch tables (trips_clean, weather_hourly),
    # so live rows join demand profiles and weather without any time-zone code.
    (
        "hour_ts",
        "timestamp",
        False,
        "Start of the hour containing event_time (= weather_hourly.hour_ts, trips_clean.start_hour_ts)",
    ),
    ("local_date", "date", False, "New York date of event_time"),
    ("local_hour", "long", False, "New York hour of event_time, 0-23 (= trips_clean.start_hour)"),
    ("weekday_num", "long", False, "New York weekday, Monday=1 ... Sunday=7 (= trips_clean.weekday_num)"),
    ("is_weekend", "bool", False, "weekday_num >= 6 (= trips_clean.is_weekend)"),
    ("ingested_at", "timestamp", False, "When the producer sent the message (UTC)"),
    ("original_event_time", "timestamp", True, "Replay with time shift only: the recorded event time"),
]

MESSAGE_FIELD_NAMES = [f[0] for f in MESSAGE_FIELDS]

# Extra columns the lake sink adds to every row of station_status_raw.
SINK_EXTRA_FIELDS: list[tuple[str, str, bool, str]] = [
    ("event_date", "date", False, "UTC date of event_time (partition column)"),
    ("kafka_topic", "string", False, "Topic the message was read from"),
    ("kafka_partition", "long", False, "Kafka partition"),
    ("kafka_offset", "long", False, "Kafka offset (topic, partition, offset) is unique"),
    ("kafka_timestamp", "timestamp", True, "Kafka broker/producer timestamp"),
    ("raw_value", "string", False, "The original message bytes as UTF-8 text"),
]


def spark_message_ddl() -> str:
    """DDL string for ``from_json`` on the Kafka value (timestamps kept as strings)."""
    mapping = {"string": "STRING", "long": "BIGINT", "bool": "BOOLEAN", "timestamp": "STRING", "date": "STRING"}
    return ", ".join(f"{name} {mapping[typ]}" for name, typ, _, _ in MESSAGE_FIELDS)


def arrow_raw_schema():
    """pyarrow schema of the station_status_raw Delta table."""
    import pyarrow as pa

    mapping = {
        "string": pa.string(),
        "long": pa.int64(),
        "bool": pa.bool_(),
        "timestamp": pa.timestamp("us", tz="UTC"),
        "date": pa.date32(),
    }
    return pa.schema(
        [pa.field(name, mapping[typ], nullable=True) for name, typ, _, _ in MESSAGE_FIELDS + SINK_EXTRA_FIELDS]
    )
