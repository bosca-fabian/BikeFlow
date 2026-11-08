"""Station metadata (name, coordinates) from the batch ``stations`` table.

The batch path owns that table and the dashboard can join it directly; putting
name and position in the Redis station hash simply keeps the live map to one
lookup. Nothing here is required: if the lake has no ``stations`` table yet, the
job runs with an empty mapping and the hash carries only the feed fields.
"""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)

# Columns the live map wants, in the order the dashboard uses them.
WANTED_COLUMNS = ("station_id", "name", "lat", "lon", "region_id", "capacity")


def load_station_metadata(spark, table: str = "stations") -> dict[str, dict]:
    """``{station_id: {name, lat, lon, region_id, capacity}}`` from the batch table.

    Returns ``{}`` when the table or a column is missing: the speed layer must start
    even before the batch path has run (weeks 5-6 of the timeline).
    """
    from bikeflow.lake import table_path

    path = table_path(table)
    try:
        frame = spark.read.format("delta").load(path)
    except Exception as exc:  # noqa: BLE001 - a missing table must not stop the job
        log.warning("station metadata: %s not readable (%s), continuing without it", path, exc)
        return {}

    available = [column for column in WANTED_COLUMNS if column in frame.columns]
    if "station_id" not in available:
        log.warning("station metadata: %s has no station_id column, ignoring it", path)
        return {}

    try:
        rows = frame.select(*available).collect()
    except Exception as exc:  # noqa: BLE001
        log.warning("station metadata: reading %s failed (%s), continuing without it", path, exc)
        return {}

    metadata: dict[str, dict] = {}
    for row in rows:
        data = row.asDict(recursive=True)
        station_id = data.pop("station_id", None)
        if station_id:
            metadata[str(station_id)] = {key: value for key, value in data.items() if value is not None}
    log.info("station metadata: %d stations from %s", len(metadata), path)
    return metadata
