# BikeFlow – batch path

Owner: Vlad (batch data lead).

Loads the Citi Bike trip history (2024-01 to 2026-09), New York hourly weather and
station information into the shared data lake, cleans the trips and maps the trip
station codes to the station IDs of the live feed. The batch processing jobs
(demand profiles, aggregates) and the dashboard read the tables written here.

## What it writes

Everything goes under the shared data folder `BIKEFLOW_DATA` (default `./data`,
`/app/data` in Docker), next to the ingestion tables.

| Path | Content |
|---|---|
| `data/raw/trip_zips/`, `data/raw/trips/year=YYYY/month=MM/` | downloaded zips and the extracted CSVs |
| `data/raw/weather/weather_YYYY.json` | Open-Meteo responses, one per year |
| `data/raw/station_information/*.json` | dated GBFS station_information snapshots |
| `data/lake/trips_raw` | trip CSVs as strings, + `source_file`; partitioned by `year`, `month` |
| `data/lake/trips_clean` | typed, cleaned, deduplicated trips; partitioned by `year`, `month` |
| `data/lake/weather_hourly` | one row per New York hour |
| `data/lake/station_snapshots` | every station from every saved snapshot |
| `data/lake/stations` | latest snapshot only (for the map) |
| `data/lake/station_id_map` | trip station code → live-feed station UUID |
| `data/reports/*.csv` | load counts, data-quality and mapping reports |

One writer per table: only these scripts write the tables above.

### `trips_clean` columns

The 13 original columns, typed (`ride_id`, `rideable_type`, `started_at`, `ended_at`,
`start_station_name`, `start_station_id`, `end_station_name`, `end_station_id`,
`start_lat`, `start_lng`, `end_lat`, `end_lng`, `member_casual`), plus:

- `duration_s`, `duration_min`
- `start_date`, `start_hour`, `start_hour_ts` (start of the hour, join key for weather)
- `weekday`, `weekday_num` (Monday = 1 … Sunday = 7), `is_weekend`
- `year`, `month` (partition columns)

Trip times are New York local time. The scripts run Spark in New York time; `get_spark()`
from `bikeflow.spark` runs in UTC. Compare instants, not formatted strings (see
`docs/DATA_CONTRACT.md`).

### Joins

- trips → weather: `trips_clean.start_hour_ts = weather_hourly.hour_ts`
- trips → live station UUID: `trips_clean.start_station_id = station_id_map.trip_station_code`
  (same for `end_station_id`)
- live feed → trips: `station_status_raw.station_id = station_id_map.station_uuid`

## Cleaning rules (`05_clean_trips.py`)

A trip is dropped if it fails at least one rule:

- missing start or end time;
- shorter than 60 s or longer than 24 h;
- missing start or end station;
- missing coordinates, or coordinates outside the NYC area box;
- unknown bike type or rider type.

Duplicate `ride_id`s are then removed. `data/reports/dq_trips.csv` has the count per rule
and per month (a trip can fail more than one rule).

## Results of the first full run (2024-01 to 2026-09)

| | |
|---|---|
| Trips in the files | 125,217,844 |
| Trips kept after cleaning | 124,806,802 (99.67%) |
| Trips dropped | 411,042 (mostly no start/end station, and trips over 24 h) |
| Duplicates / trips under 60 s | 0 / 0 (Citi Bike already removes trips under 60 s) |
| Station codes in the trips | 2,727 |
| Codes matched to the live feed | 96.11% (2,484 by short_name, 137 by name, 106 unmatched) |
| Trip endpoints matched | 97.11% |

## How to run

### In Docker (the team environment)

```
docker compose run --rm spark python batch/01_download_trips.py --month 2025-09
```

Same for every step below. Delta jars are already in the image.

### In a local venv

Needs Python 3.11 or 3.12 (not 3.13) and Java 17.

```
pip install -r requirements.txt
```

The Delta jars: put `delta-spark_2.12-3.2.1.jar` and `delta-storage-3.2.1.jar` in a
`jars/` folder at the repo root. The scripts use them if they are there; otherwise Spark
downloads them from Maven on the first run, which can hang on Windows.

```
mkdir jars
curl -L -o jars/delta-spark_2.12-3.2.1.jar https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.1/delta-spark_2.12-3.2.1.jar
curl -L -o jars/delta-storage-3.2.1.jar https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.1/delta-storage-3.2.1.jar
```

On Windows, Spark also needs `winutils.exe` and `hadoop.dll` (Hadoop 3.3.x) in
`C:\hadoop\bin`, with `HADOOP_HOME=C:\hadoop` and `C:\hadoop\bin` on the Path.
Keep the project in a path without spaces and outside OneDrive.

### Steps

Test on one month first, then run without `--month` for all months:

```
python batch/01_download_trips.py --month 2025-09
python batch/02_download_weather.py
python batch/03_download_station_info.py
python batch/04_load_trips_raw.py --month 2025-09
python batch/05_clean_trips.py --month 2025-09
python batch/06_load_weather_and_stations.py
python batch/07_station_mapping.py
```

Full run on a laptop: about 1–2 h to download (~24 GB of zips, ~45 GB in `data/raw` after
extracting) and 2–3 h for steps 04–07 (~12 GB in `data/lake`).

Every month can be re-run safely: it replaces only that month in the Delta tables.
Run `03_download_station_info.py` every few days: older snapshots raise the match rate.

### Notes

- At the end of each script, Spark on Windows prints `Failed to delete ... spark-...` for
  its temp folder. It is harmless: the data is written.
- Settings: `BIKEFLOW_DATA` (data folder), `SPARK_DRIVER_MEMORY` (default `4g`),
  `SPARK_MASTER` (default `local[*]`).
