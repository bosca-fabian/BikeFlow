"""Download hourly New York weather from the Open-Meteo archive, one request per year.
Saves data/raw/weather/weather_YYYY.json.

Usage:
    python batch/02_download_weather.py
"""
import calendar
import json
from datetime import date

import requests

from common import FIRST_MONTH, LAST_MONTH, NYC_LAT, NYC_LON, NYC_TZ, RAW

URL = "https://archive-api.open-meteo.com/v1/archive"
VARIABLES = "temperature_2m,precipitation,rain,snowfall,wind_speed_10m"


def main():
    out_dir = RAW / "weather"
    out_dir.mkdir(parents=True, exist_ok=True)
    fy, fm = map(int, FIRST_MONTH.split("-"))
    ly, lm = map(int, LAST_MONTH.split("-"))
    last_day = date(ly, lm, calendar.monthrange(ly, lm)[1])

    for year in range(fy, ly + 1):
        start = date(year, fm, 1) if year == fy else date(year, 1, 1)
        end = min(date(year, 12, 31), last_day)
        params = {
            "latitude": NYC_LAT, "longitude": NYC_LON,
            "start_date": start.isoformat(), "end_date": end.isoformat(),
            "hourly": VARIABLES, "timezone": NYC_TZ,
        }
        print(f"  weather {start} .. {end}")
        r = requests.get(URL, params=params, timeout=120)
        r.raise_for_status()
        data = r.json()
        n = len(data["hourly"]["time"])
        (out_dir / f"weather_{year}.json").write_text(json.dumps(data))
        print(f"    {n} hourly rows saved")


if __name__ == "__main__":
    main()
