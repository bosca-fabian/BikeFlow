"""Save today's Citi Bike station_information feed (GBFS) as a dated snapshot:
data/raw/station_information/station_information_YYYY-MM-DD.json

Run it once a day (or whenever you remember). Old snapshots help the station
ID mapping, because stations that closed disappear from the live feed.
"""
import json
from datetime import date

import requests

from common import RAW

GBFS_INDEX = "https://gbfs.citibikenyc.com/gbfs/gbfs.json"


def find_feed(index: dict, name: str) -> str:
    data = index["data"]
    feeds = data["feeds"] if "feeds" in data else data.get("en", next(iter(data.values())))["feeds"]
    for f in feeds:
        if f["name"] == name:
            return f["url"]
    raise KeyError(f"feed {name} not found in GBFS index")


def main():
    index = requests.get(GBFS_INDEX, timeout=30).json()
    url = find_feed(index, "station_information")
    info = requests.get(url, timeout=30).json()
    out_dir = RAW / "station_information"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"station_information_{date.today().isoformat()}.json"
    out.write_text(json.dumps(info))
    print(f"Saved {len(info['data']['stations'])} stations to {out}")


if __name__ == "__main__":
    main()
