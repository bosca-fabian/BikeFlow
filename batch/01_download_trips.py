"""Download Citi Bike monthly trip zips and extract the CSVs into
data/raw/trips/year=YYYY/month=MM/.

Usage:
    python batch/01_download_trips.py --month 2025-09     # one month, to test
    python batch/01_download_trips.py                     # all months 2024-01..2026-09

The script reads the bucket listing instead of guessing file names, skips
Jersey City files (JC-...), handles zips inside zips, and skips files that are
already downloaded, so it can be stopped and restarted.
"""
import re
import shutil
import tempfile
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

import requests

from common import RAW, month_args

BUCKET = "https://s3.amazonaws.com/tripdata/"
NS = {"s": "http://s3.amazonaws.com/doc/2006-03-01/"}
MONTHLY = re.compile(r"^(\d{4})(\d{2})-citibike-tripdata(\.csv)?\.zip$")
YEARLY = re.compile(r"^(\d{4})-citibike-tripdata\.zip$")


def list_bucket():
    keys, token = [], None
    while True:
        params = {"list-type": "2"}
        if token:
            params["continuation-token"] = token
        r = requests.get(BUCKET, params=params, timeout=60)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        for c in root.findall("s:Contents", NS):
            keys.append((c.findtext("s:Key", namespaces=NS), int(c.findtext("s:Size", namespaces=NS))))
        if root.findtext("s:IsTruncated", namespaces=NS) == "true":
            token = root.findtext("s:NextContinuationToken", namespaces=NS)
        else:
            return keys


def pick_files(keys, months):
    """Return the zip names to download for the requested months.
    Prefer monthly zips; fall back to a yearly zip if a month has none."""
    wanted = set(months)
    by_month, by_year = {}, {}
    for key, size in keys:
        if m := MONTHLY.match(key):
            by_month[(int(m[1]), int(m[2]))] = (key, size)
        elif m := YEARLY.match(key):
            by_year[int(m[1])] = (key, size)
    chosen, missing = {}, []
    for ym in sorted(wanted):
        if ym in by_month:
            chosen[by_month[ym][0]] = by_month[ym][1]
        elif ym[0] in by_year:
            chosen[by_year[ym[0]][0]] = by_year[ym[0]][1]
        else:
            missing.append(ym)
    return chosen, missing


def download(key, size, dest: Path):
    if dest.exists() and dest.stat().st_size == size:
        print(f"  already downloaded: {key}")
        return
    print(f"  downloading {key} ({size / 1e9:.2f} GB)")
    tmp = dest.with_suffix(".part")
    with requests.get(BUCKET + key, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(tmp, "wb") as f:
            shutil.copyfileobj(r.raw, f, length=8 * 1024 * 1024)
    tmp.rename(dest)


def extract_csvs(zip_path, dest_root: Path, wanted: set):
    """Extract CSVs whose name starts with YYYYMM of a wanted month. Recurses into nested zips."""
    written = 0
    with zipfile.ZipFile(zip_path) as z:
        for info in z.infolist():
            base = Path(info.filename).name
            if info.is_dir() or info.filename.startswith("__MACOSX") or base.startswith("."):
                continue
            if base.lower().endswith(".zip"):
                with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
                    with z.open(info) as src:
                        shutil.copyfileobj(src, tmp)
                try:
                    written += extract_csvs(tmp.name, dest_root, wanted)
                finally:
                    Path(tmp.name).unlink()
            elif base.lower().endswith(".csv"):
                m = re.match(r"(\d{4})(\d{2})", base)
                if not m or (int(m[1]), int(m[2])) not in wanted:
                    continue
                out = dest_root / f"year={m[1]}" / f"month={m[2]}" / base
                if out.exists() and out.stat().st_size == info.file_size:
                    continue
                out.parent.mkdir(parents=True, exist_ok=True)
                with z.open(info) as src, open(out, "wb") as dst:
                    shutil.copyfileobj(src, dst, length=8 * 1024 * 1024)
                written += 1
    return written


def main():
    months = month_args(__doc__)
    zips_dir = RAW / "trip_zips"
    csv_root = RAW / "trips"
    zips_dir.mkdir(parents=True, exist_ok=True)

    chosen, missing = pick_files(list_bucket(), months)
    if missing:
        print("WARNING: no file found in the bucket for:", ", ".join(f"{y}-{m:02d}" for y, m in missing))

    for key, size in chosen.items():
        dest = zips_dir / key
        download(key, size, dest)
        n = extract_csvs(dest, csv_root, set(months))
        print(f"  extracted {n} new CSV file(s) from {key}")
    print("Done. CSVs are in", csv_root)


if __name__ == "__main__":
    main()
