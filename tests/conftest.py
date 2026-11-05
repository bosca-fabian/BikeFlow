import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))


def station(i: int, bikes: int = 5, docks: int = 10, ebikes: int = 1, renting: int = 1) -> dict:
    """A station record shaped like Citi Bike's GBFS 2.x station_status."""
    return {
        "station_id": f"00000000-0000-4000-8000-{i:012d}",
        "legacy_id": str(1000 + i),
        "num_bikes_available": bikes,
        "num_ebikes_available": ebikes,
        "num_bikes_disabled": 0,
        "num_docks_available": docks,
        "num_docks_disabled": 0,
        "is_installed": 1,
        "is_renting": renting,
        "is_returning": 1,
        "last_reported": 1_762_070_000 + i,
        "eightd_has_available_keys": False,
    }


def feed(ts: int, stations: list[dict]) -> dict:
    return {"last_updated": ts, "ttl": 5, "version": "2.3", "data": {"stations": stations}}


@pytest.fixture
def make_station():
    return station


@pytest.fixture
def make_feed():
    return feed


@pytest.fixture(autouse=True)
def isolated_env(tmp_path, monkeypatch):
    """Point lake and recordings at a temp dir for every test."""
    monkeypatch.setenv("LAKE_PATH", str(tmp_path / "lake"))
    monkeypatch.setenv("RECORDINGS_PATH", str(tmp_path / "recordings"))
    from bikeflow import config

    config.reload_settings()
    yield
    for var in ("LAKE_PATH", "RECORDINGS_PATH"):
        os.environ.pop(var, None)
    config.reload_settings()
