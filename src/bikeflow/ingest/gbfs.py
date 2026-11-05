"""Minimal GBFS client: auto-discovery plus fetching one feed.

Handles GBFS v1/v2 (``data.<lang>.feeds``) and v3 (``data.feeds``) discovery
files, so another city only needs a different ``GBFS_DISCOVERY_URL``.
"""

from __future__ import annotations

import logging
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)

USER_AGENT = "BikeFlow-student-project/0.1 (+https://gbfs.org)"


def _session() -> requests.Session:
    retry = Retry(
        total=3,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    s = requests.Session()
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    s.headers["User-Agent"] = USER_AGENT
    return s


def parse_discovery(doc: dict, language: str = "en") -> dict[str, str]:
    """Return {feed_name: url} from a gbfs.json discovery document."""
    data = doc.get("data", {})
    if isinstance(data.get("feeds"), list):  # GBFS v3
        feeds = data["feeds"]
    else:  # GBFS v1/v2: one entry per language
        if language in data:
            feeds = data[language].get("feeds", [])
        elif data:
            first = next(iter(data))
            log.warning("Language %r not in discovery file, using %r", language, first)
            feeds = data[first].get("feeds", [])
        else:
            feeds = []
    return {f["name"]: f["url"] for f in feeds if "name" in f and "url" in f}


def to_epoch_seconds(value) -> int | None:
    """Accept epoch seconds, epoch milliseconds or an ISO-8601 string."""
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        v = float(value)
        return int(v / 1000) if v > 1e11 else int(v)
    if isinstance(value, str):
        try:
            return to_epoch_seconds(float(value))
        except ValueError:
            pass
        try:
            return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())
        except ValueError:
            return None
    return None


class GbfsClient:
    def __init__(self, discovery_url: str, language: str = "en", timeout: float = 20.0, session=None):
        self.discovery_url = discovery_url
        self.language = language
        self.timeout = timeout
        self.session = session or _session()
        self._feeds: dict[str, str] | None = None

    def _get_json(self, url: str) -> dict:
        resp = self.session.get(url, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def feeds(self, refresh: bool = False) -> dict[str, str]:
        if self._feeds is None or refresh:
            self._feeds = parse_discovery(self._get_json(self.discovery_url), self.language)
            log.info("GBFS discovery: %d feeds (%s)", len(self._feeds), ", ".join(sorted(self._feeds)))
        return self._feeds

    def fetch(self, feed_name: str) -> dict:
        """Fetch a feed by name; re-runs discovery once if the URL stopped working."""
        url = self.feeds().get(feed_name)
        if url is None:
            url = self.feeds(refresh=True).get(feed_name)
            if url is None:
                raise KeyError(f"Feed {feed_name!r} not listed in {self.discovery_url}")
        try:
            return self._get_json(url)
        except requests.HTTPError:
            new_url = self.feeds(refresh=True).get(feed_name)
            if new_url and new_url != url:
                return self._get_json(new_url)
            raise

    def fetch_station_status(self) -> dict:
        return self.fetch("station_status")
