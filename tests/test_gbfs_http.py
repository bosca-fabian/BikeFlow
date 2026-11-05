"""GbfsClient against a real local HTTP server (no internet needed)."""

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from bikeflow.ingest.gbfs import GbfsClient


@pytest.fixture
def gbfs_server(make_feed, make_station):
    state = {"hits": 0}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            base = f"http://127.0.0.1:{self.server.server_port}"
            if self.path == "/gbfs.json":
                body = {
                    "data": {"en": {"feeds": [{"name": "station_status", "url": f"{base}/en/station_status.json"}]}}
                }
            elif self.path == "/en/station_status.json":
                state["hits"] += 1
                body = make_feed(1_762_070_000, [make_station(i) for i in range(3)])
            else:
                self.send_response(404)
                self.end_headers()
                return
            data = json.dumps(body).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    srv = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{srv.server_port}", state
    srv.shutdown()


def test_discovery_and_fetch(gbfs_server, monkeypatch):
    for var in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
        monkeypatch.delenv(var, raising=False)
    base, state = gbfs_server
    client = GbfsClient(f"{base}/gbfs.json")
    client.session.trust_env = False
    feed = client.fetch_station_status()
    assert len(feed["data"]["stations"]) == 3
    client.fetch_station_status()
    assert state["hits"] == 2
