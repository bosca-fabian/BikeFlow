"""CLI wiring for produce with Kafka replaced by an in-memory publisher."""

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from bikeflow import config
from bikeflow.ingest import cli, kafka_io


class Captured:
    instances = []

    def __init__(self, bootstrap, client_id):
        self.sent = []
        Captured.instances.append(self)

    def publish(self, topic, messages):
        self.sent.append((topic, messages))
        return len(messages)

    def close(self):
        pass


@pytest.fixture
def fake_kafka(monkeypatch):
    Captured.instances = []
    monkeypatch.setattr(kafka_io, "KafkaPublisher", Captured)
    monkeypatch.setattr(kafka_io, "wait_for_kafka", lambda *a, **k: None)
    monkeypatch.setattr(kafka_io, "ensure_topics", lambda *a, **k: None)
    return Captured


def test_produce_command(fake_kafka, monkeypatch, make_feed, make_station, tmp_path):
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_GET(self):
            base = f"http://127.0.0.1:{self.server.server_port}"
            body = (
                {"data": {"en": {"feeds": [{"name": "station_status", "url": f"{base}/ss.json"}]}}}
                if self.path == "/gbfs.json"
                else make_feed(1_762_070_000, [make_station(i) for i in range(5)])
            )
            data = json.dumps(body).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    for var in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("NO_PROXY", "127.0.0.1")
    monkeypatch.setenv("GBFS_DISCOVERY_URL", f"http://127.0.0.1:{srv.server_port}/gbfs.json")
    monkeypatch.setenv("POLL_SECONDS", "0")
    config.reload_settings()
    try:
        assert cli.main(["produce", "--max-polls", "2"]) == 0
    finally:
        srv.shutdown()
    (pub,) = fake_kafka.instances
    assert [(t, len(m)) for t, m in pub.sent] == [("station_status", 5)]  # 2nd poll: same feed -> nothing
    assert list((tmp_path / "recordings" / "station_status").glob("date=*/hour=*.jsonl.gz"))
