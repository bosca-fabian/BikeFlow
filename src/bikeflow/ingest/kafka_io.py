"""Kafka helpers: topic creation, a JSON publisher and a broker wait."""

from __future__ import annotations

import logging
import time

from bikeflow.ingest.messages import encode

log = logging.getLogger(__name__)

# Admin connections only: silence librdkafka's per-retry connection errors
# (we report reachability ourselves).
QUIET = {"log_level": 0}


def producer_config(bootstrap: str, client_id: str) -> dict:
    return {
        "bootstrap.servers": bootstrap,
        "client.id": client_id,
        "acks": "all",
        "enable.idempotence": True,  # no duplicates from producer retries
        "compression.type": "zstd",
        "linger.ms": 100,
        "message.timeout.ms": 120_000,
    }


def wait_for_kafka(bootstrap: str, timeout_s: float = 120.0) -> None:
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": bootstrap, **QUIET})
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            md = admin.list_topics(timeout=5)
            log.info("Kafka reachable at %s (%d broker(s))", bootstrap, len(md.brokers))
            return
        except Exception as exc:  # noqa: BLE001 - librdkafka raises KafkaException
            if time.monotonic() > deadline:
                raise RuntimeError(f"Kafka not reachable at {bootstrap}: {exc}") from exc
            log.info("Waiting for Kafka at %s ...", bootstrap)
            time.sleep(3)


def ensure_topics(
    bootstrap: str, topics: list[str], partitions: int, retention_hours: int, replication: int = 1
) -> None:
    """Create topics if they do not exist yet (idempotent)."""
    from confluent_kafka import KafkaError, KafkaException
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap, **QUIET})
    existing = set(admin.list_topics(timeout=10).topics)
    new = [
        NewTopic(
            t,
            num_partitions=partitions,
            replication_factor=replication,
            config={"retention.ms": str(retention_hours * 3600 * 1000), "cleanup.policy": "delete"},
        )
        for t in topics
        if t not in existing
    ]
    if not new:
        return
    for topic, fut in admin.create_topics(new).items():
        try:
            fut.result()
            log.info("Created topic %s (%d partitions, %dh retention)", topic, partitions, retention_hours)
        except KafkaException as exc:
            if exc.args and exc.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                continue
            raise


class KafkaPublisher:
    """Publishes station messages as JSON, keyed by station_id.

    The key keeps every station's updates in one partition, in order.
    The Kafka message timestamp is left to Kafka (send time): replayed data
    carries old event times and would otherwise be deleted by retention.
    """

    def __init__(self, bootstrap: str, client_id: str = "bikeflow-producer"):
        from confluent_kafka import Producer

        self._producer = Producer(producer_config(bootstrap, client_id))
        self.failed = 0

    def _on_delivery(self, err, msg):
        if err is not None:
            self.failed += 1
            log.error("Delivery failed for key %s: %s", msg.key(), err)

    def publish(self, topic: str, messages: list[dict]) -> int:
        for m in messages:
            while True:
                try:
                    self._producer.produce(
                        topic, key=m["station_id"].encode(), value=encode(m), on_delivery=self._on_delivery
                    )
                    break
                except BufferError:
                    self._producer.poll(1.0)  # local queue full: let it drain
            self._producer.poll(0)
        remaining = self._producer.flush(60)
        if remaining:
            log.warning("%d message(s) still in flight after flush", remaining)
        return len(messages)

    def close(self) -> None:
        self._producer.flush(30)
