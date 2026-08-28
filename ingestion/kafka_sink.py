"""Kafka producer wrapper (and a stdout sink so the pollers run without Kafka).

confluent-kafka is imported lazily: `--dry-run` has to work on a laptop that has
never installed librdkafka.
"""
from __future__ import annotations

import logging
import sys
from collections.abc import Sequence
from typing import Protocol

from . import config
from .models import Envelope

log = logging.getLogger(__name__)


class Sink(Protocol):
    def send(self, topic: str, events: Sequence[Envelope]) -> int: ...
    def flush(self) -> None: ...
    def close(self) -> None: ...


class StdoutSink:
    """Prints what would have been produced. Used by --dry-run and the tests."""

    def send(self, topic: str, events: Sequence[Envelope]) -> int:
        for event in events:
            sys.stdout.write(f"{topic} | {event.key()} | {event.to_json().decode()}\n")
        sys.stdout.flush()
        return len(events)

    def flush(self) -> None:
        sys.stdout.flush()

    def close(self) -> None:
        self.flush()


class KafkaSink:
    def __init__(self, bootstrap: str = config.KAFKA_BOOTSTRAP, client_id: str = "gdep-poller") -> None:
        from confluent_kafka import Producer  # imported here on purpose

        self._delivered = 0
        self._failed = 0
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "client.id": client_id,
                "acks": "all",
                "enable.idempotence": True,
                "compression.type": "lz4",
                "linger.ms": 50,
                "retries": 10,
                "retry.backoff.ms": 250,
                "delivery.timeout.ms": 60_000,
            }
        )
        log.info("kafka sink ready: %s", bootstrap)

    def _on_delivery(self, err, msg) -> None:  # noqa: ANN001 - confluent callback signature
        if err is not None:
            self._failed += 1
            log.error("delivery failed for %s: %s", msg.topic() if msg else "?", err)
        else:
            self._delivered += 1

    def send(self, topic: str, events: Sequence[Envelope]) -> int:
        for event in events:
            self._producer.produce(
                topic=topic,
                key=event.key().encode("utf-8"),
                value=event.to_json(),
                on_delivery=self._on_delivery,
            )
        self._producer.poll(0)
        return len(events)

    def flush(self) -> None:
        remaining = self._producer.flush(15.0)
        if remaining:
            log.warning("%d message(s) still queued after flush", remaining)

    def close(self) -> None:
        self.flush()
        log.info("kafka sink closed (delivered=%d failed=%d)", self._delivered, self._failed)


def build_sink(dry_run: bool, client_id: str) -> Sink:
    return StdoutSink() if dry_run else KafkaSink(client_id=client_id)
