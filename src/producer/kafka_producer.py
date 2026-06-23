"""
KafkaEventProducer - publishes ChangeEvents to a Kafka topic.

Fault tolerance:
- `acks=all` + `retries` configured via config.yaml ensure broker-side durability.
- If the broker is unreachable, events are spilled to a local on-disk
  dead-letter queue (JSONL file) instead of being dropped, and a background
  thread periodically retries flushing them back into Kafka.
"""

import json
import os
import threading
import time
from pathlib import Path
from typing import List

from kafka import KafkaProducer
from kafka.errors import KafkaError

from src.models.event import ChangeEvent


class KafkaEventProducer:
    def __init__(self, kafka_config: dict, dlq_path: str = "./logs/producer_dlq.jsonl", logger=None):
        self.config = kafka_config
        self.topic = kafka_config["topic"]
        self.logger = logger
        self.dlq_path = Path(dlq_path)
        os.makedirs(self.dlq_path.parent, exist_ok=True)

        self._producer = self._build_producer()
        self._dlq_lock = threading.Lock()
        self._stop_flag = threading.Event()
        self._retry_thread = threading.Thread(target=self._retry_dlq_loop, daemon=True)
        self._retry_thread.start()

    def _build_producer(self) -> KafkaProducer:
        return KafkaProducer(
            bootstrap_servers=self.config["bootstrap_servers"].split(","),
            client_id=self.config.get("client_id", "fc-stream-producer"),
            acks=self.config.get("acks", "all"),
            retries=self.config.get("retries", 5),
            linger_ms=self.config.get("linger_ms", 10),
            value_serializer=lambda v: v.encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )

    def publish(self, events: List[ChangeEvent]):
        for event in events:
            self._publish_one(event)

    def _publish_one(self, event: ChangeEvent):
        payload = event.to_json()
        try:
            future = self._producer.send(self.topic, key=event.event_id, value=payload)
            future.add_callback(self._on_success, event)
            future.add_errback(self._on_failure, event, payload)
        except KafkaError as e:
            if self.logger:
                self.logger.error(f"Kafka send raised immediately: {e}. Spilling to DLQ.")
            self._spill_to_dlq(payload)

    def _on_success(self, event: ChangeEvent, record_metadata):
        if self.logger:
            self.logger.info(
                f"Published {event.event_type} event {event.event_id} "
                f"-> partition={record_metadata.partition} offset={record_metadata.offset}"
            )

    def _on_failure(self, event: ChangeEvent, payload: str, exc):
        if self.logger:
            self.logger.error(f"Failed to publish event {event.event_id}: {exc}. Spilling to DLQ.")
        self._spill_to_dlq(payload)

    def _spill_to_dlq(self, payload: str):
        with self._dlq_lock:
            with open(self.dlq_path, "a", encoding="utf-8") as f:
                f.write(payload + "\n")

    def _retry_dlq_loop(self, interval_seconds: int = 30):
        """Periodically attempt to re-publish anything sitting in the DLQ."""
        while not self._stop_flag.wait(interval_seconds):
            if not self.dlq_path.exists() or self.dlq_path.stat().st_size == 0:
                continue
            with self._dlq_lock:
                lines = self.dlq_path.read_text(encoding="utf-8").splitlines()
                self.dlq_path.write_text("", encoding="utf-8")
            still_failing = []
            for line in lines:
                try:
                    self._producer.send(self.topic, value=line.encode("utf-8"))
                except KafkaError:
                    still_failing.append(line)
            if still_failing:
                with self._dlq_lock:
                    with open(self.dlq_path, "a", encoding="utf-8") as f:
                        f.write("\n".join(still_failing) + "\n")
            elif self.logger and lines:
                self.logger.info(f"Flushed {len(lines)} previously-failed event(s) from DLQ.")

    def flush(self, timeout: int = 10):
        self._producer.flush(timeout=timeout)

    def close(self):
        self._stop_flag.set()
        self._producer.flush(timeout=10)
        self._producer.close(timeout=10)
