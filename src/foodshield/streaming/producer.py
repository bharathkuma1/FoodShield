"""Paced Kafka replay producer (decision D12).

Sends replay records in ``(available_at, record_id)`` order. Before
``measure_from`` (an available_at date) it sends as fast as possible, so the
consumer can build its history (12 months of prices, 10 years of weather
climatology). From ``measure_from`` on it sends at ``rate`` events per second
(0 = as fast as possible); only that window is used for throughput and latency.

``duplicate_fraction`` re-sends a deterministic random share of records right
after the original, to test the consumer's de-duplication by ``record_id``.

Delivery: acks=all with retries (one broker), so a record can be written more
than once if a retry follows a lost acknowledgement. This is at-least-once
delivery; the consumer removes duplicates.
"""

from __future__ import annotations

import random
import threading
import time
from collections import Counter
from typing import Any, Iterable

from foodshield.streaming.records import kafka_key, to_json


class ReplayProducer:
    def __init__(self, bootstrap: str, topic: str):
        from kafka import KafkaProducer

        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers=bootstrap, acks="all", retries=5, linger_ms=5,
                                      batch_size=256 * 1024, max_in_flight_requests_per_connection=1,
                                      client_id="foodshield-replay")
        self.sent = 0                  # all messages, duplicates included (read by the orchestrator)
        self.unique = Counter()        # by record_type
        self.duplicates = 0
        self.window: dict[str, Any] = {}
        self.stop_event = threading.Event()

    def run(self, records: Iterable[dict[str, Any]], measure_from: str | None, rate: float,
            duplicate_fraction: float = 0.0, seed: int = 20261107) -> dict[str, Any]:
        rng = random.Random(seed)
        start = time.time()
        window_start = None
        window_sent = 0
        boundary = f"{measure_from}T00:00:00Z" if measure_from else ""
        for record in records:
            if self.stop_event.is_set():
                break
            in_window = record["available_at"] >= boundary
            if in_window and window_start is None:
                self.producer.flush()
                window_start = time.time()
                self.window["first_send_epoch"] = window_start
                self.window["first_available_at"] = record["available_at"]
            if in_window and rate > 0:
                due = window_start + window_sent / rate
                delay = due - time.time()
                if delay > 0:
                    time.sleep(delay)
            payload, key = to_json(record), kafka_key(record).encode("utf-8")
            self.producer.send(self.topic, key=key, value=payload)
            self.sent += 1
            self.unique[record["record_type"]] += 1
            if in_window:
                window_sent += 1
            if duplicate_fraction and rng.random() < duplicate_fraction:
                self.producer.send(self.topic, key=key, value=payload)
                self.sent += 1
                self.duplicates += 1
                if in_window:
                    window_sent += 1
        self.producer.flush()
        end = time.time()
        self.producer.close()
        stats = {"messages_sent": self.sent, "unique_records": dict(self.unique),
                 "unique_total": sum(self.unique.values()), "duplicates_sent": self.duplicates,
                 "seconds": round(end - start, 3), "overall_events_per_s": round(self.sent / max(end - start, 1e-9), 1),
                 "offered_rate_events_per_s": rate or None}
        if window_start is not None:
            stats["measured_window"] = {**self.window, "messages": window_sent, "seconds": round(end - window_start, 3),
                                        "achieved_events_per_s": round(window_sent / max(end - window_start, 1e-9), 1),
                                        "last_send_epoch": end}
        return stats
