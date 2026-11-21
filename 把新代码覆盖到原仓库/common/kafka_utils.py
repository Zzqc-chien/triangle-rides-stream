"""Shared Kafka consumer loop: poll, collect a small batch, hand it to a handler, commit offsets.

Offsets are committed to Kafka only after the handler has written the batch to Postgres, so a
crash re-delivers the batch instead of losing it (at-least-once). What a re-delivered batch does
depends on the handler: row inserts are keyed (ON CONFLICT DO NOTHING), and the aggregating
consumer stores its own offsets in Postgres so it can skip messages it has already counted.
"""

import json
import signal
import threading
import time
from typing import Callable

from confluent_kafka import Consumer, KafkaException, Producer

from common.config import BOOTSTRAP

_running = True


def _stop(*_):
    global _running
    _running = False


def make_producer() -> Producer:
    return Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 50})


def run_consumer(
    group_id: str,
    topics: list[str],
    handle_batch: Callable[[list[dict]], None],
    max_batch: int = 200,
    max_wait_s: float = 1.0,
    on_idle: Callable[[], None] | None = None,
    with_positions: bool = False,
) -> None:
    """with_positions=True hands the handler (event, topic, partition, offset) tuples instead of events."""
    if threading.current_thread() is threading.main_thread():
        signal.signal(signal.SIGINT, _stop)
        signal.signal(signal.SIGTERM, _stop)

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe(topics)
    print(f"[{group_id}] consuming {topics} from {BOOTSTRAP}")

    batch: list[dict] = []
    batch_started = time.time()
    total = 0
    try:
        while _running:
            msg = consumer.poll(0.2)
            if msg is not None:
                if msg.error():
                    print(f"[{group_id}] kafka error: {msg.error()}")
                    continue
                event = json.loads(msg.value())
                batch.append((event, msg.topic(), msg.partition(), msg.offset()) if with_positions else event)

            if batch and (len(batch) >= max_batch or time.time() - batch_started >= max_wait_s):
                handle_batch(batch)
                try:
                    consumer.commit(asynchronous=False)
                except KafkaException as exc:          # e.g. nothing to commit after a rebalance
                    print(f"[{group_id}] commit skipped: {exc}")
                total += len(batch)
                batch = []
                batch_started = time.time()
            elif not batch:
                batch_started = time.time()
                if on_idle:
                    on_idle()
    finally:
        if batch:
            handle_batch(batch)
            consumer.commit(asynchronous=False)
        consumer.close()
        print(f"[{group_id}] stopped after {total} messages")
