"""Shared Kafka consumer loop: poll, collect a small batch, hand it to a handler, commit offsets.

Offsets are committed only after the handler has written the batch to Postgres, so a crash
re-delivers the batch instead of losing it (at-least-once). The tables use primary keys with
ON CONFLICT, so a re-delivered event does not create a duplicate row.
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
) -> None:
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
                batch.append(json.loads(msg.value()))

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
