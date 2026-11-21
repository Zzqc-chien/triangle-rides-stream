"""A tiny in-memory stand-in for confluent_kafka's Producer and Consumer, for tests only.

Unlike a single shared queue, it keeps a separate offset per (consumer group, topic), so several
groups reading the same topic each see every message - the behaviour the pipeline relies on.
"""

import threading
import time
from collections import defaultdict


class _Broker:
    def __init__(self):
        self.lock = threading.Lock()
        self.topics: dict[str, list] = defaultdict(list)
        self.offsets: dict[tuple[str, str], int] = defaultdict(int)


BROKER = _Broker()


class FakeMessage:
    def __init__(self, topic, key, value):
        self._t, self._k, self._v = topic, key, value

    def topic(self):
        return self._t

    def key(self):
        return self._k

    def value(self):
        return self._v

    def error(self):
        return None


class FakeProducer:
    def __init__(self, config):
        self.config = config

    def produce(self, topic, value=None, key=None, on_delivery=None, **_):
        value = value.encode() if isinstance(value, str) else value
        key = key.encode() if isinstance(key, str) else key
        with BROKER.lock:
            BROKER.topics[topic].append(FakeMessage(topic, key, value))

    def poll(self, timeout=0):
        return 0

    def flush(self, timeout=None):
        return 0


class FakeConsumer:
    def __init__(self, config):
        self.group = config["group.id"]
        self.topics: list[str] = []

    def subscribe(self, topics):
        self.topics = list(topics)

    def poll(self, timeout=1.0):
        with BROKER.lock:
            for t in self.topics:
                i = BROKER.offsets[(self.group, t)]
                if i < len(BROKER.topics[t]):
                    BROKER.offsets[(self.group, t)] = i + 1
                    return BROKER.topics[t][i]
        time.sleep(min(timeout, 0.05))
        return None

    def commit(self, *_, **__):
        return None

    def close(self):
        pass
