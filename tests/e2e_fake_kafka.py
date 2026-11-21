"""End-to-end smoke test without a Kafka broker: real producer + all three consumers + real Postgres,
with confluent_kafka swapped for the in-memory fake. Useful when Docker is not available.

Run:  DATABASE_URL=postgresql://... python tests/e2e_fake_kafka.py [seconds] [events_per_sec]
"""

import os
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

seconds = float(sys.argv[1]) if len(sys.argv) > 1 else 60
os.environ["EVENTS_PER_SEC"] = sys.argv[2] if len(sys.argv) > 2 else "10"

import confluent_kafka  # noqa: E402

from fake_kafka import BROKER, FakeConsumer, FakeProducer  # noqa: E402

confluent_kafka.Producer = FakeProducer
confluent_kafka.Consumer = FakeConsumer

import consumer_metrics  # noqa: E402
import consumer_ml  # noqa: E402
import consumer_storage  # noqa: E402
import producer  # noqa: E402
from common import kafka_utils  # noqa: E402
from common.config import TRIPS_TOPIC  # noqa: E402

threads = [
    threading.Thread(target=producer.main, name="producer"),
    threading.Thread(target=kafka_utils.run_consumer, name="storage",
                     args=(consumer_storage.GROUP_ID, [TRIPS_TOPIC], consumer_storage.handle_batch)),
    threading.Thread(target=kafka_utils.run_consumer, name="metrics",
                     args=(consumer_metrics.GROUP_ID, [TRIPS_TOPIC], consumer_metrics.handle_batch),
                     kwargs={"max_wait_s": 2.0, "with_positions": True}),
    threading.Thread(target=consumer_ml.main, name="ml"),
]
for t in threads:
    t.start()

start = time.time()
while time.time() - start < seconds:
    time.sleep(10)
    with consumer_storage.conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM trips")
        n = cur.fetchone()[0]
    consumer_storage.conn.rollback()
    print(f"--- t={time.time() - start:.0f}s produced={len(BROKER.topics[TRIPS_TOPIC])} trips_rows={n}")

producer.running = False
kafka_utils._running = False
for t in threads:
    t.join(timeout=15)
print("topics:", {k: len(v) for k, v in BROKER.topics.items()})

# Replay check: hand every trip to the aggregating consumer a second time, as Kafka would after a
# crash between the database commit and the offset commit. The totals must not change.
TOTALS = "SELECT coalesce(sum(trips), 0), coalesce(round(sum(revenue_usd)::numeric, 2), 0) FROM zone_minute_metrics"
c = consumer_metrics.conn
with c.cursor() as cur:
    cur.execute(TOTALS)
    before = cur.fetchone()
c.commit()
replay = [(__import__("json").loads(m.value()), m.topic(), m.partition(), m.offset())
          for m in BROKER.topics[TRIPS_TOPIC]]
consumer_metrics.handle_batch(replay)
with c.cursor() as cur:
    cur.execute(TOTALS)
    after = cur.fetchone()
c.commit()
print(f"replay check: zone totals before={before} after={after} ->",
      "OK, unchanged" if before == after else "FAILED, totals inflated")
