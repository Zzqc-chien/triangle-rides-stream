"""Part 4 - Stream processing: one-minute tumbling windows per pickup zone + surge alerts.

Reads `ride-trips` in its own group and, for every trip, updates the window it falls in
(window = event_time truncated to the minute, per pickup_zone):

    trips, completed, cancelled, revenue, average surge, average pickup ETA

Results go to two places:
  * table  zone_minute_metrics  (upserted while the window is still filling) and topic ride-zone-metrics
  * table  surge_alerts         when a window crosses an alert rule, and topic ride-surge-alerts

Run:  python consumer_metrics.py
Consumer group: zone-metrics
"""

import json
from collections import defaultdict
from datetime import datetime

from common.config import SURGE_ALERTS_TOPIC, TRIPS_TOPIC, ZONE_METRICS_TOPIC
from common.db import connect, ensure_schema
from common.kafka_utils import make_producer, run_consumer

GROUP_ID = "zone-metrics"

# Alert rules, checked on the running totals of the current window.
SURGE_THRESHOLD = 2.0          # average surge multiplier in the window
CANCEL_THRESHOLD = 0.40        # share of trips cancelled in the window
MIN_TRIPS = 8                  # ignore windows with too few trips to judge a surge level
MIN_TRIPS_CANCEL = 15          # cancellations are noisier: a rate needs more trips behind it

UPSERT_SQL = """
INSERT INTO zone_minute_metrics
    (window_start, pickup_zone, trips, completed, cancelled, revenue_usd, avg_surge, avg_eta_min, updated_at)
VALUES (%(window_start)s, %(pickup_zone)s, %(trips)s, %(completed)s, %(cancelled)s,
        %(revenue_usd)s, %(avg_surge)s, %(avg_eta_min)s, now())
ON CONFLICT (window_start, pickup_zone) DO UPDATE SET
    avg_surge   = (zone_minute_metrics.avg_surge * zone_minute_metrics.trips
                   + EXCLUDED.avg_surge * EXCLUDED.trips)
                  / (zone_minute_metrics.trips + EXCLUDED.trips),
    avg_eta_min = (zone_minute_metrics.avg_eta_min * zone_minute_metrics.trips
                   + EXCLUDED.avg_eta_min * EXCLUDED.trips)
                  / (zone_minute_metrics.trips + EXCLUDED.trips),
    trips       = zone_minute_metrics.trips + EXCLUDED.trips,
    completed   = zone_minute_metrics.completed + EXCLUDED.completed,
    cancelled   = zone_minute_metrics.cancelled + EXCLUDED.cancelled,
    revenue_usd = zone_minute_metrics.revenue_usd + EXCLUDED.revenue_usd,
    updated_at  = now()
RETURNING window_start, pickup_zone, trips, completed, cancelled, revenue_usd, avg_surge, avg_eta_min
"""

ALERT_SQL = """
INSERT INTO surge_alerts (window_start, pickup_zone, rule, value, trips, message)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (window_start, pickup_zone, rule) DO NOTHING
RETURNING alerted_at
"""

conn = connect()
ensure_schema(conn)
producer = make_producer()


def window_start(event_time: str) -> datetime:
    return datetime.fromisoformat(event_time).replace(second=0, microsecond=0)


def aggregate(trips: list[dict]) -> list[dict]:
    """Per-batch partial aggregates for each (window, zone) touched by this batch."""
    acc = defaultdict(lambda: {"trips": 0, "completed": 0, "cancelled": 0,
                               "revenue_usd": 0.0, "surge_sum": 0.0, "eta_sum": 0.0})
    for t in trips:
        a = acc[(window_start(t["event_time"]), t["pickup_zone"])]
        a["trips"] += 1
        a["completed"] += t["status"] == "completed"
        a["cancelled"] += t["status"] != "completed"
        a["revenue_usd"] += t["fare_usd"] + t["tip_usd"]
        a["surge_sum"] += t["surge_multiplier"]
        a["eta_sum"] += t["eta_min"]
    return [
        {"window_start": w, "pickup_zone": z, "trips": a["trips"], "completed": a["completed"],
         "cancelled": a["cancelled"], "revenue_usd": a["revenue_usd"],
         "avg_surge": a["surge_sum"] / a["trips"], "avg_eta_min": a["eta_sum"] / a["trips"]}
        for (w, z), a in acc.items()
    ]


def check_rules(row: dict) -> list[tuple[str, float, str]]:
    if row["trips"] < MIN_TRIPS:
        return []
    fired = []
    if row["avg_surge"] >= SURGE_THRESHOLD:
        fired.append(("high_surge", row["avg_surge"],
                      f"{row['pickup_zone']}: surge averaging {row['avg_surge']:.1f}x "
                      f"over {row['trips']} trips - send more drivers"))
    cancel_rate = row["cancelled"] / row["trips"]
    if cancel_rate >= CANCEL_THRESHOLD and row["trips"] >= MIN_TRIPS_CANCEL:
        fired.append(("high_cancellations", cancel_rate,
                      f"{row['pickup_zone']}: {cancel_rate:.0%} of {row['trips']} trips cancelled"))
    return fired


def handle_batch(trips: list[dict]) -> None:
    updated, new_alerts = [], []
    with conn.cursor() as cur:
        for partial in aggregate(trips):
            cur.execute(UPSERT_SQL, partial)
            cols = [d.name for d in cur.description]
            row = dict(zip(cols, cur.fetchone()))
            updated.append(row)
            for rule, value, message in check_rules(row):
                cur.execute(ALERT_SQL, (row["window_start"], row["pickup_zone"], rule,
                                        value, row["trips"], message))
                if cur.fetchone():                       # first time this window hit this rule
                    new_alerts.append({"window_start": row["window_start"].isoformat(),
                                       "pickup_zone": row["pickup_zone"], "rule": rule,
                                       "value": round(value, 3), "trips": row["trips"],
                                       "message": message})
    conn.commit()

    for row in updated:
        payload = {**row, "window_start": row["window_start"].isoformat()}
        producer.produce(ZONE_METRICS_TOPIC, key=row["pickup_zone"], value=json.dumps(payload))
    for alert in new_alerts:
        producer.produce(SURGE_ALERTS_TOPIC, key=alert["pickup_zone"], value=json.dumps(alert))
        print(f"[{GROUP_ID}] ALERT {alert['message']}")
    producer.poll(0)


if __name__ == "__main__":
    try:
        run_consumer(GROUP_ID, [TRIPS_TOPIC], handle_batch, max_wait_s=2.0)
    finally:
        producer.flush(5)
