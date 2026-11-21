"""Reproducible check of the Part 4 alert rules against the generator's simulated demand events.

Simulation: TripGenerator(seed) at the producer's default rate (Poisson, 2 trips/s), for HOURS
of simulated time starting at START (Fri 2026-11-20 17:00 US Eastern, evening rush into night).
Trips are fed in 2-second batches through consumer_metrics.aggregate() and check_rules(), the same
code the live consumer runs, and every first firing of a rule per (zone, minute) is recorded.

Matching rule: an alert for zone Z and window minute W *coincides* with a demand event when an
event in the SAME zone Z was active at some moment between one minute before W and the end of W
(the extra minute covers the event's 20-second surge ramp and cancellations that land just after).

Two numbers, which answer different questions:
  * coincidence rate = alerts that coincide with an event / all alerts fired
    ("when an alert fires, is something really happening in that zone?")
  * event detection rate = events with at least one coinciding alert / all events
    ("when something happens, does an alert fire?")

Run:  python tests/evaluate_alerts.py [hours] [seed ...]        (defaults: 2 hours, seeds 3 4)
"""

import os
import random
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("DATABASE_URL", "postgresql://unused")

from common import db, kafka_utils  # noqa: E402

db.connect = lambda: None                       # consumer_metrics opens these on import; not needed
db.ensure_schema = lambda conn: None
kafka_utils.make_producer = lambda: None

import consumer_metrics as cm  # noqa: E402
from common.generator import TripGenerator  # noqa: E402

START = datetime(2026, 11, 20, 17, 0, tzinfo=ZoneInfo("America/New_York")).timestamp()
RATE = 2.0          # trips per second, the producer default
BATCH_S = 2.0       # the metrics consumer flushes at most every 2 s


def run(hours: float, seed: int) -> dict:
    gen, rng = TripGenerator(seed=seed), random.Random(seed)
    totals = defaultdict(lambda: {"trips": 0, "completed": 0, "cancelled": 0, "avg_surge": 0.0})
    alerts, events = {}, []
    t, batch, batch_start = START, [], START
    while t < START + hours * 3600:
        t += rng.expovariate(RATE)
        batch.append(gen.next_trip(t))
        ev = gen.active_event
        if ev and (not events or events[-1]["starts_at"] != ev["starts_at"]):
            events.append(dict(ev))
        if t - batch_start < BATCH_S:
            continue
        for part in cm.aggregate(batch):
            key = (part["window_start"], part["pickup_zone"])
            row = totals[key]
            n = row["trips"] + part["trips"]
            row["avg_surge"] = (row["avg_surge"] * row["trips"] + part["avg_surge"] * part["trips"]) / n
            row["completed"] += part["completed"]
            row["cancelled"] += part["cancelled"]
            row["trips"] = n
            for rule, _, _ in cm.check_rules(dict(row, pickup_zone=key[1])):
                alerts.setdefault((key, rule), key)
        batch, batch_start = [], t

    def coincides(alert_key, event) -> bool:
        window, zone = alert_key
        w0 = window.timestamp()
        return zone == event["zone"] and event["starts_at"] < w0 + 60 and event["ends_at"] > w0 - 60

    alert_keys = list(alerts.values())
    matched_alerts = sum(any(coincides(a, e) for e in events) for a in alert_keys)
    detected_events = sum(any(coincides(a, e) for a in alert_keys) for e in events)
    return {"seed": seed, "alerts": len(alert_keys), "matched": matched_alerts,
            "events": len(events), "detected": detected_events}


if __name__ == "__main__":
    hours = float(sys.argv[1]) if len(sys.argv) > 1 else 2.0
    seeds = [int(s) for s in sys.argv[2:]] or [3, 4]
    print(f"{hours:g} h per seed at {RATE:g} trips/s from "
          f"{datetime.fromtimestamp(START, ZoneInfo('America/New_York'))}\n")
    print("| Seed | Alerts fired | Coinciding with an event | Demand events | Events with an alert |")
    print("|---|---|---|---|---|")
    tot = defaultdict(int)
    for s in seeds:
        r = run(hours, s)
        for k in ("alerts", "matched", "events", "detected"):
            tot[k] += r[k]
        print(f"| {s} | {r['alerts']} | {r['matched']} ({r['matched'] / max(1, r['alerts']):.0%}) | "
              f"{r['events']} | {r['detected']} ({r['detected'] / max(1, r['events']):.0%}) |")
    print(f"| all | {tot['alerts']} | {tot['matched']} ({tot['matched'] / max(1, tot['alerts']):.0%}) | "
          f"{tot['events']} | {tot['detected']} ({tot['detected'] / max(1, tot['events']):.0%}) |")
