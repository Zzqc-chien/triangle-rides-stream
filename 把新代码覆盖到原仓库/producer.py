"""Part 1 - Producer: publishes synthetic ride-hailing trips to the `ride-trips` topic, forever.

Run:  python producer.py            (Ctrl+C to stop)
      EVENTS_PER_SEC=5 python producer.py

Message key = pickup_zone (see README for why).
"""

import json
import random
import signal
import threading
import time

from confluent_kafka import Producer

from common.config import BOOTSTRAP, EVENTS_PER_SEC, TRIPS_TOPIC
from common.generator import TripGenerator

running = True


def stop(*_):
    global running
    running = False


def on_delivery(err, msg):
    if err is not None:
        print(f"delivery failed: {err}")


def main():
    if threading.current_thread() is threading.main_thread():
        signal.signal(signal.SIGINT, stop)
        signal.signal(signal.SIGTERM, stop)

    producer = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 50, "acks": "all"})
    generator = TripGenerator()
    print(f"producing to '{TRIPS_TOPIC}' on {BOOTSTRAP} at ~{EVENTS_PER_SEC}/s (Ctrl+C to stop)")

    sent, last_event_label, last_report = 0, None, time.time()
    while running:
        trip = generator.next_trip()
        producer.produce(
            TRIPS_TOPIC,
            key=trip["pickup_zone"],
            value=json.dumps(trip),
            on_delivery=on_delivery,
        )
        producer.poll(0)
        sent += 1

        event = generator.active_event
        label = event and f"{event['label']} ({event['zone']})"
        if label != last_event_label:
            print(f"demand event started: {label}" if label else "demand event ended")
            last_event_label = label

        if time.time() - last_report >= 10:
            print(f"sent {sent} trips | last: {trip['pickup_zone']} -> {trip['dropoff_zone']} "
                  f"{trip['service_type']} {trip['status']} ${trip['fare_usd']}")
            last_report = time.time()

        # Poisson arrivals: exponential gaps, rate scaled by local time of day (busier at rush hour).
        rate = EVENTS_PER_SEC * min(1.6, max(0.6, generator.demand_multiplier(time.time()) / 1.5))
        time.sleep(random.expovariate(rate))

    print("flushing...")
    producer.flush(10)
    print(f"stopped after {sent} trips")


if __name__ == "__main__":
    main()
