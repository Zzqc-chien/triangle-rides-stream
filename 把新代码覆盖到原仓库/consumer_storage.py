"""Parts 2 + 3 - Storage consumer: reads `ride-trips` in its own group and inserts every trip.

Run:  python consumer_storage.py
Consumer group: trip-storage
"""

from common.config import TRIPS_TOPIC
from common.db import connect, ensure_schema
from common.kafka_utils import run_consumer

GROUP_ID = "trip-storage"

COLUMNS = [
    "trip_id", "event_time", "pickup_zone", "dropoff_zone", "service_type", "status",
    "distance_km", "duration_min", "eta_min", "surge_multiplier", "fare_usd", "tip_usd",
    "payment_method", "driver_rating",
]
INSERT_SQL = (
    f"INSERT INTO trips ({', '.join(COLUMNS)}) "
    f"VALUES ({', '.join(['%s'] * len(COLUMNS))}) "
    "ON CONFLICT (trip_id) DO NOTHING"
)

conn = connect()
ensure_schema(conn)
stored = 0


def handle_batch(trips: list[dict]) -> None:
    global stored
    with conn.cursor() as cur:
        cur.executemany(INSERT_SQL, [tuple(t[c] for c in COLUMNS) for t in trips])
    conn.commit()
    stored += len(trips)
    if stored % 500 < len(trips):
        print(f"[{GROUP_ID}] stored {stored} trips")


if __name__ == "__main__":
    run_consumer(GROUP_ID, [TRIPS_TOPIC], handle_batch)
