"""Settings shared by the producer and every consumer, read from the environment / .env."""

import os

from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Topics. The raw topic is named for the domain; the other three are derived streams.
TRIPS_TOPIC = os.getenv("TRIPS_TOPIC", "ride-trips")
ZONE_METRICS_TOPIC = os.getenv("ZONE_METRICS_TOPIC", "ride-zone-metrics")
SURGE_ALERTS_TOPIC = os.getenv("SURGE_ALERTS_TOPIC", "ride-surge-alerts")
ANOMALIES_TOPIC = os.getenv("ANOMALIES_TOPIC", "ride-fare-anomalies")

# Local Postgres from docker-compose by default; set DATABASE_URL to Supabase for deployment.
DATABASE_URL = os.getenv("DATABASE_URL") or "postgresql://rides:rides@localhost:5432/rides"

# Producer pacing: average trip events per second (Poisson arrivals, scaled by time of day).
EVENTS_PER_SEC = float(os.getenv("EVENTS_PER_SEC", "2"))
