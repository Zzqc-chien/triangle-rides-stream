"""Synthetic ride-hailing trips around the Research Triangle (Durham / Raleigh / Chapel Hill).

Every call to `next_trip()` returns one finished trip (completed or cancelled). The values are
driven by simple, realistic mechanics rather than uniform noise:

* demand follows the local time of day (morning and evening rush, late-night peak);
* each pickup zone has its own driver supply, so pickup ETAs differ (the airport waits longest);
* surge pricing per zone drifts slowly and jumps during "demand events"
  (a Duke game letting out, a bank of flights landing at RDU, a concert downtown);
* longer ETAs and higher surge make cancellations more likely;
* fares follow a per-service price card: base + per km + per minute, times surge, with a minimum.

About 0.4% of trips are deliberately corrupted (a billing glitch or a GPS glitch) so the
anomaly detector in consumer_ml.py has something real to find. `last_injected` exposes the
ground truth for offline evaluation only; it is never sent to Kafka.
"""

from __future__ import annotations

import math
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

LOCAL_TZ = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class Zone:
    name: str
    lat: float
    lon: float
    demand: float      # relative share of pickups
    base_eta: float    # typical minutes until a driver arrives


ZONES = [
    Zone("Duke West Campus", 36.0014, -78.9382, 1.3, 4.0),
    Zone("Downtown Durham", 35.9940, -78.8986, 1.5, 3.5),
    Zone("Ninth Street", 36.0110, -78.9220, 0.8, 4.0),
    Zone("Southpoint", 35.9036, -78.9425, 0.7, 5.5),
    Zone("RTP", 35.8992, -78.8636, 0.9, 6.0),
    Zone("RDU Airport", 35.8801, -78.7880, 1.2, 7.5),
    Zone("Downtown Raleigh", 35.7796, -78.6382, 1.6, 3.0),
    Zone("NC State", 35.7847, -78.6821, 1.0, 4.0),
    Zone("Chapel Hill", 35.9049, -79.0469, 1.1, 5.0),
    Zone("Cary", 35.7915, -78.7811, 0.7, 6.5),
]
ZONE_BY_NAME = {z.name: z for z in ZONES}

# service: (share, base $, $/km, $/min, minimum fare $, cancel bias)
SERVICES = {
    "Standard": (0.60, 2.50, 0.95, 0.30, 7.50, 0.0),
    "Comfort": (0.15, 3.50, 1.30, 0.40, 10.00, -0.3),
    "XL": (0.12, 4.00, 1.65, 0.50, 12.00, -0.1),
    "Shared": (0.13, 2.00, 0.70, 0.22, 6.00, 0.4),
}
PAYMENTS = (["card", "apple_pay", "google_pay", "paypal"], [0.55, 0.25, 0.12, 0.08])
RATINGS = ([5, 4, 3, 2, 1], [0.78, 0.14, 0.05, 0.02, 0.01])

# Demand events: zone, label, demand multiplier, surge target
EVENTS = [
    ("Duke West Campus", "Duke basketball game lets out", 3.0, 2.6),
    ("RDU Airport", "a bank of evening flights lands", 2.8, 2.2),
    ("Downtown Raleigh", "concert ends at Red Hat Amphitheater", 3.2, 2.8),
    ("Downtown Durham", "DPAC show lets out", 2.5, 2.1),
    ("Chapel Hill", "Franklin Street after a UNC win", 2.6, 2.4),
    ("NC State", "PNC Arena game ends", 2.8, 2.5),
]


def haversine_km(a: Zone, b: Zone) -> float:
    r = 6371.0
    p1, p2 = math.radians(a.lat), math.radians(b.lat)
    dp, dl = p2 - p1, math.radians(b.lon - a.lon)
    h = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(h))


def time_of_day_factor(local_hour: float) -> float:
    """Relative demand by local hour: morning rush, evening rush, late-night bump."""
    peaks = [(8.5, 1.6, 1.2), (17.8, 1.9, 1.5), (23.0, 1.4, 1.6)]
    level = 0.45
    for centre, height, width in peaks:
        d = min(abs(local_hour - centre), 24 - abs(local_hour - centre))
        level += height * math.exp(-0.5 * (d / width) ** 2)
    return level


class TripGenerator:
    def __init__(self, seed: int | None = None, anomaly_rate: float = 0.004):
        self.rng = random.Random(seed)
        self.anomaly_rate = anomaly_rate
        self.log_surge = {z.name: 0.0 for z in ZONES}   # AR(1) state per zone
        self.active_event: dict | None = None
        self.next_event_at: float | None = None
        self.last_time: float | None = None
        self.last_injected: str | None = None           # ground truth, never published

    # ------------------------------------------------------------------ world state
    def _advance(self, now: float) -> None:
        """Move surge and demand events forward to wall-clock time `now` (epoch seconds)."""
        if self.last_time is None:
            self.last_time = now
            self.next_event_at = now + self.rng.uniform(40, 90)   # first event early in a demo
        dt = max(0.0, now - self.last_time)
        self.last_time = now

        # Surge mean-reverts toward ~1.0 with a little noise, in steps of one second.
        steps = min(int(dt) + 1, 600)
        for name in self.log_surge:
            x = self.log_surge[name]
            for _ in range(steps):
                x = 0.985 * x + self.rng.gauss(0, 0.025)
            self.log_surge[name] = x

        if self.active_event and now >= self.active_event["ends_at"]:
            self.active_event = None
            self.next_event_at = now + self.rng.uniform(150, 300)
        if self.active_event is None and now >= self.next_event_at:
            zone, label, mult, surge = self.rng.choice(EVENTS)
            self.active_event = {
                "zone": zone, "label": label, "demand": mult, "surge": surge,
                "starts_at": now, "ends_at": now + self.rng.uniform(90, 150),
            }

    def current_surge(self, zone: str) -> float:
        surge = 1.0 + max(0.0, self.log_surge[zone]) * 2.0
        ev = self.active_event
        if ev and ev["zone"] == zone:
            # ramp up over ~20 s, then hold near the event's surge level
            ramp = min(1.0, (self.last_time - ev["starts_at"]) / 20)
            surge = max(surge, 1.0 + (ev["surge"] - 1.0) * ramp + self.rng.gauss(0, 0.1))
        return round(min(max(surge, 1.0), 3.5), 1)

    def demand_multiplier(self, now: float) -> float:
        local = datetime.fromtimestamp(now, LOCAL_TZ)
        hour = local.hour + local.minute / 60
        weekend_night = local.weekday() >= 4 and (hour >= 21 or hour < 3)
        return time_of_day_factor(hour) * (1.25 if weekend_night else 1.0)

    # ------------------------------------------------------------------ one trip
    def next_trip(self, now: float | None = None) -> dict:
        rng = self.rng
        now = now if now is not None else datetime.now(timezone.utc).timestamp()
        self._advance(now)
        self.last_injected = None
        local = datetime.fromtimestamp(now, LOCAL_TZ)
        hour = local.hour + local.minute / 60

        weights = [z.demand for z in ZONES]
        if self.active_event:
            i = [z.name for z in ZONES].index(self.active_event["zone"])
            weights[i] *= self.active_event["demand"]
        pickup = rng.choices(ZONES, weights=weights)[0]

        if pickup.name != "RDU Airport" and rng.random() < 0.33:
            dropoff = pickup
        else:
            others = [z for z in ZONES if z is not pickup]
            w = [z.demand * math.exp(-haversine_km(pickup, z) / 18) for z in others]
            dropoff = rng.choices(others, weights=w)[0]

        service = rng.choices(list(SERVICES), weights=[s[0] for s in SERVICES.values()])[0]
        _, base, per_km, per_min, min_fare, cancel_bias = SERVICES[service]

        if dropoff is pickup:
            distance = 0.8 + rng.gammavariate(2.0, 1.4)
        else:
            distance = haversine_km(pickup, dropoff) * 1.3 * rng.uniform(0.95, 1.15)

        rush = 8 <= hour < 9.5 or 16.5 <= hour < 19
        speed = (27 if rush else 37) + 12 * min(1.0, distance / 25)        # km/h, highways faster
        duration = distance / speed * 60 + 1.5 + rng.lognormvariate(0, 0.5)

        surge = self.current_surge(pickup.name)
        eta = max(1.0, pickup.base_eta * (1 + 0.6 * (surge - 1)) * rng.lognormvariate(0, 0.35))
        if service in ("Comfort", "XL"):
            eta *= 1.25                                                     # fewer of these cars

        p_cancel = 1 / (1 + math.exp(-(-3.4 + 0.17 * eta + 1.0 * (surge - 1) + cancel_bias)))
        if rng.random() < p_cancel:
            status = "rider_cancelled" if rng.random() < 0.75 else "driver_cancelled"
        else:
            status = "completed"

        if status == "completed":
            fees = rng.choice([0.0, 0.0, 0.0, 1.0, 1.5])                    # tolls / airport fees
            route_noise = rng.lognormvariate(0, 0.05)                       # detours, waits
            fare = max(min_fare, (base + per_km * distance + per_min * duration) * route_noise) \
                * surge + fees
            tip = round(fare * rng.uniform(0.10, 0.25), 2) if rng.random() < 0.32 else 0.0
            rating = float(rng.choices(*RATINGS)[0])
            trip_minutes = duration
        else:
            # Cancelled trips keep the quoted route distance; rider cancellations after the
            # driver has been on the way a while are charged a $5 fee.
            fare = 5.0 if status == "rider_cancelled" and eta > 5 and rng.random() < 0.6 else 0.0
            tip, rating, trip_minutes = 0.0, None, 0.0

        trip = {
            "trip_id": str(uuid.uuid4()),
            "event_time": datetime.fromtimestamp(now, timezone.utc).isoformat(timespec="milliseconds"),
            "pickup_zone": pickup.name,
            "dropoff_zone": dropoff.name,
            "service_type": service,
            "status": status,
            "distance_km": round(distance, 2),
            "duration_min": round(trip_minutes, 1),
            "eta_min": round(eta, 1),
            "surge_multiplier": surge,
            "fare_usd": round(fare, 2),
            "tip_usd": tip,
            "payment_method": rng.choices(*PAYMENTS)[0],
            "driver_rating": rating,
        }

        if status == "completed" and rng.random() < self.anomaly_rate:
            if rng.random() < 0.6:
                trip["fare_usd"] = round(trip["fare_usd"] * rng.uniform(4, 9), 2)
                self.last_injected = "billing_glitch"
            else:
                trip["distance_km"] = round(trip["distance_km"] * rng.uniform(6, 15), 2)
                self.last_injected = "gps_glitch"
        return trip
