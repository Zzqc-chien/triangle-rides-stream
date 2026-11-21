"""Extension B - Sequential modeling on the live stream. Two online models, both learned as events arrive.

1. Trip completion probability (prediction)
   A logistic regression, refitted every 200 trips on a sliding window of recent trips, scores each
   incoming trip record before learning from its outcome (prequential, "test, then train"), so every
   stored prediction is out of sample. Features: pickup zone, service type, quoted pickup ETA,
   surge multiplier, route distance and local time of day - fields a real system would know at
   request time. Two limits: the stream carries finished trips (there is no separate request
   event), and distance_km on ~0.4% of completed trips has been altered by the simulated GPS glitch.

2. Fare anomaly detection
   Per service type, a recursive-least-squares regression learns the price curve
   fare / surge ~ b0 + b1 * km + b2 * minutes from the stream. A completed trip is flagged when
   its log fare is far from that curve (robust z-score on the last 300 residuals, |z| > 6), or when
   the implied speed is physically impossible (> 130 km/h, a sign of a GPS glitch).

Run:  python consumer_ml.py
Consumer group: trip-ml
"""

import json
import math
from collections import deque
from datetime import datetime

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score

from common.config import ANOMALIES_TOPIC, TRIPS_TOPIC
from common.db import connect, ensure_schema
from common.generator import LOCAL_TZ, SERVICES, ZONES
from common.kafka_utils import make_producer, run_consumer

GROUP_ID = "trip-ml"
ZONE_NAMES = [z.name for z in ZONES]
SERVICE_NAMES = list(SERVICES)
EVAL_WINDOW = 1000            # predictions in the rolling evaluation window
METRICS_EVERY = 100           # write a model_metrics row every N trips


# --------------------------------------------------------------------------- completion model
def features(t: dict) -> np.ndarray:
    local = datetime.fromisoformat(t["event_time"]).astimezone(LOCAL_TZ)
    hour = local.hour + local.minute / 60
    zone = [1.0 if t["pickup_zone"] == z else 0.0 for z in ZONE_NAMES]
    svc = [1.0 if t["service_type"] == s else 0.0 for s in SERVICE_NAMES]
    numeric = [
        t["eta_min"] / 10,
        t["surge_multiplier"] - 1,
        t["distance_km"] / 20,
        math.sin(2 * math.pi * hour / 24),
        math.cos(2 * math.pi * hour / 24),
        1.0 if local.weekday() >= 5 else 0.0,
    ]
    return np.array(zone + svc + numeric)


class CompletionModel:
    """Logistic regression re-fitted every REFIT_EVERY trips on a sliding window of recent trips.

    The sliding window lets the model follow drift (time of day, demand events) while staying
    stable; refitting on a few thousand rows takes milliseconds.
    """

    TRAIN_WINDOW = 4000
    REFIT_EVERY = 200
    MIN_TRAIN = 300

    def __init__(self):
        self.clf = LogisticRegression(C=1.0, max_iter=500)
        self.fitted = False
        self.seen = 0
        self.completed_seen = 0
        self.since_refit = 0
        self.X_buf = deque(maxlen=self.TRAIN_WINDOW)
        self.y_buf = deque(maxlen=self.TRAIN_WINDOW)
        self.recent = deque(maxlen=EVAL_WINDOW)     # (p_complete, actual, base_rate_at_the_time)

    def predict(self, X: np.ndarray) -> np.ndarray:
        if not self.fitted:
            base = (self.completed_seen + 1) / (self.seen + 2)
            return np.full(len(X), base)
        return self.clf.predict_proba(X)[:, 1]

    def learn(self, X: np.ndarray, y: np.ndarray, p: np.ndarray) -> None:
        for xi, pi, yi in zip(X, p, y):
            base = (self.completed_seen + 1) / (self.seen + 2)     # running completion rate
            self.recent.append((float(pi), int(yi), base))
            self.X_buf.append(xi)
            self.y_buf.append(int(yi))
            self.seen += 1
            self.completed_seen += int(yi)
            self.since_refit += 1
        ys = np.array(self.y_buf)
        ready = len(ys) >= self.MIN_TRAIN and 0 < ys.sum() < len(ys)
        if ready and (not self.fitted or self.since_refit >= self.REFIT_EVERY):
            self.clf.fit(np.vstack(self.X_buf), ys)
            self.fitted = True
            self.since_refit = 0

    def metrics(self) -> dict | None:
        if len(self.recent) < 100:
            return None
        p = np.array([r[0] for r in self.recent])
        y = np.array([r[1] for r in self.recent])
        base = np.array([r[2] for r in self.recent])
        eps = 1e-6
        ll = -np.mean(y * np.log(np.clip(p, eps, 1)) + (1 - y) * np.log(np.clip(1 - p, eps, 1)))
        bll = -np.mean(y * np.log(base) + (1 - y) * np.log(1 - base))
        majority = max(y.mean(), 1 - y.mean())
        auc = roc_auc_score(y, p) if 0 < y.sum() < len(y) else None
        return {
            "trips_seen": self.seen, "window_size": len(self.recent),
            "accuracy": float(np.mean((p >= 0.5) == y)), "baseline_accuracy": float(majority),
            "log_loss": float(ll), "baseline_log_loss": float(bll),
            "auc": None if auc is None else float(auc),
        }


# --------------------------------------------------------------------------- anomaly detector
class FareCurve:
    """Recursive least squares for fare/surge ~ b0 + b1*km + b2*min, plus a residual buffer."""

    def __init__(self, forgetting: float = 0.999):
        self.w = np.zeros(3)
        self.P = np.eye(3) * 1000.0
        self.lam = forgetting
        self.residuals = deque(maxlen=300)
        self.recent_y = deque(maxlen=300)
        self.n = 0

    def predict(self, km: float, minutes: float) -> float:
        linear = float(self.w @ np.array([1.0, km, minutes]))
        # Short trips hit the service's minimum fare, which a straight line cannot capture.
        # The 2nd percentile of recent normal fares is a good running estimate of that floor.
        floor = float(np.percentile(self.recent_y, 2)) if len(self.recent_y) >= 30 else 0.0
        return max(linear, floor)

    def update(self, km: float, minutes: float, y: float) -> None:
        x = np.array([1.0, km, minutes])
        Px = self.P @ x
        k = Px / (self.lam + x @ Px)
        self.w = self.w + k * (y - x @ self.w)
        self.P = (self.P - np.outer(k, Px)) / self.lam
        self.recent_y.append(y)
        self.n += 1


class FareAnomalyDetector:
    WARMUP = 50
    Z_THRESHOLD = 6.0
    MAX_SPEED_KMH = 130.0
    MIN_SPREAD = 0.05

    def __init__(self):
        self.curves = {s: FareCurve() for s in SERVICE_NAMES}

    def check(self, t: dict) -> dict | None:
        if t["status"] != "completed" or t["duration_min"] <= 0 or t["distance_km"] <= 0:
            return None
        curve = self.curves[t["service_type"]]
        km, minutes = t["distance_km"], t["duration_min"]
        y = t["fare_usd"] / t["surge_multiplier"]
        speed = km / (minutes / 60)
        residual = math.log(max(y, 0.5)) - math.log(max(curve.predict(km, minutes), 0.5))

        z, reasons = 0.0, []
        if curve.n >= self.WARMUP and len(curve.residuals) >= 30:
            r = np.array(curve.residuals)
            med = float(np.median(r))
            # Floor the spread at 5% so tolls and airport fees on a quiet stream don't look abnormal.
            mad = max(float(np.median(np.abs(r - med))) * 1.4826, self.MIN_SPREAD)
            z = (residual - med) / mad
            if z > self.Z_THRESHOLD:
                reasons.append("fare far above expected")
            elif z < -self.Z_THRESHOLD:
                reasons.append("fare far below expected")
        if speed > self.MAX_SPEED_KMH:
            reasons.append(f"impossible speed {speed:.0f} km/h")

        if reasons:                       # keep anomalies out of the model and the baseline
            return {
                "trip_id": t["trip_id"], "event_time": t["event_time"],
                "pickup_zone": t["pickup_zone"], "service_type": t["service_type"],
                "fare_usd": t["fare_usd"], "distance_km": km,
                "fare_per_km": round(t["fare_usd"] / km, 2), "speed_kmh": round(speed, 1),
                "robust_z": round(z, 2), "reason": "; ".join(reasons),
            }
        curve.update(km, minutes, y)
        curve.residuals.append(residual)
        return None


# --------------------------------------------------------------------------- consumer
PRED_SQL = """
INSERT INTO completion_predictions (trip_id, event_time, pickup_zone, service_type, p_complete, completed)
VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (trip_id) DO NOTHING
"""
ANOMALY_SQL = """
INSERT INTO fare_anomalies (trip_id, event_time, pickup_zone, service_type, fare_usd, distance_km,
                            fare_per_km, speed_kmh, robust_z, reason)
VALUES (%(trip_id)s, %(event_time)s, %(pickup_zone)s, %(service_type)s, %(fare_usd)s, %(distance_km)s,
        %(fare_per_km)s, %(speed_kmh)s, %(robust_z)s, %(reason)s)
ON CONFLICT (trip_id) DO NOTHING
"""
METRICS_SQL = """
INSERT INTO model_metrics (trips_seen, window_size, accuracy, baseline_accuracy, log_loss,
                           baseline_log_loss, auc)
VALUES (%(trips_seen)s, %(window_size)s, %(accuracy)s, %(baseline_accuracy)s, %(log_loss)s,
        %(baseline_log_loss)s, %(auc)s)
"""

model = CompletionModel()
detector = FareAnomalyDetector()


def process(trips: list[dict]) -> tuple[list[tuple], list[dict], dict | None]:
    """Pure stream logic (no I/O), so it can be tested without Kafka or Postgres."""
    X = np.vstack([features(t) for t in trips])
    y = np.array([1 if t["status"] == "completed" else 0 for t in trips])
    before = model.seen
    p = model.predict(X)                    # test ...
    model.learn(X, y, p)                    # ... then train
    preds = [(t["trip_id"], t["event_time"], t["pickup_zone"], t["service_type"],
              round(float(pi), 4), bool(yi)) for t, pi, yi in zip(trips, p, y)]
    anomalies = [a for a in (detector.check(t) for t in trips) if a]
    crossed = model.seen // METRICS_EVERY > before // METRICS_EVERY
    return preds, anomalies, (model.metrics() if crossed else None)


def main():
    conn = connect()
    ensure_schema(conn)
    producer = make_producer()

    def handle_batch(trips: list[dict]) -> None:
        preds, anomalies, metrics = process(trips)
        with conn.cursor() as cur:
            cur.executemany(PRED_SQL, preds)
            if anomalies:
                cur.executemany(ANOMALY_SQL, anomalies)
            if metrics:
                cur.execute(METRICS_SQL, metrics)
        conn.commit()
        for a in anomalies:
            producer.produce(ANOMALIES_TOPIC, key=a["pickup_zone"], value=json.dumps(a))
            print(f"[{GROUP_ID}] ANOMALY {a['service_type']} ${a['fare_usd']} "
                  f"{a['distance_km']} km - {a['reason']}")
        producer.poll(0)
        if metrics:
            print(f"[{GROUP_ID}] after {metrics['trips_seen']} trips: AUC={metrics['auc']:.3f} "
                  f"log-loss={metrics['log_loss']:.3f} (baseline {metrics['baseline_log_loss']:.3f})")

    try:
        run_consumer(GROUP_ID, [TRIPS_TOPIC], handle_batch, max_batch=100)
    finally:
        producer.flush(5)


if __name__ == "__main__":
    main()
