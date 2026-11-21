"""Unit tests for the pure stream logic (no Kafka or Postgres needed).  Run: pytest -q"""

import os
import sys
import time
from datetime import datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.generator import TripGenerator  # noqa: E402

REQUIRED = {"trip_id": str, "event_time": str, "pickup_zone": str, "service_type": str,
            "status": str, "fare_usd": float, "distance_km": float, "surge_multiplier": float}


@pytest.fixture
def trips():
    gen, t0 = TripGenerator(seed=42), time.time()
    return [gen.next_trip(t0 + i * 0.5) for i in range(3000)]


def test_event_has_required_fields(trips):
    for t in trips[:100]:
        assert len(t) >= 6
        for field, kind in REQUIRED.items():
            assert isinstance(t[field], kind), field
        datetime.fromisoformat(t["event_time"])
    assert len({t["trip_id"] for t in trips}) == len(trips)          # ids are unique


def test_values_are_realistic(trips):
    completed = [t for t in trips if t["status"] == "completed"]
    assert 0.75 < len(completed) / len(trips) < 0.95
    assert all(1.0 <= t["surge_multiplier"] <= 3.5 for t in trips)
    assert all(t["driver_rating"] is None for t in trips if t["status"] != "completed")


def test_window_aggregation_and_alert_rules(monkeypatch):
    # consumer_metrics opens a DB connection and a Kafka producer on import; stub both.
    from common import db, kafka_utils
    monkeypatch.setattr(db, "connect", lambda: None)
    monkeypatch.setattr(db, "ensure_schema", lambda conn: None)
    monkeypatch.setattr(kafka_utils, "make_producer", lambda: None)
    sys.modules.pop("consumer_metrics", None)
    import consumer_metrics as cm

    base = {"event_time": "2026-11-20T21:05:12.000+00:00", "pickup_zone": "RDU Airport",
            "fare_usd": 20.0, "tip_usd": 2.0, "eta_min": 6.0}
    trips = [dict(base, status="completed", surge_multiplier=2.4) for _ in range(6)] + \
            [dict(base, status="rider_cancelled", surge_multiplier=2.4, fare_usd=0.0, tip_usd=0.0)
             for _ in range(4)]
    (row,) = cm.aggregate(trips)
    assert row["window_start"].minute == 5 and row["window_start"].second == 0
    assert (row["trips"], row["completed"], row["cancelled"]) == (10, 6, 4)
    assert row["revenue_usd"] == pytest.approx(132.0)
    rules = {r[0] for r in cm.check_rules(row)}
    assert rules == {"high_surge"}                       # 40% cancelled, but only 10 trips < 15
    assert cm.check_rules(dict(row, trips=5)) == []


def test_anomaly_detector_flags_glitches_only():
    os.environ.setdefault("DATABASE_URL", "postgresql://unused")
    from consumer_ml import FareAnomalyDetector
    det, gen, t0 = FareAnomalyDetector(), TripGenerator(seed=5, anomaly_rate=0.0), time.time()
    normal = [gen.next_trip(t0 + i) for i in range(2000)]
    assert sum(det.check(t) is not None for t in normal) == 0
    glitch = next(t for t in normal if t["status"] == "completed").copy()
    glitch["trip_id"], glitch["fare_usd"] = "x", glitch["fare_usd"] * 6
    assert det.check(glitch) is not None
