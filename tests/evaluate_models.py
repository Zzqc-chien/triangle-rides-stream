"""Offline evaluation of consumer_ml.py, reproducible: fixed seed AND fixed simulated start time.

What it does
  * Generates N trips with TripGenerator(seed), 0.5 s apart in simulated time, starting at
    START (Tue 2026-11-17 07:00 US Eastern), so time-of-day effects are the same on every run.
    20,000 trips cover 07:00-09:47, i.e. the morning rush.
  * Feeds them in batches of 20 to the same process() function the live consumer runs.

What it reports
  * Fare anomaly detector: precision and recall against the generator's hidden labels.
  * Completion model, scored on the whole stream after a 500-trip warm-up:
      - AUC of the test-then-train predictions;
      - log-loss of those predictions;
      - baseline log-loss of ONE constant prediction equal to the completion rate of that same
        evaluated segment (the best constant in hindsight, a slightly generous baseline).
    The live dashboard uses a different, stricter-for-the-baseline view: a rolling window of the
    last 1,000 trips, compared with the running completion rate known at each moment.

Run:  python tests/evaluate_models.py [n_trips] [seed ...]       (defaults: 20000, seeds 1 2 3)
"""

import importlib
import os
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("DATABASE_URL", "postgresql://unused")   # consumer_ml only connects in main()

import numpy as np  # noqa: E402
from sklearn.metrics import roc_auc_score  # noqa: E402

import consumer_ml  # noqa: E402
from common.generator import TripGenerator  # noqa: E402

START = datetime(2026, 11, 17, 7, 0, tzinfo=ZoneInfo("America/New_York")).timestamp()
WARMUP = 500


def evaluate(n: int, seed: int) -> dict:
    ml = importlib.reload(consumer_ml)           # fresh models for every seed
    gen = TripGenerator(seed=seed)
    truth, flagged, batch, probs, labels = {}, set(), [], [], []
    for i in range(n):
        trip = gen.next_trip(START + i * 0.5)
        truth[trip["trip_id"]] = gen.last_injected
        batch.append(trip)
        if len(batch) == 20:
            preds, anomalies, _ = ml.process(batch)
            probs += [p[4] for p in preds]
            labels += [int(p[5]) for p in preds]
            flagged |= {a["trip_id"] for a in anomalies}
            batch = []

    y = np.array(labels[WARMUP:])
    p = np.clip(np.array(probs[WARMUP:]), 1e-6, 1 - 1e-6)
    base = y.mean()
    injected = {k for k, v in truth.items() if v}
    tp = len(flagged & injected)
    return {
        "seed": seed, "injected": len(injected), "flagged": len(flagged),
        "precision": tp / max(1, len(flagged)), "recall": tp / max(1, len(injected)),
        "auc": roc_auc_score(y, p),
        "log_loss": float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p))),
        "baseline_log_loss": float(-(base * np.log(base) + (1 - base) * np.log(1 - base))),
        "completion_rate": float(base),
    }


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 20000
    seeds = [int(s) for s in sys.argv[2:]] or [1, 2, 3]
    print(f"{n} trips per seed from {datetime.fromtimestamp(START, ZoneInfo('America/New_York'))}, "
          f"metrics after a {WARMUP}-trip warm-up\n")
    print("| Seed | Anomalies injected | Flagged | Precision | Recall | Completion AUC "
          "| Log-loss (model / constant baseline) |")
    print("|---|---|---|---|---|---|---|")
    for s in seeds:
        r = evaluate(n, s)
        print(f"| {r['seed']} | {r['injected']} | {r['flagged']} | {r['precision']:.2f} | "
              f"{r['recall']:.2f} | {r['auc']:.3f} | {r['log_loss']:.3f} / {r['baseline_log_loss']:.3f} |")
