"""Offline evaluation of consumer_ml.py on the generator, using its ground-truth anomaly labels.

Feeds N synthetic trips (0.5 s apart in simulated time) through the same `process()` function the
live consumer uses, in batches of 20, and reports detection precision/recall and the rolling
completion-model metrics.

Run:  python tests/evaluate_models.py [n_trips] [seed ...]
"""

import importlib
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import numpy as np  # noqa: E402
from sklearn.metrics import roc_auc_score  # noqa: E402

import consumer_ml  # noqa: E402
from common.generator import TripGenerator  # noqa: E402


def evaluate(n: int, seed: int) -> dict:
    ml = importlib.reload(consumer_ml)           # fresh models for every seed
    gen, t0 = TripGenerator(seed=seed), time.time()
    truth, flagged, batch, last, probs, labels = {}, set(), [], None, [], []
    for i in range(n):
        trip = gen.next_trip(t0 + i * 0.5)
        truth[trip["trip_id"]] = gen.last_injected
        batch.append(trip)
        if len(batch) == 20:
            preds, anomalies, metrics = ml.process(batch)
            probs += [p[4] for p in preds]
            labels += [int(p[5]) for p in preds]
            flagged |= {a["trip_id"] for a in anomalies}
            last = metrics or last
            batch = []
    # Whole-stream prequential scores, skipping the first 500 trips (model warm-up).
    y, p = np.array(labels[500:]), np.clip(np.array(probs[500:]), 1e-6, 1 - 1e-6)
    base = y.mean()
    stream = {"auc_all": roc_auc_score(y, p),
              "ll_all": float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p))),
              "ll_base_all": float(-(base * np.log(base) + (1 - base) * np.log(1 - base)))}
    injected = {k for k, v in truth.items() if v}
    tp = len(flagged & injected)
    return {**stream, "seed": seed, "injected": len(injected), "flagged": len(flagged),
            "precision": tp / max(1, len(flagged)), "recall": tp / max(1, len(injected)),
            "auc": last["auc"], "log_loss": last["log_loss"],
            "baseline_log_loss": last["baseline_log_loss"],
            "accuracy": last["accuracy"], "baseline_accuracy": last["baseline_accuracy"]}


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 20000
    seeds = [int(s) for s in sys.argv[2:]] or [1, 2, 3]
    for s in seeds:
        r = evaluate(n, s)
        print(f"seed {r['seed']}: anomalies injected={r['injected']} flagged={r['flagged']} "
              f"precision={r['precision']:.2f} recall={r['recall']:.2f} | completion model, whole stream: "
              f"AUC={r['auc_all']:.3f} log-loss={r['ll_all']:.3f} (constant base rate {r['ll_base_all']:.3f})")
