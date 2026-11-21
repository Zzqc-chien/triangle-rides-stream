# 🚕 Triangle Rides — a real-time ride-hailing pipeline

**Live dashboard:** https://triangle-rides.streamlit.app/

The dashboard updates while the producer and consumers are running. If ingestion stops, it keeps
showing the last recorded 30 minutes under a "Pipeline paused" banner.

Triangle Rides simulates ride demand around Durham, Raleigh and Chapel Hill. When the generator
simulates a burst of pickups, for example a Duke game letting out, the dashboard shows surge
pricing and cancellations in that zone changing minute by minute.

## Architecture

```mermaid
flowchart LR
    G["generator.py<br/>synthetic trips"] --> P["producer.py"]
    P -- "key = pickup_zone" --> T[("Kafka topic<br/>ride-trips<br/>3 partitions")]

    T -- "group: trip-storage" --> S["consumer_storage.py"]
    T -- "group: zone-metrics" --> M["consumer_metrics.py<br/>1-min tumbling windows<br/>+ alert rules"]
    T -- "group: trip-ml" --> ML["consumer_ml.py<br/>completion model<br/>+ fare anomaly detector"]

    S --> DB1[("trips")]
    M --> DB2[("zone_minute_metrics<br/>surge_alerts<br/>stream_offsets")]
    M --> T2[("ride-zone-metrics<br/>ride-surge-alerts")]
    ML --> DB3[("completion_predictions<br/>model_metrics<br/>fare_anomalies")]
    ML --> T3[("ride-fare-anomalies")]

    subgraph PG["Postgres (local container or Supabase)"]
        DB1
        DB2
        DB3
    end

    PG --> D["Streamlit dashboard<br/>auto-refresh every 5 s<br/>(Streamlit Community Cloud)"]
```

* **Topic:** `ride-trips` (3 partitions) carries raw trips. Three derived topics carry results:
  `ride-zone-metrics`, `ride-surge-alerts` and `ride-fare-anomalies`.
* **Message key: `pickup_zone`.** All trips from one zone go to the same partition in order, so
  the per-zone windows and alerts see each zone's trips in sequence, and a scaled-out consumer
  group splits work by whole zones.
* **Consumer groups:** `trip-storage`, `zone-metrics` and `trip-ml`. Each consumer has its own
  group, so each one receives every trip.
* **Delivery:** a consumer commits its Kafka offset only after its database transaction commits
  (at-least-once). If it crashes between the two, Kafka re-delivers the batch.
  * Raw trip inserts are deduplicated by `trip_id` (`ON CONFLICT DO NOTHING`), and so are
    predictions, anomalies and alerts.
  * The zone aggregates are sums, so a replay would inflate them. To prevent that,
    `consumer_metrics.py` stores the last offset it has counted per partition in
    `stream_offsets`, in the same transaction as the sums, and skips anything at or below it.
    `tests/e2e_fake_kafka.py` replays every message and checks that the totals do not change.
  * The ML consumer's in-memory models can learn from a replayed batch twice. Its stored
    predictions are not duplicated.

## Quick start (Docker only, no local Python needed)

```bash
cp .env.example .env                          # first run only; defaults use the local Postgres container
docker compose --profile apps up -d --build   # Kafka, topics, Kafka UI, Postgres, producer, 3 consumers, dashboard
```

* Dashboard: http://localhost:8501
* Kafka UI (topics, consumer groups, messages): http://localhost:8080
* Rows growing: `docker exec postgres psql -U rides -c "select count(*) from trips"`
* Logs: `docker compose --profile apps logs -f producer consumer-metrics consumer-ml`
* Pause / resume the pipeline: `docker compose --profile apps stop` / `start`
* Stop and wipe everything: `docker compose --profile apps down -v`

Manual Python runs, Supabase and cloud deployment are covered [further down](#other-ways-to-run).

## Data source

The data is synthetic, produced by [`common/generator.py`](common/generator.py). Each event is
one finished trip, either completed or cancelled.

| Mechanic | How it is modelled |
|---|---|
| Zones | 10 pickup zones, each with a sampling weight (its share of pickups) and a typical pickup ETA. RDU Airport has the longest ETAs, downtown the shortest |
| Trip length | In-zone trips are about 3 km. Cross-zone distances are estimated from zone coordinates (great-circle distance × 1.3 road factor × small noise). Speeds drop during rush hours (US Eastern), which changes durations and fares |
| Surge | Per zone, a slowly drifting random process |
| Demand events | A Duke game, RDU arrivals, a concert downtown and similar. The first comes 40–90 s after the producer starts. Each later one starts 150–300 s after the previous one ends and lasts 90–150 s. An event multiplies one zone's sampling weight by 2.5–3.2, which raises its share of pickups, and lifts its surge to 2.1–2.8× |
| Sending rate | Poisson arrivals at `EVENTS_PER_SEC` (default 2/s), scaled between 0.6× and 1.6× by time of day. Demand events change *where* trips come from, not the total rate |
| Cancellations | Probability is logistic in pickup ETA, surge and service type (Shared riders cancel more). It does not depend on time of day |
| Fares | Per-service price card: base + $/km + $/min, a minimum fare, ±5% route noise, × surge, plus occasional tolls or airport fees |
| Anomalies | About 0.4% of completed trips are corrupted: a billing glitch (fare × 4–9) or a GPS glitch (distance × 6–15). The labels never leave the generator |

The producer runs until it is stopped.

### Event schema (14 fields)

| Field | Type | Example | Notes |
|---|---|---|---|
| `trip_id` | string (UUID) | `b2af6281-…` | **unique id** |
| `event_time` | ISO-8601 UTC | `2026-11-20T21:19:52.331+00:00` | **timestamp**, set by the producer |
| `pickup_zone` | string | `Duke West Campus` | **category**; message key |
| `dropoff_zone` | string | `Downtown Durham` | category |
| `service_type` | string | `Standard` / `Comfort` / `XL` / `Shared` | category |
| `status` | string | `completed` / `rider_cancelled` / `driver_cancelled` | category |
| `distance_km` | float | `12.4` | **numeric**; the quoted route for cancelled trips |
| `duration_min` | float | `14.8` | 0 for cancelled trips |
| `eta_min` | float | `4.9` | quoted pickup ETA |
| `surge_multiplier` | float | `1.0` – `3.5` | |
| `fare_usd` | float | `18.20` | **numeric**; a $5 fee on some late rider cancellations |
| `tip_usd` | float | `3.10` | |
| `payment_method` | string | `card` / `apple_pay` / `google_pay` / `paypal` | |
| `driver_rating` | int or null | `5` | null when cancelled |

### Database tables

| Table | Written by | Contents |
|---|---|---|
| `trips` | consumer_storage | one row per trip, one column per event field, plus `ingested_at` |
| `zone_minute_metrics` | consumer_metrics | per zone and minute: trips, completed, cancelled, bookings, avg surge, avg ETA |
| `surge_alerts` | consumer_metrics | alerts fired by the two rules below |
| `stream_offsets` | consumer_metrics | last Kafka offset per partition already counted into the aggregates |
| `completion_predictions` | consumer_ml | P(complete) for each trip record, made before the model learns its outcome |
| `model_metrics` | consumer_ml | rolling AUC, log-loss and accuracy vs. baselines |
| `fare_anomalies` | consumer_ml | flagged trips with z-score and reason |

The schema is in [`common/schema.sql`](common/schema.sql); the consumers create it on start-up.
Row-level security is on for every table, so Supabase's public REST API cannot read them. The
pipeline's own Postgres role is not affected.

## Stream processing (Part 4)

`consumer_metrics.py` assigns each trip to a **one-minute tumbling window per pickup zone** by
event time. While a window is still filling, it upserts the running totals into
`zone_minute_metrics` and publishes them to `ride-zone-metrics`. After every update it checks
two rules:

* `high_surge`: average surge ≥ 2.0× over at least 8 trips
* `high_cancellations`: at least 40% of trips cancelled, over at least 15 trips

Each rule fires at most once per zone and window. The alert goes to `surge_alerts` and
`ride-surge-alerts`.

**How well the rules track the simulated events.** `python tests/evaluate_alerts.py` runs the
same aggregation and rule code on 2 hours of simulated trips at 2/s for each of two seeds. The
start is fixed at Fri 2026-11-20 17:00 US Eastern. An alert for zone Z and minute W counts as
*coinciding* with a demand event when an event in the same zone Z was active at some point
between one minute before W and the end of W.

| Seed | Alerts fired | Coinciding with an event | Demand events | Events with an alert |
|---|---|---|---|---|
| 3 | 59 | 59 (100%) | 22 | 21 (95%) |
| 4 | 70 | 69 (99%) | 21 | 21 (100%) |
| all | 129 | 128 (99%) | 43 | 42 (98%) |

The two rates answer different questions. 128 of 129 alerts coincided with a simulated demand
event in their zone under these settings, and 42 of 43 events produced at least one alert. The
thresholds were tuned on this simulation. An earlier version required only 8 trips for the
cancellation rule, and it fired on chance clusters of cancellations in small windows.

## Extension B: sequential modeling

Both models live in `consumer_ml.py` and learn while the stream runs. Nothing is pre-trained.

**1. Completion probability.** The consumer scores each incoming trip record before learning from
its outcome (test-then-train). The model is a logistic regression, refitted every 200 trips on a
sliding window of the latest 4,000. Its features are pickup zone, service type, quoted ETA,
surge, route distance, and local time of day and weekday. Two limits:

* The stream carries finished trips. It has no separate request event, so this is not a
  prediction served at request time. It is an honest out-of-sample score for each trip, made
  before its outcome is used.
* In a real system the features would be known at request time. Here, `distance_km` on about
  0.4% of completed trips has been altered by the simulated GPS glitch.

**2. Fare anomaly detection.** For each service type, a recursive-least-squares regression learns
the price curve `fare / surge ≈ b0 + b1·km + b2·min` from the stream, with a learned
minimum-fare floor. A completed trip is flagged when its log fare is more than 6 robust z-scores
(median/MAD over the last 300 residuals) from that curve, or when its implied speed exceeds
130 km/h, which points to a GPS glitch. Flagged trips are kept out of the model's updates.

### Results

`python tests/evaluate_models.py` feeds 20,000 trips per seed (0.5 s apart, from a fixed start of
Tue 2026-11-17 07:00 US Eastern) through the same `process()` function the live consumer runs.

| Seed | Anomalies injected | Flagged | Precision | Recall | Completion AUC | Log-loss (model / constant baseline) |
|---|---|---|---|---|---|---|
| 1 | 68 | 68 | 1.00 | 1.00 | 0.701 | 0.343 / 0.379 |
| 2 | 68 | 68 | 1.00 | 1.00 | 0.704 | 0.345 / 0.383 |
| 3 | 69 | 68 | 1.00 | 0.99 | 0.716 | 0.337 / 0.378 |

Evaluation protocol:
* Anomaly precision and recall are scored against the generator's hidden labels.
* Completion metrics cover the whole stream after a 500-trip warm-up.
* The baseline is one constant prediction equal to that segment's completion rate, the best
  constant in hindsight.
* The dashboard uses a different measure: a rolling window of the last 1,000 trips, compared
  with the running completion rate known at each moment.

**Discussion.**
* The first anomaly detector had a precision of 0.16. Fares in the simulation are nearly
  deterministic, so the residual spread was tiny, and ordinary $1–1.50 tolls crossed the
  threshold. Flooring the spread at 5% in log terms fixed that. So did using the 2nd percentile
  of recent normal fares as the expected minimum fare on short trips, which a straight line
  cannot fit.
* The first completion model used per-batch `SGDClassifier.partial_fit` and scored worse than
  the baseline. It was replaced with the sliding-window refit.
* The model ends up about 10% below the constant baseline in log-loss, with AUC ≈ 0.70.
  Cancellations depend on ETA and surge, which the model sees. The rest is random, which caps
  AUC. The time-of-day features carry no signal in this data, because the generator's
  cancellation probability does not depend on time.
* About 87% of trips complete, so accuracy is not informative, and the dashboard shows log-loss
  and calibration instead.

## Live demo record

[`docs/live_demo.md`](docs/live_demo.md) records a run of the real pipeline: real producer, real
Kafka broker and real consumers, writing to the Supabase database behind the public dashboard. It
lists the start and end times, the `trips` row count sampled every 30 s, a zone window updated
during the run, and end-to-end latency. The latency is measured per trip as
`ingested_at - event_time`, from producer to Postgres, with one traced `trip_id`. The dashboard
adds up to one 5 s refresh on top. To record a new run while the pipeline is running:

```bash
docker compose --profile apps build
docker compose --profile apps run --rm -v "$PWD/docs:/app/docs" producer python scripts/live_demo_check.py
```

## Dashboard

| Section | What it shows |
|---|---|
| Status | Live / paused banner, time since the last trip, total trips stored |
| Last 15 minutes | **Trips**, **gross bookings** (fare + tip over all trips, including cancellation fees), **average fare** of completed trips, **cancellation rate** and **average pickup ETA**, each with an arrow against the previous 15 minutes. **Zone alerts** counts both alert rules for windows in the last 15 minutes and has no comparison |
| Raw-stream charts | trips per minute (completed vs cancelled), bookings by pickup zone, fare distribution by service, cancellation rate by ETA band |
| Stream processing (Part 4) | surge heatmap of zone × minute from `zone_minute_metrics`; latest alerts from `surge_alerts` |
| Sequential models (Extension B) | rolling AUC and log-loss vs base rate, calibration plot, fare-vs-distance with anomalies highlighted, latest flagged fares |

![Dashboard screenshot](docs/dashboard.png)

## Other ways to run

### Python scripts on your laptop

```bash
docker compose up -d                       # Kafka + UI + Postgres only
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt            # Python 3.10–3.13
cp .env.example .env

python producer.py                         # one terminal each
python consumer_storage.py
python consumer_metrics.py
python consumer_ml.py
streamlit run dashboard/app.py
```

### Point the pipeline at Supabase

The public dashboard runs in the cloud and cannot reach a database on a laptop, so the consumers
write to a hosted Postgres.

1. In your Supabase project, open **Connect** and copy the **Session pooler** string (IPv4, port
   5432). The direct connection is IPv6-only, and Streamlit Cloud cannot reach it.
2. Put it in `.env` as `DATABASE_URL=postgresql://postgres.<ref>:<password>@aws-…pooler.supabase.com:5432/postgres`.
   Use a password of letters and digits only. Docker Compose and URLs treat `$`, `@`, `#` and
   `/` specially.
3. Restart the apps: `docker compose --profile apps up -d --force-recreate`. The consumers create
   the tables in Supabase on start-up.

### Deploy the dashboard (Streamlit Community Cloud)

1. Push the repository to GitHub. `.env` and `secrets.toml` are git-ignored.
2. On share.streamlit.io, choose **Create app**, then this repo, branch `main` and main file
   `dashboard/app.py`. The build installs `dashboard/requirements.txt`.
3. Under **Advanced settings → Secrets**, add one TOML line, quotes included:
   `DATABASE_URL = "postgresql://postgres.<ref>:<password>@aws-…pooler.supabase.com:5432/postgres"`.
   After you change a secret, use **Reboot app**, because the database connection is cached.

> **Free-tier note:** at 2 trips/s the pipeline writes about 170k trips a day. Supabase's free
> tier holds 500 MB, so pause the pipeline when you are not demoing, or clear old data with
> `TRUNCATE trips, completion_predictions;`.

## Tests

```bash
pytest -q                                   # event schema, windowing, alert rules, anomaly detector
python tests/evaluate_models.py             # model results table (fixed seeds and start time)
python tests/evaluate_alerts.py             # alert results table (fixed seeds and start time)
DATABASE_URL=postgresql://rides:rides@localhost:5432/rides \
  python tests/e2e_fake_kafka.py 120 5      # whole pipeline + replay check, with an in-memory Kafka stand-in
```

`e2e_fake_kafka.py` swaps the Kafka client for an in-memory stand-in, so it shows that the
application logic works end to end. It does not test a real broker. For that, see the live demo
record above.

## Repository layout

```
├── producer.py              Part 1   synthetic trips → ride-trips
├── consumer_storage.py      Part 2-3 ride-trips → trips table
├── consumer_metrics.py      Part 4   1-min windows + alert rules → tables + topics (replay-safe)
├── consumer_ml.py           Ext. B   completion model + fare anomaly detector
├── common/
│   ├── generator.py         the event generator (zones, surge, demand events, fares)
│   ├── schema.sql           all tables
│   ├── db.py, kafka_utils.py, config.py
├── dashboard/
│   ├── app.py               Part 5   Streamlit dashboard
│   └── requirements.txt     slim requirements for Streamlit Cloud
├── scripts/
│   └── live_demo_check.py   records a live run into docs/live_demo.md
├── tests/                   unit tests, evaluations, end-to-end run with replay check
├── docs/                    dashboard screenshot, live demo record
├── docker-compose.yml       Kafka (KRaft) + topics + Kafka UI + Postgres + apps
├── Dockerfile
├── requirements.txt
└── .env.example
```
