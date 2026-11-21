"""Part 5 - Streamlit dashboard for the Triangle Rides pipeline.

Reads Postgres only (never Kafka), so it can run on Streamlit Community Cloud against Supabase.
Everything below the header lives in a fragment that re-runs every REFRESH_SECONDS: no reload button.

Run locally:  streamlit run dashboard/app.py
"""

import os
from datetime import timedelta

import altair as alt
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

REFRESH_SECONDS = 5
LOCAL_TZ = "America/New_York"

# Palette (validated reference palette from the dataviz guide): categorical slots in fixed order.
BLUE, ORANGE, AQUA, YELLOW = "#2a78d6", "#eb6834", "#1baf7a", "#eda100"
CRITICAL, MUTED_MARK = "#d03b3b", "#c3c2b7"
INK, INK_2, MUTED, GRID, AXIS = "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7"
SERVICE_COLORS = alt.Scale(domain=["Standard", "Comfort", "XL", "Shared"],
                           range=[BLUE, ORANGE, AQUA, YELLOW])
SEQ_BLUE = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]

st.set_page_config(page_title="Triangle Rides - live", page_icon="🚕", layout="wide")


# --------------------------------------------------------------------------- data access
def database_url() -> str:
    url = None
    try:
        url = st.secrets.get("DATABASE_URL")
    except Exception:                                  # no secrets.toml locally
        pass
    url = url or os.getenv("DATABASE_URL") or "postgresql://rides:rides@localhost:5432/rides"
    # SQLAlchemy needs the driver named; Supabase gives a plain postgresql:// URL.
    for prefix in ("postgresql://", "postgres://"):
        if url.startswith(prefix):
            return "postgresql+psycopg://" + url[len(prefix):]
    return url


@st.cache_resource
def engine():
    # prepare_threshold=None: the Supabase transaction pooler cannot use prepared statements.
    return create_engine(database_url(), pool_pre_ping=True, pool_size=2,
                         connect_args={"prepare_threshold": None})


@st.cache_data(ttl=REFRESH_SECONDS - 1, show_spinner=False)
def query(sql: str, **params) -> pd.DataFrame:
    with engine().connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


def to_local(df: pd.DataFrame, *cols: str) -> pd.DataFrame:
    for c in cols:
        df[c] = pd.to_datetime(df[c], utc=True).dt.tz_convert(LOCAL_TZ).dt.tz_localize(None)
    return df


# --------------------------------------------------------------------------- chart styling
def styled(chart: alt.Chart, height: int = 260) -> alt.Chart:
    return (
        chart.properties(height=height)
        .configure(background="transparent", font="sans-serif")
        .configure_view(stroke=None)
        .configure_axis(labelColor=MUTED, titleColor=INK_2, gridColor=GRID, domainColor=AXIS,
                        tickColor=AXIS, labelFontSize=11, titleFontSize=11, titleFontWeight="normal")
        .configure_legend(labelColor=INK_2, titleColor=INK_2, orient="top", title=None,
                          labelFontSize=11, symbolType="square")
    )


def fmt_delta(now: float, before: float, pct: bool = False) -> str | None:
    if before is None or pd.isna(before) or pd.isna(now):
        return None
    d = now - before
    return f"{d * 100:+.1f} pts" if pct else f"{d:+,.1f}"


# --------------------------------------------------------------------------- header (static)
st.title("🚕 Triangle Rides — live ride-hailing stream")
st.caption(
    "Synthetic trips across Durham, Raleigh and Chapel Hill flow producer → Kafka `ride-trips` → "
    "consumers → Postgres → this page. Refreshes every "
    f"{REFRESH_SECONDS} s. Times are US Eastern."
)


@st.fragment(run_every=timedelta(seconds=REFRESH_SECONDS))
def live():
    try:
        status = query("SELECT max(event_time) AS last_event, count(*) AS n FROM trips")
    except Exception as exc:
        # Show which host we tried and the driver's reason, never the password.
        url = engine().url
        reason = str(getattr(exc, "orig", exc)).strip().splitlines()[0][:300]
        st.error(f"Cannot reach the database: {exc.__class__.__name__}\n\n"
                 f"Host tried: `{url.host}:{url.port}` (user `{url.username}`)\n\n"
                 f"Reason: {reason}\n\n"
                 "If the host is `localhost`, the DATABASE_URL secret was not found.")
        return
    if status["n"].iloc[0] == 0:
        st.info("No trips stored yet - start the producer and consumers (see README).")
        return

    anchor = pd.to_datetime(status["last_event"].iloc[0], utc=True)
    lag = (pd.Timestamp.now(tz="UTC") - anchor).total_seconds()
    if lag < 60:
        st.success(f"● Live — last trip {lag:.0f} s ago · {status['n'].iloc[0]:,} trips stored")
    else:
        st.warning(f"Pipeline paused — last trip {lag / 60:.0f} min ago. Showing the last "
                   f"30 minutes it recorded ({status['n'].iloc[0]:,} trips stored).")

    # ----------------------------------------------------------------- KPIs (last 15 min vs previous 15)
    kpi = query("""
        SELECT (event_time > :a - interval '15 minutes') AS current,
               count(*) AS trips,
               sum(fare_usd + tip_usd) AS revenue,
               avg(fare_usd) FILTER (WHERE status = 'completed') AS avg_fare,
               avg((status <> 'completed')::int) AS cancel_rate,
               avg(eta_min) AS eta
        FROM trips WHERE event_time > :a - interval '30 minutes'
        GROUP BY 1""", a=anchor).set_index("current")
    cur = kpi.loc[True] if True in kpi.index else None
    prev = kpi.loc[False] if False in kpi.index else pd.Series(dtype=float)
    alerts_15 = query("SELECT count(*) AS n FROM surge_alerts WHERE window_start > :a - interval '15 minutes'",
                      a=anchor)["n"].iloc[0]

    st.subheader("Last 15 minutes", help="Arrows compare the first five numbers with the 15 minutes "
                                         "before that. Zone alerts has no comparison.")
    c = st.columns(6)
    if cur is not None:
        c[0].metric("Trips", f"{int(cur.trips):,}", fmt_delta(cur.trips, prev.get("trips")))
        c[1].metric("Gross bookings", f"${cur.revenue:,.0f}", fmt_delta(cur.revenue, prev.get("revenue")),
                    help="Sum of fare + tip over all trips, including the $5 fee on some late rider "
                         "cancellations.")
        c[2].metric("Avg fare (completed)", f"${cur.avg_fare:,.2f}", fmt_delta(cur.avg_fare, prev.get("avg_fare")))
        c[3].metric("Cancellation rate", f"{cur.cancel_rate:.1%}",
                    fmt_delta(cur.cancel_rate, prev.get("cancel_rate"), pct=True), delta_color="inverse")
        c[4].metric("Avg pickup ETA", f"{cur.eta:.1f} min", fmt_delta(cur.eta, prev.get("eta")),
                    delta_color="inverse")
    c[5].metric("Zone alerts", f"{alerts_15}",
                help="Alerts fired by the stream processor for windows in the last 15 minutes: "
                     "high surge (avg ≥ 2.0×, ≥ 8 trips) or high cancellations (≥ 40%, ≥ 15 trips).")

    # ----------------------------------------------------------------- raw-stream charts
    left, right = st.columns(2)
    with left:
        st.markdown("**Trips per minute** · completed vs cancelled, last 30 full minutes")
        per_min = query("""
            SELECT date_trunc('minute', event_time) AS minute,
                   count(*) FILTER (WHERE status = 'completed') AS "Completed",
                   count(*) FILTER (WHERE status <> 'completed') AS "Cancelled"
            FROM trips WHERE event_time > :a - interval '30 minutes'
              AND event_time < date_trunc('minute', CAST(:a AS timestamptz))   -- skip the minute still filling
            GROUP BY 1 ORDER BY 1""", a=anchor)
        per_min = to_local(per_min, "minute").melt("minute", var_name="outcome", value_name="trips")
        chart = alt.Chart(per_min).mark_line(strokeWidth=2, point=alt.OverlayMarkDef(size=30)).encode(
            x=alt.X("minute:T", title=None, axis=alt.Axis(format="%H:%M", grid=False, tickMinStep=60_000)),
            y=alt.Y("trips:Q", title="trips / min"),
            color=alt.Color("outcome:N", scale=alt.Scale(domain=["Completed", "Cancelled"],
                                                         range=[BLUE, ORANGE])),
            tooltip=[alt.Tooltip("minute:T", format="%H:%M"), "outcome:N", "trips:Q"],
        )
        st.altair_chart(styled(chart), width="stretch")

    with right:
        st.markdown("**Gross bookings by pickup zone** · last 30 min")
        by_zone = query("""
            SELECT pickup_zone, sum(fare_usd + tip_usd) AS revenue, count(*) AS trips
            FROM trips WHERE event_time > :a - interval '30 minutes'
            GROUP BY 1 ORDER BY 2 DESC""", a=anchor)
        chart = alt.Chart(by_zone).mark_bar(color=BLUE, cornerRadiusEnd=4, height={"band": 0.7}).encode(
            y=alt.Y("pickup_zone:N", sort="-x", title=None, axis=alt.Axis(labelOverlap=False, labelLimit=170)),
            x=alt.X("revenue:Q", title="USD", axis=alt.Axis(format="$,.0f")),
            tooltip=["pickup_zone:N", alt.Tooltip("revenue:Q", format="$,.0f"), "trips:Q"],
        )
        st.altair_chart(styled(chart), width="stretch")

    left, right = st.columns(2)
    with left:
        st.markdown("**Fare distribution by service** · completed trips, last 30 min")
        fares = query("""
            SELECT service_type, fare_usd FROM trips
            WHERE status = 'completed' AND event_time > :a - interval '30 minutes'""", a=anchor)
        chart = alt.Chart(fares).mark_boxplot(size=26, extent="min-max", median={"color": "white"},
                                              rule={"color": MUTED}).encode(
            x=alt.X("service_type:N", title=None, sort=["Shared", "Standard", "Comfort", "XL"],
                    axis=alt.Axis(labelAngle=0)),
            y=alt.Y("fare_usd:Q", title="fare (USD, log scale)", scale=alt.Scale(type="log"),
                    axis=alt.Axis(values=[5, 10, 20, 50, 100, 200], format="$,.0f")),
            color=alt.Color("service_type:N", scale=SERVICE_COLORS, legend=None),
        )
        st.altair_chart(styled(chart), width="stretch")

    with right:
        st.markdown("**Pickup ETA vs cancellation** · all trips in the last 30 min, by ETA band")
        eta = query("""
            SELECT least(floor(eta_min / 2) * 2, 16) AS eta_band,
                   avg((status <> 'completed')::int) AS cancel_rate, count(*) AS trips
            FROM trips WHERE event_time > :a - interval '30 minutes'
            GROUP BY 1 ORDER BY 1""", a=anchor)
        eta["band"] = eta["eta_band"].map(lambda b: "16+ min" if b >= 16 else f"{b:.0f}–{b + 2:.0f} min")
        chart = alt.Chart(eta).mark_bar(color=ORANGE, cornerRadiusEnd=4).encode(
            x=alt.X("band:N", title="quoted pickup ETA", sort=list(eta["band"]), axis=alt.Axis(labelAngle=0)),
            y=alt.Y("cancel_rate:Q", title="cancelled", axis=alt.Axis(format=".0%")),
            tooltip=["band:N", alt.Tooltip("cancel_rate:Q", format=".1%"), "trips:Q"],
        )
        st.altair_chart(styled(chart), width="stretch")

    # ----------------------------------------------------------------- Part 4: derived stream
    st.divider()
    st.subheader("Stream processing — 1-minute windows per zone",
                 help="Computed by consumer_metrics.py from the stream and stored in zone_minute_metrics "
                      "(also published to the ride-zone-metrics topic).")
    left, right = st.columns([3, 2])
    with left:
        st.markdown("**Average surge multiplier** · zone × minute, last 30 min")
        windows = query("""
            SELECT window_start, pickup_zone, avg_surge, trips, cancelled, revenue_usd
            FROM zone_minute_metrics WHERE window_start > :a - interval '30 minutes'""", a=anchor)
        windows = to_local(windows, "window_start")
        # One discrete column per minute: a temporal x would stretch each rect to the next window.
        windows["minute"] = windows["window_start"].dt.strftime("%H:%M")
        minutes = sorted(windows["minute"].unique())
        chart = alt.Chart(windows).mark_rect(stroke="white", strokeWidth=1).encode(
            x=alt.X("minute:O", title=None, sort=minutes,
                    axis=alt.Axis(labelAngle=0, labelOverlap="parity", grid=False)),
            y=alt.Y("pickup_zone:N", title=None, axis=alt.Axis(labelOverlap=False, labelLimit=170)),
            color=alt.Color("avg_surge:Q", title="surge",
                            scale=alt.Scale(range=SEQ_BLUE, domain=[1, 3], clamp=True),
                            legend=alt.Legend(orient="right", format=".1f", gradientLength=160)),
            tooltip=["pickup_zone:N", "minute:N",
                     alt.Tooltip("avg_surge:Q", format=".2f", title="avg surge"), "trips:Q",
                     "cancelled:Q", alt.Tooltip("revenue_usd:Q", format="$,.0f", title="bookings")],
        )
        st.altair_chart(styled(chart, height=300), width="stretch")

    with right:
        st.markdown("**Alerts fired by the stream**")
        alerts = query("""
            SELECT window_start, rule, message FROM surge_alerts
            ORDER BY window_start DESC, alerted_at DESC LIMIT 12""")
        if alerts.empty:
            st.caption("No alerts yet. They fire when a zone's 1-minute window averages ≥ 2.0× surge "
                       "(min. 8 trips) or ≥ 40% cancellations (min. 15 trips).")
        else:
            alerts = to_local(alerts, "window_start")
            alerts["minute"] = alerts["window_start"].dt.strftime("%H:%M")
            alerts["type"] = alerts["rule"].map({"high_surge": "⚠️ surge",
                                                 "high_cancellations": "⛔ cancels"})
            st.dataframe(alerts[["minute", "type", "message"]], hide_index=True, width="stretch",
                         height=300, column_config={"minute": st.column_config.TextColumn(width="small"),
                                                    "type": st.column_config.TextColumn(width="small"),
                                                    "message": st.column_config.TextColumn(width="large")})

    # ----------------------------------------------------------------- Extension B: models
    st.divider()
    st.subheader("Sequential models — learned live from the stream",
                 help="consumer_ml.py: completion probability (logistic regression on a sliding window, "
                      "scored before it learns each outcome) and fare anomaly detection.")
    m = query("SELECT * FROM model_metrics ORDER BY recorded_at DESC LIMIT 240")
    if not m.empty:
        latest = m.iloc[0]
        c = st.columns(4)
        c[0].metric("Completion model AUC", "–" if pd.isna(latest.auc) else f"{latest.auc:.3f}",
                    help="Rolling, last 1,000 trips. 0.5 = no better than chance.")
        gain = 1 - latest.log_loss / latest.baseline_log_loss
        c[1].metric("Log-loss vs base rate", f"{latest.log_loss:.3f}", f"{gain:+.0%} better",
                    help=f"Baseline (always predict the running completion rate): {latest.baseline_log_loss:.3f}")
        c[2].metric("Trips scored", f"{int(latest.trips_seen):,}")
        n_anom = query("SELECT count(*) AS n FROM fare_anomalies")["n"].iloc[0]
        c[3].metric("Fare anomalies flagged", f"{n_anom:,}")

    left, mid, right = st.columns(3)
    with left:
        st.markdown("**Model log-loss over time** · lower is better")
        if m.empty:
            st.caption("Waiting for the first 100 scored trips…")
        else:
            mm = to_local(m.copy(), "recorded_at").rename(
                columns={"log_loss": "Model", "baseline_log_loss": "Base rate"})
            mm = mm.melt("recorded_at", ["Model", "Base rate"], var_name="series", value_name="log_loss")
            chart = alt.Chart(mm).mark_line(strokeWidth=2).encode(
                x=alt.X("recorded_at:T", title=None, axis=alt.Axis(format="%H:%M", grid=False, tickMinStep=60_000)),
                y=alt.Y("log_loss:Q", title="log-loss (rolling)", scale=alt.Scale(zero=False)),
                color=alt.Color("series:N", scale=alt.Scale(domain=["Model", "Base rate"],
                                                            range=[BLUE, MUTED_MARK])),
                tooltip=[alt.Tooltip("recorded_at:T", format="%H:%M:%S"), "series:N",
                         alt.Tooltip("log_loss:Q", format=".3f")],
            )
            st.altair_chart(styled(chart), width="stretch")

    with mid:
        st.markdown("**Calibration** · predicted vs actual completion, last 3,000 trips")
        cal = query("""
            SELECT least(floor(p_complete * 10) / 10, 0.9) AS bucket,
                   avg(p_complete) AS predicted, avg(completed::int) AS actual, count(*) AS trips
            FROM (SELECT * FROM completion_predictions ORDER BY event_time DESC LIMIT 3000) p
            GROUP BY 1 HAVING count(*) >= 10 ORDER BY 1""")
        if cal.empty:
            st.caption("Waiting for predictions…")
        else:
            diag = alt.Chart(pd.DataFrame({"x": [0, 1], "y": [0, 1]})).mark_line(
                color=AXIS, strokeDash=[4, 4]).encode(x="x:Q", y="y:Q")
            pts = alt.Chart(cal).mark_circle(color=BLUE, opacity=1, stroke="white", strokeWidth=2).encode(
                x=alt.X("predicted:Q", title="predicted P(complete)", scale=alt.Scale(domain=[0, 1]),
                        axis=alt.Axis(format=".0%")),
                y=alt.Y("actual:Q", title="actual completion rate", scale=alt.Scale(domain=[0, 1]),
                        axis=alt.Axis(format=".0%")),
                size=alt.Size("trips:Q", legend=None, scale=alt.Scale(range=[60, 400])),
                tooltip=[alt.Tooltip("predicted:Q", format=".1%"), alt.Tooltip("actual:Q", format=".1%"),
                         "trips:Q"],
            )
            st.altair_chart(styled(diag + pts), width="stretch")

    with right:
        st.markdown("**Fare vs distance** · anomalies in red")
        normal = query("""
            SELECT distance_km, fare_usd, service_type FROM trips
            WHERE status = 'completed' ORDER BY event_time DESC LIMIT 1500""")
        anom = query("""
            SELECT distance_km, fare_usd, service_type, reason, event_time FROM fare_anomalies
            ORDER BY event_time DESC LIMIT 50""")
        base = alt.Chart(normal).mark_circle(size=14, color=MUTED_MARK, opacity=0.6).encode(
            x=alt.X("distance_km:Q", title="distance (km, log scale)", scale=alt.Scale(type="log"),
                    axis=alt.Axis(values=[1, 2, 5, 10, 20, 50, 100, 200], grid=False)),
            y=alt.Y("fare_usd:Q", title="fare (USD, log scale)", scale=alt.Scale(type="log"),
                    axis=alt.Axis(values=[5, 10, 20, 50, 100, 200, 500], format="$,.0f")),
        )
        layers = base
        if not anom.empty:
            anom = to_local(anom, "event_time")
            layers = base + alt.Chart(anom).mark_point(
                shape="diamond", size=110, filled=True, color=CRITICAL, stroke="white", strokeWidth=1.5
            ).encode(x="distance_km:Q", y="fare_usd:Q",
                     tooltip=[alt.Tooltip("event_time:T", format="%H:%M:%S"), "service_type:N",
                              alt.Tooltip("fare_usd:Q", format="$,.2f"), "distance_km:Q", "reason:N"])
        st.altair_chart(styled(layers), width="stretch")

    if not query("SELECT 1 FROM fare_anomalies LIMIT 1").empty:
        recent = query("""
            SELECT event_time, pickup_zone, service_type, fare_usd, distance_km, speed_kmh, robust_z, reason
            FROM fare_anomalies ORDER BY event_time DESC LIMIT 8""")
        recent = to_local(recent, "event_time")
        recent["event_time"] = recent["event_time"].dt.strftime("%H:%M:%S")
        with st.expander("Latest flagged fares", expanded=False):
            st.dataframe(recent, hide_index=True, width="stretch")


live()
