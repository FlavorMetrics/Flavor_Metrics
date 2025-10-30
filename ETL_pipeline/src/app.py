# app.py — Beer production live KPIs
import glob
from pathlib import Path
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

RAW_ROOT = Path("data/raw")
MART_ROOT = Path("data/marts")
ENGINE = "fastparquet"

st.set_page_config(page_title="Beer Production KPIs", layout="wide")

# --- sidebar controls ---
st.sidebar.title("Filters")
sel_date = st.sidebar.date_input("Date", value=date.today())
auto_refresh = st.sidebar.selectbox("Auto-refresh", ["5s", "10s", "30s", "Off"], index=1)
refresh_ms = {"5s": 5000, "10s": 10000, "30s": 30000, "Off": 0}[auto_refresh]
if refresh_ms:
    st_autorefresh(interval=refresh_ms, key="autorefresh")

view_mode = st.sidebar.radio(
    "Show",
    ["All data", "Only anomalies"],
    index=0,
)

# --- data loaders ---
@st.cache_data(ttl=10, show_spinner=False)
def load_raw_for_date(d: date) -> pd.DataFrame | None:
    # our consumer_beer_parquet.py writes: data/raw/date=YYYY-MM-DD/beer-*.parquet
    part_dir = RAW_ROOT / f"date={d.isoformat()}"
    parts = sorted(glob.glob(str(part_dir / "*.parquet")))
    if not parts:
        return None
    df = pd.concat([pd.read_parquet(p, engine=ENGINE) for p in parts], ignore_index=True)
    df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts"])
    return df

@st.cache_data(ttl=10, show_spinner=False)
def load_kpi_for_date(d: date) -> pd.DataFrame | None:
    # kpi_beer_minute.py writes: data/marts/beer_kpi_date=YYYY-MM-DD.parquet
    f = MART_ROOT / f"beer_kpi_date={d.isoformat()}.parquet"
    if not f.exists():
        return None
    df = pd.read_parquet(f, engine=ENGINE)
    # minute should already be datetime, but make sure
    if "minute" in df.columns:
        df["minute"] = pd.to_datetime(df["minute"], utc=True, errors="coerce")
    return df

# --- main ---
st.title("🍺 Beer Production — Live KPIs")

raw_df = load_raw_for_date(sel_date)
kpi_df = load_kpi_for_date(sel_date)

if raw_df is None or raw_df.empty:
    st.info(
        f"No raw parts found in {RAW_ROOT}/date={sel_date.isoformat()}/ yet. "
        "Keep the beer simulator & consumer running to generate data."
    )
else:
    # dynamic filters
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        plant = st.selectbox("Plant", sorted(raw_df["plant_id"].dropna().unique()))
    with c2:
        line = st.selectbox("Line", sorted(raw_df["line_id"].dropna().unique()))
    with c3:
        step = st.selectbox("Step", sorted(raw_df["step"].dropna().unique()))
    with c4:
        sensor = st.selectbox("Sensor", sorted(raw_df["sensor"].dropna().unique()))

    df_sel = raw_df.query(
        "plant_id == @plant and line_id == @line and step == @step and sensor == @sensor"
    ).copy()

    df_sel["ts"] = pd.to_datetime(df_sel["ts"], utc=True, errors="coerce")
    df_sel = df_sel.dropna(subset=["ts"])
    
    # --- anomaly detection (last 5 minutes) ---
    if view_mode == "Only anomalies":

        now_utc = datetime.now(timezone.utc)
        window_start = now_utc - timedelta(minutes=5)

    
        recent = df_sel[df_sel["ts"] >= window_start]
        recent_oos = recent[recent["in_spec"] == False]

        if not recent_oos.empty:
            st.error(f"⚠️ {len(recent_oos)} out-of-spec readings in the last 5 minutes.")
        else:
            st.success("✅ No out-of-spec readings in the last 5 minutes.")
    
        st.subheader("Recent anomalies")
        if not recent_oos.empty:
            show_cols = [
                "ts",
                "value",
                "unit",
                "step",
                "sensor",
                "plant_id",
                "line_id",
                "batch_id",
            ]
            st.dataframe(
                recent_oos.sort_values("ts", ascending=False)[show_cols].head(30),
                use_container_width=True,
                height=250,
            )
        st.stop()

    if df_sel.empty:
        st.warning("No data after applying filters.")
    else:
        # if KPI file exists, use it (it already has per-minute aggregates)
        if kpi_df is not None and not kpi_df.empty:
            kpi_sel = kpi_df.query(
                "step == @step and sensor == @sensor"
            ).sort_values("minute")
        else:
            # fallback: compute per-minute KPI from this filtered raw
            df_sel["value"] = pd.to_numeric(df_sel["value"], errors="coerce")
            df_sel = df_sel.dropna(subset=["value"])
            df_sel["minute"] = df_sel["ts"].dt.floor("min")
            kpi_sel = (
                df_sel.groupby(["plant_id", "line_id", "step", "sensor", "minute"])
                .agg(
                    readings=("value", "count"),
                    mean_value=("value", "mean"),
                    oos_rate=("in_spec", lambda x: 1 - x.mean()),
                )
                .reset_index()
                .sort_values("minute")
            )

        latest_row = kpi_sel.iloc[-1] if not kpi_sel.empty else None

        m1, m2, m3 = st.columns(3)
        with m1:
            st.metric(
                "Readings (last minute)",
                int(latest_row["readings"]) if latest_row is not None else 0,
            )
        with m2:
            st.metric(
                "Mean value (last minute)",
                f"{latest_row['mean_value']:.2f}" if latest_row is not None else "—",
            )
        with m3:
            st.metric(
                "OOS rate (last minute)",
                f"{(latest_row['oos_rate']*100):.1f}%"
                if latest_row is not None
                else "—",
            )

        left, right = st.columns((2, 1))
        with left:
            st.subheader("Mean value over time")
            if not kpi_sel.empty:
                chart_df = kpi_sel.set_index("minute")[["mean_value"]]
                st.line_chart(chart_df)
            else:
                st.info("No KPI rows yet for this filter.")
        with right:
            st.subheader("OOS rate over time")
            if not kpi_sel.empty:
                chart_df2 = kpi_sel.set_index("minute")[["oos_rate"]]
                st.line_chart(chart_df2)
            else:
                st.info("No KPI rows yet for this filter.")

        st.subheader("Latest raw readings")
        tail = df_sel.sort_values("ts").tail(50)
        cols = [
            "ts",
            "value",
            "unit",
            "in_spec",
            "plant_id",
            "line_id",
            "step",
            "sensor",
            "batch_id",
        ]
        st.dataframe(tail[cols], use_container_width=True, height=300)
