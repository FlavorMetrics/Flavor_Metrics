# app.py — Live KPI viewer for MQTT -> Parquet pipeline
import glob
from pathlib import Path
from datetime import datetime, date, timezone

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

RAW_ROOT = Path("data/raw")
ENGINE = "fastparquet"   # we installed fastparquet; change to "pyarrow" if you switch

st.set_page_config(page_title="Manufacturing Live KPIs", layout="wide")

# --- sidebar controls ---
st.sidebar.title("Filters")
sel_date = st.sidebar.date_input("Date", value=date.today())
auto_refresh = st.sidebar.selectbox("Auto-refresh", ["5s", "10s", "30s", "Off"], index=1)
refresh_ms = {"5s": 5000, "10s": 10000, "30s": 30000, "Off": 0}[auto_refresh]
if refresh_ms:
   # st.experimental_rerun  # just to keep mypy happy
   # st.runtime.legacy_caching.clear_cache()  # reduce stale cache (optional)
   # st.experimental_set_query_params(ts=int(datetime.now().timestamp()))  # bust state (optional)
    st_autorefresh(interval=refresh_ms, key="autorefresh")

# --- data loaders ---
@st.cache_data(ttl=10, show_spinner=False)
def load_raw_for_date(d: date) -> pd.DataFrame | None:
    part_dir = RAW_ROOT / f"date={d.isoformat()}"
    parts = sorted(glob.glob(str(part_dir / "part-*.parquet")))
    if not parts:
        return None
    df = pd.concat([pd.read_parquet(p, engine=ENGINE) for p in parts], ignore_index=True)
    # normalize
    df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts"])
    return df

@st.cache_data(ttl=10, show_spinner=False)
def compute_kpi_minute(df: pd.DataFrame) -> pd.DataFrame:
    # robust typing
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["value"])
    df["minute"] = df["ts"].dt.floor("min")
    kpi = (df.groupby(["plant_id", "line_id", "step", "param", "minute"])
             .agg(readings=("value","count"),
                  mean_value=("value","mean"),
                  oos_rate=("in_spec", lambda x: 1 - x.mean()))
             .reset_index())
    return kpi.sort_values("minute")

# --- main ---
st.title("🏭 Live Manufacturing KPIs (per minute)")
raw_df = load_raw_for_date(sel_date)

if raw_df is None or raw_df.empty:
    st.info(f"No raw parts found in {RAW_ROOT}/date={sel_date.isoformat()}/ yet. "
            "Keep the simulator & consumer running to generate data.")
else:
    # dynamic filters based on available values
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        plant = st.selectbox("Plant", sorted(raw_df["plant_id"].dropna().unique()))
    with c2:
        line = st.selectbox("Line", sorted(raw_df["line_id"].dropna().unique()))
    with c3:
        step = st.selectbox("Step", sorted(raw_df["step"].dropna().unique()))
    with c4:
        param = st.selectbox("Param", sorted(raw_df["param"].dropna().unique()))

    # filter
    df_sel = raw_df.query(
        "plant_id == @plant and line_id == @line and step == @step and param == @param"
    ).copy()

    if df_sel.empty:
        st.warning("No data after applying filters.")
    else:
        kpi = compute_kpi_minute(df_sel)

        # top KPIs
        latest_row = kpi.iloc[-1] if not kpi.empty else None
        m1, m2, m3 = st.columns(3)
        with m1:
            st.metric("Readings (last minute)", int(latest_row["readings"]) if latest_row is not None else 0)
        with m2:
            st.metric("Mean value (last minute)", 
                      f"{latest_row['mean_value']:.2f}" if latest_row is not None else "—")
        with m3:
            st.metric("OOS rate (last minute)", 
                      f"{(latest_row['oos_rate']*100):.1f}%" if latest_row is not None else "—")

        # charts
        left, right = st.columns((2,1))
        with left:
            st.subheader("Mean value over time")
            if not kpi.empty:
                chart_df = kpi.set_index("minute")[["mean_value"]]
                st.line_chart(chart_df)
            else:
                st.info("No KPI rows yet for this filter.")
        with right:
            st.subheader("OOS rate over time")
            if not kpi.empty:
                chart_df2 = kpi.set_index("minute")[["oos_rate"]]
                st.line_chart(chart_df2)
            else:
                st.info("No KPI rows yet for this filter.")

        # recent raw tail
        st.subheader("Latest raw readings")
        tail = df_sel.sort_values("ts").tail(50)
        # pick friendly columns
        cols = ["ts","value","unit","in_spec","plant_id","line_id","step","param","batch_id"]
        st.dataframe(tail[cols], use_container_width=True, height=300)
