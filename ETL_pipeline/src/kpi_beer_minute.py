# kpi_beer_minute.py
from pathlib import Path
import pandas as pd
from datetime import date
import sys

RAW_ROOT = Path("data/raw")
MART_ROOT = Path("data/marts")

def build_for(day: str):
    dpath = RAW_ROOT / f"date={day}"
    parts = list(dpath.glob("*.parquet"))
    if not parts:
        print("no data")
        return
    df = pd.concat([pd.read_parquet(p) for p in parts], ignore_index=True)
    df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts"])
    df["minute"] = df["ts"].dt.floor("min")

    # basic KPI
    agg = (
        df.groupby(["minute", "step", "sensor"])
          .agg(
              readings=("value", "count"),
              in_spec_rate=("in_spec", "mean"),
              avg_value=("value", "mean"),
              total_value=("value", "sum"),
          )
          .reset_index()
          .sort_values("minute")
    )

    MART_ROOT.mkdir(parents=True, exist_ok=True)
    outpath = MART_ROOT / f"beer_kpi_date={day}.parquet"
    agg.to_parquet(outpath, engine="fastparquet")
    print(f"wrote KPIs → {outpath} (rows={len(agg)})")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        day = sys.argv[1]
    else:
        day = date.today().isoformat()
    build_for(day)
