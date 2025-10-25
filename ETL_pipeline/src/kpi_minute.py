import sys, glob
from pathlib import Path
import pandas as pd

ENGINE = "fastparquet"
RAW_ROOT = Path("data/raw")
MART_ROOT = Path("data/marts"); MART_ROOT.mkdir(parents=True, exist_ok=True)

if len(sys.argv) < 2:
    print("Usage: python kpi_minute.py YYYY-MM-DD"); sys.exit(1)
date = sys.argv[1]

parts = sorted(glob.glob(str(RAW_ROOT / f"date={date}" / "part-*.parquet")))
if not parts:
    print(f"No raw parts for {date}"); sys.exit(0)

df = pd.concat([pd.read_parquet(p, engine=ENGINE) for p in parts], ignore_index=True)
df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
df["minute"] = df["ts"].dt.floor("T")

kpi = (df.groupby(["plant_id","line_id","step","param","minute"])
         .agg(readings=("value","count"),
              mean_value=("value","mean"),
              oos_rate=("in_spec", lambda x: 1 - x.mean()))
         .reset_index())

out = MART_ROOT / f"kpi_minute_date={date}.parquet"
kpi.to_parquet(out, index=False, engine=ENGINE)
print(f"Wrote KPIs → {out} (rows={len(kpi)})")
