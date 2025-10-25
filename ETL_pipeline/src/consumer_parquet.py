# consumer_parquet.py
import json, os, time, signal, sys
from pathlib import Path
from datetime import datetime
import pandas as pd
import paho.mqtt.client as mqtt

# ---- config ----
ENGINE = "fastparquet"
BROKER_HOST = "localhost"
BROKER_PORT = 1883
TOPIC       = "factory/line1/sensors"

SOFT_LOWER  = 72.0   # in-spec band for temperature (C)
SOFT_UPPER  = 78.0

BUFFER_MAX  = 200    # flush when buffer reaches this many rows
FLUSH_SECS  = 10     # also flush every N seconds (time-based)

DATA_DIR    = Path("data/raw")  # root for raw parquet partitions
DATA_DIR.mkdir(parents=True, exist_ok=True)

BUFFER = []
_last_flush = time.time()

def _date_of(rec_ts: str) -> str:
    # rec_ts like "2025-10-13T20:10:03.265234+00:00"
    # safest: parse then format; fast enough for demo
    try:
        dt = datetime.fromisoformat(rec_ts.replace("Z", "+00:00"))
        return dt.date().isoformat()
    except Exception:
        # fallback: first 10 chars (YYYY-MM-DD)
        return rec_ts[:10]

def _flush(force: bool = False):
    """Write current BUFFER to parquet as a new part-file."""
    global BUFFER, _last_flush
    if not BUFFER:
        return
    if (not force) and (len(BUFFER) < BUFFER_MAX) and (time.time() - _last_flush < FLUSH_SECS):
        return

    df = pd.DataFrame(BUFFER)
    # group by date so one flush can write multiple date partitions if needed
    for date_str, grp in df.groupby(df["ts"].map(_date_of)):
        out_dir = DATA_DIR / f"date={date_str}"
        out_dir.mkdir(parents=True, exist_ok=True)
        part_name = f"part-{int(time.time() * 1000)}.parquet"
        out_path = out_dir / part_name
        grp.to_parquet(out_path, index=False, engine=ENGINE)
        print(f"[flush] wrote {len(grp)} rows → {out_path}")

    BUFFER = []
    _last_flush = time.time()

def on_message(client, userdata, msg):
    global BUFFER
    try:
        rec = json.loads(msg.payload.decode("utf-8"))
        # compute in_spec for temp; if you add more params, you can branch by rec['param']
        val = float(rec["value"])
        rec["in_spec"] = (SOFT_LOWER <= val <= SOFT_UPPER)
        BUFFER.append(rec)
        _flush(force=False)  # size/time-based
    except Exception as ex:
        print("[WARN] dropped bad message:", ex)

def _graceful_exit(signum, frame):
    print("\n[signal] flushing and exiting…")
    _flush(force=True)
    try:
        client.disconnect()
    except Exception:
        pass
    sys.exit(0)

# register signal handlers for clean shutdown
signal.signal(signal.SIGINT,  _graceful_exit)  # Ctrl+C
signal.signal(signal.SIGTERM, _graceful_exit)

client = mqtt.Client()  # works for paho-mqtt 1.x and 2.x
client.on_message = on_message
client.connect(BROKER_HOST, BROKER_PORT, 60)
client.subscribe(TOPIC, qos=0)
print(f"listening & buffering on {TOPIC} …  (Ctrl+C to stop)")

try:
    client.loop_forever()
finally:
    _flush(force=True)
