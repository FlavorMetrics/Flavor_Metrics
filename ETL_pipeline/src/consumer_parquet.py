# consumer_parquet.py
import json, os, time, signal, sys
from pathlib import Path
from datetime import datetime, date
import pandas as pd
import paho.mqtt.client as mqtt

# ---- config ----
ENGINE = "fastparquet"
BROKER = "localhost"
BROKER_PORT = 1883
TOPIC       = "factory/beer/sensors"
ROOT = Path("data/raw")
ROOT.mkdir(parents=True, exist_ok=True)


# simple spec lookup: (step, sensor) → (low, high)
SPECS = {
    ("mashing", "temp"): (62, 68),
    ("boiling", "temp"): (98, 101),
    ("fermentation", "temp"): (18, 22),
    ("fermentation", "gravity"): (1.010, 1.030),
    ("packaging", "count"): (80, 120),
}


BUF = []
FLUSH_EVERY = 20

def check_spec(step, sensor, value):
    key = (step, sensor)
    if key not in SPECS:
        return True  # if we don't know spec, don't fail
    low, high = SPECS[key]
    return low <= value <= high

def on_message(client, userdata, msg):
    global BUF
    data = json.loads(msg.payload.decode("utf-8"))
    step = data["step"]
    sensor = data["sensor"]
    val = float(data["value"])
    data["in_spec"] = check_spec(step, sensor, val)
    BUF.append(data)
    if len(BUF) >= FLUSH_EVERY:
        flush()

def flush():
    global BUF
    if not BUF:
        return
    df = pd.DataFrame(BUF)
    # partition by date
    today = date.today().isoformat()
    outdir = ROOT / f"date={today}"
    outdir.mkdir(parents=True, exist_ok=True)
    ts = int(time.time() * 1000)
    outpath = outdir / f"beer-{ts}.parquet"
    df.to_parquet(outpath, engine="fastparquet")
    print(f"[flush] wrote {len(df)} rows → {outpath}")
    BUF = []

client = mqtt.Client()
client.on_message = on_message
client.connect(BROKER, 1883, 60)
client.subscribe(TOPIC)
print("listening for beer telemetry …")
client.loop_forever()



