import json, random, time
from datetime import datetime, timezone
import paho.mqtt.client as mqtt

BROKER_HOST = "localhost"
BROKER_PORT = 1883
TOPIC = "factory/line1/sensors"

def sample_value():
    base, noise = 75.0, 1.5
    v = random.gauss(base, noise)
    if random.random() < 0.05:  # occasional outlier
        v += random.choice([-1, 1]) * 5
    return round(v, 2)


client = mqtt.Client()
client.connect(BROKER_HOST, BROKER_PORT, 60)

batch_id = f"batch-{int(time.time())}"
print("publishing… Ctrl+C to stop")
while True:
    payload = {
        "plant_id": "plantA",
        "line_id": "line1",
        "step": "pasteurization",
        "param": "temp",
        "value": sample_value(),
        "unit": "C",
        "ts": datetime.now(timezone.utc).isoformat(),
        "batch_id": batch_id
    }
    client.publish(TOPIC, json.dumps(payload), qos=0)
    time.sleep(0.5)

