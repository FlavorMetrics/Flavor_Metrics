import json
import paho.mqtt.client as mqtt

BROKER_HOST = "localhost"
BROKER_PORT = 1883
TOPIC = "factory/line1/sensors"

SOFT_LOWER = 72.0
SOFT_UPPER = 78.0

def on_message(client, userdata, msg):
    try:
        rec = json.loads(msg.payload.decode("utf-8"))
        val = float(rec["value"])
        in_spec = SOFT_LOWER <= val <= SOFT_UPPER
        print(f"{rec['ts']}  {rec['step']}.{rec['param']}={val}°{rec['unit']}  in_spec={in_spec}")
    except Exception as ex:
        print("[WARN] bad message:", ex)

client = mqtt.Client()
client.on_message = on_message
client.connect(BROKER_HOST, BROKER_PORT, 60)
client.subscribe(TOPIC, qos=0)
print(f"validating on {TOPIC} …  (Ctrl+C to stop)")
try:
    client.loop_forever()
except KeyboardInterrupt:
    print("\nstopping validator…")
finally:
    client.disconnect()
    print("bye!")
