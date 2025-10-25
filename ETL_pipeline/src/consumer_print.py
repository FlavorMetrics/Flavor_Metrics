import json
import paho.mqtt.client as mqtt

BROKER_HOST = "localhost"
BROKER_PORT = 1883
TOPIC = "factory/line1/sensors"

def on_message(client, userdate, msg):
    print(json.loads(msg.payload.decode("utf-8")))

client = mqtt.Client()
client.on_message = on_message
client.connect(BROKER_HOST, BROKER_PORT, 60)
client.subscribe(TOPIC, qos=0)
print(f"listening on {TOPIC} …")
client.loop_forever()
