import os
import json
import time
import random
from kafka import KafkaProducer

BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")
TOPIC = os.getenv("TOPIC", "iot-data")

producer = KafkaProducer(
    bootstrap_servers=os.environ["KAFKA_BROKER"],
    api_version=(2, 8, 0),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=10,
    linger_ms=500
)

DEVICE_TYPES = ["thermostat", "humidity_sensor", "pressure_sensor", "gps_tracker"]

def gen_device_reading(device_id):
    dtype = random.choice(DEVICE_TYPES)
    ts = int(time.time() * 1000)
    if dtype == "thermostat":
        val = round(random.uniform(15, 30), 2)
    elif dtype == "humidity_sensor":
        val = round(random.uniform(20, 90), 2)
    elif dtype == "pressure_sensor":
        val = round(random.uniform(980, 1030), 2)
    else:
        val = {"lat": round(48 + random.uniform(-0.1,0.1),6),
               "lon": round(2 + random.uniform(-0.1,0.1),6)}
    payload = {
        "device_id": f"device_{device_id}",
        "device_type": dtype,
        "timestamp": ts,
        "value": val
    }
    return payload

def run(num_devices=20, interval=0.5):
    device_ids = list(range(1, num_devices+1))
    while True:
        dev = random.choice(device_ids)
        msg = gen_device_reading(dev)
        producer.send(TOPIC, msg)
        producer.flush()
        time.sleep(interval)

if __name__ == "__main__":
    run(num_devices=30, interval=0.2)

