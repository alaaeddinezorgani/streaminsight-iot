import os
import json
import time
import random
from kafka import KafkaProducer

BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")
TOPIC = os.getenv("TOPIC", "iot-data")

producer = KafkaProducer(
    bootstrap_servers=BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

DEVICE_TYPES = ["thermostat", "humidity_sensor", "pressure_sensor"]

def gen_data():
    return {
        "device_id": f"device_{random.randint(1,5)}",
        "device_type": random.choice(DEVICE_TYPES),
        "value": round(random.uniform(20, 30), 2),
        "timestamp": int(time.time() * 1000)
    }

while True:
    data = gen_data()
    producer.send(TOPIC, data)
    producer.flush()
    print("Sent:", data)
    time.sleep(1)

