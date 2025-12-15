import os
import json
import time
import random
import logging
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from kafka.admin import KafkaAdminClient

# ----------------------
# Logging configuration
# ----------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# ----------------------
# Kafka & devices setup
# ----------------------
BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")
TOPIC = os.getenv("TOPIC", "iot-data")
DEVICE_TYPES = ["thermostat", "humidity_sensor", "pressure_sensor", "gps_tracker"]

# ----------------------
# Wait for Kafka to be ready
# ----------------------
def wait_for_kafka(broker=BROKER, timeout=60):
    start = time.time()
    while time.time() - start < timeout:
        try:
            admin = KafkaAdminClient(bootstrap_servers=broker, api_version=(2,8,0))
            admin.list_topics()
            logger.info("Kafka is ready ✅")
            return True
        except Exception:
            logger.warning("Waiting for Kafka to be ready...")
            time.sleep(2)
    raise RuntimeError("Kafka broker did not become ready in time")

# ----------------------
# Producer creation
# ----------------------
def create_producer():
    while True:
        try:
            logger.info(f"Connecting to Kafka at {BROKER}...")
            producer = KafkaProducer(
                bootstrap_servers=BROKER,
                api_version=(2, 8, 0),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                linger_ms=500,
                retries=100,                  # retry many times
                retry_backoff_ms=2000,        # wait 2s between retries
                request_timeout_ms=10000,     # timeout per request
            )
            logger.info("Kafka producer connected ✅")
            return producer
        except NoBrokersAvailable:
            logger.warning("Kafka not ready, retrying in 5 seconds...")
            time.sleep(5)

# ----------------------
# Device data generation
# ----------------------
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
        val = {
            "lat": round(48 + random.uniform(-0.1, 0.1), 6),
            "lon": round(2 + random.uniform(-0.1, 0.1), 6),
        }

    return {
        "device_id": f"device_{device_id}",
        "device_type": dtype,
        "timestamp": ts,
        "value": val,
    }

# ----------------------
# Main loop
# ----------------------
def run(num_devices=30, interval=0.2):
    wait_for_kafka()                  # <- wait until Kafka is ready
    producer = create_producer()
    device_ids = list(range(1, num_devices + 1))

    while True:
        msg = gen_device_reading(random.choice(device_ids))
        try:
            producer.send(TOPIC, msg)
            producer.flush()  # ensure message is delivered immediately
            logger.info(f"Sent message: {msg}")
        except Exception as e:
            logger.error(f"Failed to send message: {e}")
            try:
                producer.flush()  # attempt to flush pending messages
            except Exception as flush_e:
                logger.error(f"Failed to flush messages: {flush_e}")
        time.sleep(interval)

# ----------------------
# Entry point
# ----------------------
if __name__ == "__main__":
    run()

