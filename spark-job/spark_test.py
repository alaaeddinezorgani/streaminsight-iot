from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, LongType

# Influx environment variables
INFLUX_URL = "http://influxdb:8086"
INFLUX_TOKEN = "admin-token"
INFLUX_BUCKET = "iot_bucket"
INFLUX_ORG = "streaminsight"

# Kafka settings
KAFKA_BROKER = "redpanda:9092"
TOPIC = "iot-data"

# Define schema of incoming JSON
schema = StructType() \
    .add("device_id", StringType()) \
    .add("device_type", StringType()) \
    .add("timestamp", LongType()) \
    .add("value", DoubleType())

# Spark session
spark = SparkSession.builder.appName("KafkaToInfluxTest").getOrCreate()

# Read from Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON
json_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

def write_to_influx(batch_df, batch_id):
    import requests
    print(f"Processing batch {batch_id} with {batch_df.count()} rows")
    for row in batch_df.collect():
        payload = f'iot_measurement,device_type={row.device_type},device_id={row.device_id} value={row.value}'
        try:
            resp = requests.post(f"{INFLUX_URL}/api/v2/write?org={INFLUX_ORG}&bucket={INFLUX_BUCKET}&precision=s",
                                 headers={"Authorization": f"Token {INFLUX_TOKEN}"},
                                 data=payload)
            print(f"Sent payload: {payload}, response: {resp.status_code}, {resp.text}")
        except Exception as e:
            print(f"Error writing to Influx: {e}")




json_df.writeStream.foreachBatch(write_to_influx).start().awaitTermination()

