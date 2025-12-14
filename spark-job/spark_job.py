import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, LongType
from influxdb_client import InfluxDBClient, Point, WritePrecision

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")
TOPIC = "iot-data"

INFLUX_URL = os.getenv("INFLUX_URL", "http://influxdb:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "admin-token")
INFLUX_ORG = os.getenv("INFLUX_ORG", "streaminsight")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "iot_bucket")

spark = SparkSession.builder.appName("iot_stream").getOrCreate()

schema = StructType([
    StructField("device_id", StringType()),
    StructField("device_type", StringType()),
    StructField("value", DoubleType()),
    StructField("timestamp", LongType())
])

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

def write_to_influx(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = client.write_api()
    for row in batch_df.collect():
        pt = Point("iot_measurements") \
            .tag("device_type", row.device_type) \
            .field("value", row.value) \
            .time(row.timestamp, WritePrecision.MS)
        write_api.write(INFLUX_BUCKET, INFLUX_ORG, pt)
    client.close()

json_df.writeStream.foreachBatch(write_to_influx).start().awaitTermination()

