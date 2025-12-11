
import os
import json
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, window, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")
INFLUX_URL = os.getenv("INFLUX_URL", "http://influxdb:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "admin:adminpass")
INFLUX_ORG = os.getenv("INFLUX_ORG", "streaminsight")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "iot_bucket")
TOPIC = "iot-data"

# Start Spark session (local mode)
spark = SparkSession.builder \
    .appName("iot_stream_agg") \
    .master("local[2]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
    .getOrCreate()

schema = StructType([
    StructField("device_id", StringType()),
    StructField("device_type", StringType()),
    StructField("timestamp", LongType()),  # milliseconds
    StructField("value", StringType())
])

# Read streaming data from Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# value is bytes -> cast to string then parse JSON
raw = df.selectExpr("CAST(value AS STRING) as json_str")
parsed = raw.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

# Convert value to double
def parse_value(v):
    try:
        return float(v)
    except Exception:
        return None

parse_value_udf = udf(parse_value, DoubleType())
parsed = parsed.withColumn("value_num", parse_value_udf(col("value")))

# Convert bigint timestamp (ms) to TimestampType for Spark
parsed = parsed.withColumn("timestamp_ts", to_timestamp((col("timestamp")/1000).cast("double")))

# Windowed aggregation: average per device_type over 30s windows
from pyspark.sql.types import TimestampType

# convert bigint timestamp (ms) to Spark TimestampType
parsed_ts = parsed.withColumn("ts", (col("timestamp") / 1000).cast(TimestampType()))

agg = parsed_ts.withWatermark("ts", "60 seconds") \
    .groupBy(window("ts", "30 seconds"), col("device_type")) \
    .agg(expr("avg(value_num) as avg_value"), expr("count(*) as cnt"))


# Write aggregated rows to InfluxDB
def write_to_influx(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    pdf = batch_df.toPandas()
    from influxdb_client import InfluxDBClient, Point, WritePrecision
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = client.write_api()
    for _, row in pdf.iterrows():
        window_start = row["window"].start if hasattr(row["window"], "start") else row["window"]["start"]
        pt = Point("iot_agg") \
            .tag("device_type", row["device_type"]) \
            .field("avg_value", float(row["avg_value"]) if row["avg_value"] is not None else 0.0) \
            .field("count", int(row["cnt"])) \
            .time(window_start, WritePrecision.S)
        write_api.write(INFLUX_BUCKET, INFLUX_ORG, pt)
    client.close()

query = agg.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_influx) \
    .option("checkpointLocation", "/tmp/spark_checkpoints") \
    .start()

query.awaitTermination()
