FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk wget && rm -rf /var/lib/apt/lists/*

RUN pip install pyspark kafka-python influxdb-client

WORKDIR /app
COPY spark_job.py /app/

CMD ["python", "spark_job.py"]

