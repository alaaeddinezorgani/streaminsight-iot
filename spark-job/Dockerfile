FROM python:3.11-slim

# Install dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
        tar \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy pre-downloaded Corretto 17 tarball into the image
COPY amazon-corretto-17-x64-linux-jdk.tar.gz /tmp/

# Extract and set up JAVA_HOME
RUN mkdir -p /opt/java \
    && tar -xzf /tmp/amazon-corretto-17-x64-linux-jdk.tar.gz -C /opt/java \
    && rm /tmp/amazon-corretto-17-x64-linux-jdk.tar.gz \
    && ln -s /opt/java/$(ls /opt/java) /opt/java/current

ENV JAVA_HOME=/opt/java/current
ENV PATH="$JAVA_HOME/bin:$PATH"

# Install Python dependencies
RUN pip install --no-cache-dir pyspark==3.4.1 kafka-python influxdb-client

WORKDIR /app
COPY spark_job.py /app/

CMD ["python", "spark_job.py"]

