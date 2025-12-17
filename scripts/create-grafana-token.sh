#!/bin/sh
set -e

INFLUX_ORG="streaminsight"
INFLUX_TOKEN="admin-token"
BUCKET_NAME="iot_bucket"
OUTPUT_FILE="/influx-secrets/grafana-token.txt"

echo "Waiting for InfluxDB to be ready..."

# Wait for InfluxDB API to respond
for i in $(seq 1 30); do
    if influx ping -t "$INFLUX_TOKEN" >/dev/null 2>&1; then
        echo "InfluxDB is up"
        break
    fi
    echo "InfluxDB not ready, waiting..."
    sleep 2
done

# Wait for the bucket to exist
for i in $(seq 1 30); do
    BUCKET_ID=$(influx bucket list -o "$INFLUX_ORG" -t "$INFLUX_TOKEN" 2>/dev/null \
        | awk -v name="$BUCKET_NAME" '$2 == name {print $1}')

    if [ -n "$BUCKET_ID" ]; then
        echo "Found bucket '$BUCKET_NAME' with ID: $BUCKET_ID"
        break
    fi

    echo "Waiting for bucket '$BUCKET_NAME'..."
    sleep 2
done

if [ -z "$BUCKET_ID" ]; then
    echo "ERROR: Could not find bucket '$BUCKET_NAME' after waiting"
    exit 1
fi

echo "Creating Grafana read token..."
influx auth create \
    -o "$INFLUX_ORG" \
    -t "$INFLUX_TOKEN" \
    -d grafana-token \
    --read-bucket "$BUCKET_ID" \
    > "$OUTPUT_FILE"

echo "Grafana token written to $OUTPUT_FILE"
