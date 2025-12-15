FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends build-essential && \
    pip install --no-cache-dir kafka-python

WORKDIR /app
COPY producer.py /app/
CMD ["python", "producer.py"]

