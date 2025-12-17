FROM spark-base:3.4.1

WORKDIR /app
COPY spark_test.py /app/

CMD ["spark-submit", "--jars", "/opt/spark/jars/*", "/app/spark_test.py"]
