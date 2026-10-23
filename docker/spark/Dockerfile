FROM python:3.12-slim

LABEL maintainer="Digital Payments Lakehouse" \
      description="Apache Spark 3.5.3 with Delta Lake 3.2.0 and MinIO S3A for Lakehouse Ingestion"

ENV JAVA_HOME=/usr/lib/jvm/default-java \
    SPARK_HOME=/usr/local/lib/python3.12/site-packages/pyspark \
    PYTHONPATH=/app/src:$PYTHONPATH \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    DEBIAN_FRONTEND=noninteractive

# Install Java JRE 17, curl, procps
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        default-jre-headless \
        curl \
        procps && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python Lakehouse dependencies
RUN pip install --no-cache-dir \
    pyspark==3.5.3 \
    delta-spark==3.2.0 \
    pyyaml \
    confluent-kafka \
    pyarrow \
    faker \
    pydantic \
    pytest

# Pre-fetch required Spark Maven packages during image build so startup is instant and offline-ready
RUN python3 -c "\
from pyspark.sql import SparkSession; \
spark = SparkSession.builder.master('local[1]') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262') \
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
    .getOrCreate(); \
spark.stop()"

# Copy cached jars from ivy cache to pyspark jars for offline and instant execution
RUN cp -r /root/.ivy2/jars/* /usr/local/lib/python3.12/site-packages/pyspark/jars/ 2>/dev/null || true

CMD ["tail", "-f", "/dev/null"]
