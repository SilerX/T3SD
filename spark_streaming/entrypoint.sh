#!/usr/bin/env bash
set -e

KAFKA_PKG="${SPARK_KAFKA_PKG:-org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0}"
ES_PKG="${SPARK_ES_PKG:-org.elasticsearch:elasticsearch-spark-30_2.12:8.13.4}"
IVY_DIR="${IVY_DIR:-/opt/bitnami/spark/.ivy2}"

echo "[entrypoint] Esperando a Kafka (${KAFKA_BOOTSTRAP:-kafka:9092}) y Elasticsearch (${ES_NODES:-elasticsearch}:${ES_PORT:-9200})..."
sleep "${STARTUP_DELAY:-25}"

exec /opt/bitnami/spark/bin/spark-submit \
  --master "local[*]" \
  --packages "${KAFKA_PKG},${ES_PKG}" \
  --conf spark.jars.ivy="${IVY_DIR}" \
  --conf spark.sql.streaming.metricsEnabled=true \
  /opt/job/streaming_job.py
