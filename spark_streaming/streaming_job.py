"""
Job de Apache Spark Structured Streaming - Tarea 3 Sistemas Distribuidos.

Pipeline:
    metrics-topic (Kafka)  -->  Spark Structured Streaming  -->  Elasticsearch  -->  Kibana

El job:
  1. Lee el stream de eventos de metrica desde el topico `metrics-topic`.
  2. Parsea los mensajes JSON con un esquema explicito.
  3. Aplica ventanas de tiempo DESLIZANTES (sliding windows) con watermark para
     tolerar datos que llegan ligeramente desordenados.
  4. Calcula por ventana:
        - throughput        : consultas exitosas por minuto (hit + miss_ok)
        - latency_p50_ms    : mediana de latencia de consultas exitosas
        - latency_p95_ms    : percentil 95 de latencia de consultas exitosas
        - hit_rate          : proporcion de cache hits sobre consultas resueltas
        - retry_rate        : proporcion de eventos que terminaron en reintento
        - dlq_count         : cantidad de consultas enviadas a la DLQ
        - total_events / ok_events / retry_events
  5. Escribe los resultados agregados en un indice de Elasticsearch.

Variables de entorno relevantes:
    KAFKA_BOOTSTRAP   (def: kafka:9092)
    TOPIC_METRICS     (def: metrics-topic)
    ES_NODES          (def: elasticsearch)
    ES_PORT           (def: 9200)
    ES_INDEX          (def: metrics-aggregated)
    WINDOW_DURATION   (def: 60 seconds)
    SLIDE_DURATION    (def: 30 seconds)
    WATERMARK         (def: 2 minutes)
    TRIGGER_INTERVAL  (def: 10 seconds)
    CHECKPOINT_DIR    (def: /tmp/spark-checkpoint)
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, count, sum as spark_sum,
    expr, when, round as spark_round, lit, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, BooleanType
)


# ---------------------------------------------------------------------------
# Configuracion
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_METRICS = os.getenv("TOPIC_METRICS", "metrics-topic")
ES_NODES = os.getenv("ES_NODES", "elasticsearch")
ES_PORT = os.getenv("ES_PORT", "9200")
ES_INDEX = os.getenv("ES_INDEX", "metrics-aggregated")
WINDOW_DURATION = os.getenv("WINDOW_DURATION", "60 seconds")
SLIDE_DURATION = os.getenv("SLIDE_DURATION", "30 seconds")
WATERMARK = os.getenv("WATERMARK", "2 minutes")
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "10 seconds")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoint")


# Esquema explicito de los eventos emitidos por el Sistema de Metricas
EVENT_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("consumer_id", StringType(), True),
    StructField("tipo", StringType(), True),
    StructField("zona", StringType(), True),
    StructField("outcome", StringType(), True),       # hit | miss_ok | retry | dlq
    StructField("retry_count", IntegerType(), True),
    StructField("retried", BooleanType(), True),
    StructField("latencia_ms", DoubleType(), True),
    StructField("created_at", DoubleType(), True),
    StructField("ts", DoubleType(), True),
    StructField("event_time", StringType(), True),    # ISO-8601
])


def build_spark():
    return (
        SparkSession.builder
        .appName("T3-MetricsStreaming")
        .config("spark.sql.shuffle.partitions", "4")
        .config("es.nodes", ES_NODES)
        .config("es.port", ES_PORT)
        .config("es.nodes.wan.only", "true")
        .config("es.index.auto.create", "true")
        .getOrCreate()
    )


def read_stream(spark):
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC_METRICS)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), EVENT_SCHEMA).alias("e"))
        .select("e.*")
        .withColumn("event_time", to_timestamp(col("event_time")))
    )
    return parsed


def aggregate(parsed):
    """Agregaciones por ventana temporal deslizante."""
    is_ok = col("outcome").isin("hit", "miss_ok")

    aggregated = (
        parsed
        .withWatermark("event_time", WATERMARK)
        .groupBy(window(col("event_time"), WINDOW_DURATION, SLIDE_DURATION))
        .agg(
            count(lit(1)).alias("total_events"),
            spark_sum(when(is_ok, 1).otherwise(0)).alias("ok_events"),
            spark_sum(when(col("outcome") == "hit", 1).otherwise(0)).alias("hit_events"),
            spark_sum(when(col("outcome") == "miss_ok", 1).otherwise(0)).alias("miss_ok_events"),
            spark_sum(when(col("outcome") == "retry", 1).otherwise(0)).alias("retry_events"),
            spark_sum(when(col("outcome") == "dlq", 1).otherwise(0)).alias("dlq_count"),
            # percentiles solo sobre consultas exitosas
            expr("percentile_approx(CASE WHEN outcome IN ('hit','miss_ok') THEN latencia_ms END, 0.5)").alias("latency_p50_ms"),
            expr("percentile_approx(CASE WHEN outcome IN ('hit','miss_ok') THEN latencia_ms END, 0.95)").alias("latency_p95_ms"),
        )
    )

    # Duracion de la ventana en minutos para normalizar el throughput
    win_minutes = _window_minutes()

    result = (
        aggregated
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop("window")
        .withColumn("throughput_per_min", spark_round(col("ok_events") / lit(win_minutes), 2))
        .withColumn(
            "hit_rate",
            spark_round(
                when(col("ok_events") > 0, col("hit_events") / col("ok_events")).otherwise(0.0), 4
            ),
        )
        .withColumn(
            "retry_rate",
            spark_round(
                when(col("total_events") > 0, col("retry_events") / col("total_events")).otherwise(0.0), 4
            ),
        )
        .withColumn("latency_p50_ms", spark_round(col("latency_p50_ms"), 2))
        .withColumn("latency_p95_ms", spark_round(col("latency_p95_ms"), 2))
        .withColumn("ingested_at", current_timestamp())
    )
    return result


def _window_minutes():
    # WINDOW_DURATION viene como "60 seconds" o "1 minute"
    parts = WINDOW_DURATION.split()
    val = float(parts[0])
    unit = parts[1].lower() if len(parts) > 1 else "seconds"
    if unit.startswith("second"):
        return val / 60.0
    if unit.startswith("minute"):
        return val
    return val / 60.0


def write_to_es(batch_df, batch_id):
    """Escribe cada micro-batch agregado en Elasticsearch.

    Se usa un id deterministico por ventana (window_start + window_end) para que
    las actualizaciones de una misma ventana sobre-escriban el documento previo
    en lugar de duplicarlo (modo de salida `update`).
    """
    count_rows = batch_df.count()
    print(f"[Spark] batch {batch_id}: {count_rows} ventanas -> Elasticsearch", flush=True)
    if count_rows == 0:
        return
    out = batch_df.withColumn(
        "doc_id",
        expr("concat(cast(window_start as string), '_', cast(window_end as string))")
    )
    (
        out.write
        .format("org.elasticsearch.spark.sql")
        .option("es.nodes", ES_NODES)
        .option("es.port", ES_PORT)
        .option("es.nodes.wan.only", "true")
        .option("es.mapping.id", "doc_id")
        .option("es.write.operation", "upsert")
        .mode("append")
        .save(ES_INDEX)
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    print(f"[Spark] Leyendo de Kafka topic={TOPIC_METRICS} bootstrap={KAFKA_BOOTSTRAP}", flush=True)
    print(f"[Spark] Ventana={WINDOW_DURATION} slide={SLIDE_DURATION} watermark={WATERMARK}", flush=True)
    print(f"[Spark] Escribiendo en ES index={ES_INDEX} nodes={ES_NODES}:{ES_PORT}", flush=True)

    parsed = read_stream(spark)
    result = aggregate(parsed)

    query = (
        result.writeStream
        .outputMode("update")
        .foreachBatch(write_to_es)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
