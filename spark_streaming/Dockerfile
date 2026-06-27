FROM bitnami/spark:3.5.0

USER root
WORKDIR /opt/job

# Copia el job de streaming
COPY streaming_job.py /opt/job/streaming_job.py
COPY entrypoint.sh /opt/job/entrypoint.sh
RUN chmod +x /opt/job/entrypoint.sh

# Pre-descarga de los conectores (Kafka + Elasticsearch) en la imagen para
# evitar dependencia de red durante el arranque del contenedor.
ENV SPARK_KAFKA_PKG="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
ENV SPARK_ES_PKG="org.elasticsearch:elasticsearch-spark-30_2.12:8.13.4"
ENV IVY_DIR="/opt/bitnami/spark/.ivy2"

RUN mkdir -p $IVY_DIR && \
    /opt/bitnami/spark/bin/spark-shell \
      --packages ${SPARK_KAFKA_PKG},${SPARK_ES_PKG} \
      --conf spark.jars.ivy=$IVY_DIR \
      -i /dev/null 2>/dev/null || true

ENTRYPOINT ["/opt/job/entrypoint.sh"]
