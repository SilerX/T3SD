import json
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


class MetricsPublisher:
    """Publica cada evento de metrica en el topico dedicado `metrics-topic`.

    Este componente desacopla el plano de procesamiento (consultas) del plano
    de observabilidad (metricas): el servicio de metricas ya no calcula
    agregaciones, solo emite eventos crudos estructurados que luego son
    procesados por Apache Spark Structured Streaming.
    """

    def __init__(self, bootstrap_servers, topic):
        self.topic = topic
        self.producer = self._connect(bootstrap_servers)

    def _connect(self, bootstrap, max_retries=30):
        for i in range(max_retries):
            try:
                p = KafkaProducer(
                    bootstrap_servers=bootstrap,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    acks='all',
                    retries=5,
                    linger_ms=10,
                )
                print(f"[OK] MetricsPublisher conectado a Kafka (intento {i+1})", flush=True)
                return p
            except NoBrokersAvailable:
                print(f"[..] Esperando Kafka para metrics-topic (intento {i+1}/{max_retries})", flush=True)
                time.sleep(2)
        print("[WARN] MetricsPublisher no pudo conectar a Kafka; se omite la publicacion")
        return None

    def publish(self, event):
        if self.producer is None:
            return
        # event_time en formato ISO-8601 para que Spark lo parsee como timestamp
        ts = float(event.get('ts', time.time()))
        enriched = dict(event)
        enriched['event_time'] = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
        try:
            self.producer.send(self.topic, key=event.get('id'), value=enriched)
        except Exception as e:
            print(f"[WARN] No se pudo publicar evento en metrics-topic: {e}", flush=True)

    def close(self):
        if self.producer is not None:
            self.producer.flush()
            self.producer.close()
