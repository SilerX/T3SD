import json
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


class QueryProducer:
    def __init__(self, bootstrap_servers, topic):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = self._connect()

    def _connect(self, max_retries=30, backoff=2):
        for i in range(max_retries):
            try:
                p = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    acks='all',
                    retries=5,
                    linger_ms=10,
                )
                print(f"[OK] Kafka producer conectado (intento {i+1})")
                return p
            except NoBrokersAvailable:
                print(f"[..] Esperando Kafka (intento {i+1}/{max_retries})")
                time.sleep(backoff)
        raise RuntimeError("No se pudo conectar a Kafka")

    def send(self, query):
        payload = query.to_dict() if hasattr(query, 'to_dict') else query
        key = payload.get('id')
        self.producer.send(self.topic, key=key, value=payload)

    def flush(self):
        self.producer.flush()

    def close(self):
        self.producer.flush()
        self.producer.close()
