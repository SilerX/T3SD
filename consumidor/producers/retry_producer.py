import json
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


class RetryProducer:
    def __init__(self, bootstrap_servers, topic_retry, topic_dlq):
        self.topic_retry = topic_retry
        self.topic_dlq = topic_dlq
        self.producer = self._connect(bootstrap_servers)

    def _connect(self, bootstrap, max_retries=30):
        for i in range(max_retries):
            try:
                return KafkaProducer(
                    bootstrap_servers=bootstrap,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    acks='all',
                    retries=5,
                )
            except NoBrokersAvailable:
                time.sleep(2)
        raise RuntimeError("Producer retry no se pudo conectar")

    def send_retry(self, payload):
        self.producer.send(self.topic_retry, key=payload.get('id'), value=payload)
        self.producer.flush()

    def send_dlq(self, payload):
        self.producer.send(self.topic_dlq, key=payload.get('id'), value=payload)
        self.producer.flush()

    def close(self):
        self.producer.flush()
        self.producer.close()
