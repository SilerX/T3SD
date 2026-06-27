import json
import sys
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable


class QueryConsumer:
    def __init__(self, bootstrap_servers, topic, group_id, handler, backoff_ms=0):
        self.topic = topic
        self.group_id = group_id
        self.handler = handler
        self.backoff_ms = backoff_ms
        self.consumer = self._connect(bootstrap_servers)

    def _connect(self, bootstrap, max_retries=30):
        for i in range(max_retries):
            try:
                c = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=bootstrap,
                    group_id=self.group_id,
                    auto_offset_reset='earliest',
                    enable_auto_commit=True,
                    auto_commit_interval_ms=1000,
                    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                    key_deserializer=lambda k: k.decode('utf-8') if k else None,
                    session_timeout_ms=10000,
                    heartbeat_interval_ms=3000,
                    max_poll_interval_ms=300000,
                    max_poll_records=20,
                    api_version=(2, 5, 0),
                )
                print(f"[OK] Consumer suscrito al topic '{self.topic}' (grupo='{self.group_id}')", flush=True)
                return c
            except NoBrokersAvailable:
                print(f"[..] Esperando Kafka para consumer (intento {i+1}/{max_retries})", flush=True)
                time.sleep(2)
        raise RuntimeError("Consumer no pudo conectar a Kafka")

    def run(self):
        print(f"[INFO] Iniciando bucle de poll sobre '{self.topic}'...", flush=True)
        total = 0
        empty_polls = 0
        try:
            while True:
                records = self.consumer.poll(timeout_ms=1000, max_records=20)
                if not records:
                    empty_polls += 1
                    if empty_polls % 30 == 0:
                        assignment = self.consumer.assignment()
                        print(f"[INFO] {empty_polls} polls vacios - asignacion actual: {assignment}", flush=True)
                    continue
                empty_polls = 0
                for tp, batch in records.items():
                    for msg in batch:
                        if self.backoff_ms > 0:
                            time.sleep(self.backoff_ms / 1000.0)
                        try:
                            self.handler.handle(msg.value)
                            total += 1
                            if total % 25 == 0:
                                print(f"[INFO] Procesados {total} mensajes", flush=True)
                        except Exception as e:
                            print(f"[ERR] handler fail en msg {msg.offset}: {type(e).__name__}: {e}", flush=True)
        except KeyboardInterrupt:
            print(f"[INFO] Consumer detenido por usuario. Total procesados: {total}", flush=True)
        except Exception as e:
            print(f"[FATAL] Bucle de consumo termino con error: {type(e).__name__}: {e}", flush=True)
            sys.stdout.flush()
            raise
        finally:
            try:
                self.consumer.close()
            except Exception:
                pass
