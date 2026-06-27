import sys
import time
import requests
from config import Config
from services.query_factory import QueryFactory
from services.traffic_simulator import TrafficSimulator
from producers.kafka_producer import QueryProducer


class TrafficGeneratorApp:
    def __init__(self, config):
        self.config = config

    def _wait_metrics(self, max_retries=30):
        for _ in range(max_retries):
            try:
                r = requests.get(f"{self.config.URL_METRICAS}/health", timeout=3)
                if r.status_code == 200:
                    print("[OK] Sistema de metricas disponible")
                    return
            except Exception:
                pass
            time.sleep(2)
        print("[WARN] Metricas no disponible, continuando igual")

    def run(self):
        self._wait_metrics()
        factory = QueryFactory(distribucion=self.config.DISTRIBUCION)
        producer = QueryProducer(self.config.KAFKA_BOOTSTRAP, self.config.TOPIC_MAIN)
        simulator = TrafficSimulator(factory, producer, self.config)
        try:
            simulator.run()
        finally:
            producer.close()
        sys.stdout.flush()


if __name__ == '__main__':
    TrafficGeneratorApp(Config).run()
