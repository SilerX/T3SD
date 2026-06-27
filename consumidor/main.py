import sys
from config import Config
from cache.cache_manager import CacheManager
from clients.response_client import ResponseClient, MetricsClient
from producers.retry_producer import RetryProducer
from services.query_handler import QueryHandler
from consumers.kafka_consumer import QueryConsumer


class ConsumerApp:
    def __init__(self, config):
        self.config = config

    def run(self):
        cache = CacheManager(self.config.REDIS_HOST, self.config.REDIS_PORT, self.config.CACHE_TTL)
        response_client = ResponseClient(self.config.URL_RESPUESTA)
        metrics_client = MetricsClient(self.config.URL_METRICAS)
        retry_producer = RetryProducer(self.config.KAFKA_BOOTSTRAP, self.config.TOPIC_RETRY, self.config.TOPIC_DLQ)

        handler = QueryHandler(
            cache=cache,
            response_client=response_client,
            metrics_client=metrics_client,
            retry_producer=retry_producer,
            max_retries=self.config.MAX_RETRIES,
            consumer_id=self.config.CONSUMER_ID,
        )

        if self.config.MODE == 'retry':
            topic = self.config.TOPIC_RETRY
            group = self.config.CONSUMER_GROUP + '-retry'
            backoff = self.config.RETRY_BACKOFF_MS
            print(f"[INFO] Iniciando en modo RETRY topic={topic} backoff={backoff}ms")
        else:
            topic = self.config.TOPIC_MAIN
            group = self.config.CONSUMER_GROUP
            backoff = 0
            print(f"[INFO] Iniciando en modo MAIN topic={topic}")

        consumer = QueryConsumer(
            self.config.KAFKA_BOOTSTRAP,
            topic,
            group,
            handler,
            backoff_ms=backoff,
        )
        sys.stdout.flush()
        consumer.run()
        retry_producer.close()


if __name__ == '__main__':
    ConsumerApp(Config).run()
