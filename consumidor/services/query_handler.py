import time


class QueryHandler:
    def __init__(self, cache, response_client, metrics_client, retry_producer, max_retries, consumer_id):
        self.cache = cache
        self.response_client = response_client
        self.metrics = metrics_client
        self.retry_producer = retry_producer
        self.max_retries = max_retries
        self.consumer_id = consumer_id

    def handle(self, payload):
        t0 = time.time()
        key = self.cache.build_key(payload)
        cached = self.cache.get(key)

        if cached is not None:
            latencia = (time.time() - t0) * 1000.0
            self._emit('hit', payload, latencia, retried=payload.get('retry_count', 0) > 0)
            return

        ok, body = self.response_client.query(payload)
        latencia = (time.time() - t0) * 1000.0

        if ok:
            self.cache.set(key, body.get('result'))
            self._emit('miss_ok', payload, latencia, retried=payload.get('retry_count', 0) > 0)
            return

        retry_count = int(payload.get('retry_count', 0)) + 1
        payload['retry_count'] = retry_count
        payload['last_error'] = body

        if retry_count >= self.max_retries:
            self.retry_producer.send_dlq(payload)
            self._emit('dlq', payload, latencia, retried=True)
        else:
            self.retry_producer.send_retry(payload)
            self._emit('retry', payload, latencia, retried=True)

    def _emit(self, outcome, payload, latencia_ms, retried=False):
        event = {
            "id": payload.get('id'),
            "consumer_id": self.consumer_id,
            "tipo": payload.get('type'),
            "zona": payload.get('zone_id'),
            "outcome": outcome,
            "retry_count": int(payload.get('retry_count', 0)),
            "retried": bool(retried),
            "latencia_ms": float(latencia_ms),
            "created_at": payload.get('created_at'),
            "ts": time.time(),
        }
        self.metrics.record(event)
