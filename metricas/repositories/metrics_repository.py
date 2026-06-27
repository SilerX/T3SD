import json
import redis


class MetricsRepository:
    def __init__(self, host, port, key):
        self.client = redis.Redis(host=host, port=port, decode_responses=True)
        self.key = key

    def save(self, event):
        self.client.rpush(self.key, json.dumps(event))

    def all(self):
        n = self.client.llen(self.key)
        if n == 0:
            return []
        return [json.loads(x) for x in self.client.lrange(self.key, 0, -1)]

    def reset(self):
        self.client.delete(self.key)
