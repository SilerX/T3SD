import json
import redis


class CacheManager:
    def __init__(self, host, port, ttl):
        self.client = redis.Redis(host=host, port=port, decode_responses=True)
        self.ttl = ttl

    def build_key(self, query):
        tipo = query.get('type')
        zona = query.get('zone_id')
        conf = query.get('confidence_min')
        if tipo == 'Q1':
            return f"count:{zona}:conf={conf}"
        if tipo == 'Q2':
            return f"area:{zona}:conf={conf}"
        if tipo == 'Q3':
            return f"density:{zona}:conf={conf}"
        if tipo == 'Q4':
            return f"compare:density:{zona}:{query.get('zone_id_b')}:conf={conf}"
        if tipo == 'Q5':
            return f"confidence_dist:{zona}:bins={query.get('bins')}"
        return f"unknown:{zona}:{tipo}"

    def get(self, key):
        try:
            v = self.client.get(key)
            if v is None:
                return None
            return json.loads(v)
        except Exception:
            return None

    def set(self, key, value):
        try:
            self.client.setex(key, self.ttl, json.dumps(value))
        except Exception:
            pass
