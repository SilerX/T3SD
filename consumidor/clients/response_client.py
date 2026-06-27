import requests


class ResponseClient:
    def __init__(self, base_url, timeout=10):
        self.base_url = base_url
        self.timeout = timeout

    def query(self, payload):
        try:
            r = requests.post(f"{self.base_url}/query", json=payload, timeout=self.timeout)
            if r.status_code == 200:
                return True, r.json()
            return False, {"status_code": r.status_code, "body": r.text[:200]}
        except Exception as e:
            return False, {"error": type(e).__name__, "msg": str(e)[:200]}


class MetricsClient:
    def __init__(self, base_url, timeout=5):
        self.base_url = base_url
        self.timeout = timeout

    def record(self, event):
        try:
            requests.post(f"{self.base_url}/record", json=event, timeout=self.timeout)
        except Exception:
            pass
