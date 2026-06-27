import random
import time


class FailureSimulator:
    def __init__(self, failure_rate=0.0, latency_ms=0.0, down_at=-1, down_duration=0):
        self.failure_rate = failure_rate
        self.latency_ms = latency_ms
        self.down_at = down_at
        self.down_duration = down_duration
        self._started_at = time.time()

    def should_fail(self):
        if self.failure_rate <= 0:
            in_window = False
        else:
            in_window = random.random() < self.failure_rate

        if self.down_at >= 0 and self.down_duration > 0:
            elapsed = time.time() - self._started_at
            if self.down_at <= elapsed < self.down_at + self.down_duration:
                return True

        return in_window

    def inject_latency(self):
        if self.latency_ms > 0:
            time.sleep(self.latency_ms / 1000.0)
