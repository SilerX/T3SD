import time


class TrafficSimulator:
    def __init__(self, factory, producer, config):
        self.factory = factory
        self.producer = producer
        self.config = config

    def _current_rate(self, idx, total):
        rate = self.config.RATE
        if self.config.SPIKE == 1:
            spike_start = int(total * self.config.SPIKE_AT)
            spike_msgs = int(self.config.SPIKE_DURATION * self.config.RATE * self.config.SPIKE_FACTOR)
            spike_end = spike_start + spike_msgs
            if spike_start <= idx < spike_end:
                rate = self.config.RATE * self.config.SPIKE_FACTOR
        return max(1e-3, rate)

    def run(self):
        n = self.config.N_CONSULTAS
        print(f"[INFO] Publicando {n} consultas | dist={self.config.DISTRIBUCION} | rate={self.config.RATE} req/s | spike={self.config.SPIKE}")
        t0 = time.time()
        for i in range(n):
            q = self.factory.generate()
            self.producer.send(q)
            rate = self._current_rate(i, n)
            time.sleep(1.0 / rate)
            if (i + 1) % 100 == 0:
                print(f"[..] {i+1}/{n} publicadas")
        self.producer.flush()
        elapsed = time.time() - t0
        print(f"[OK] {n} consultas publicadas en {elapsed:.2f}s ({n/elapsed:.2f} req/s)")
        return elapsed
