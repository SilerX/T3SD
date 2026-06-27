import numpy as np
from models.query import Query


class QueryFactory:
    ZONAS = ["Z1", "Z2", "Z3", "Z4", "Z5"]
    TIPOS = ["Q1", "Q2", "Q3", "Q4", "Q5"]
    CONFIDENCIAS = [0.0, 0.3, 0.5, 0.7, 0.9]

    def __init__(self, distribucion='zipf', zipf_s_zona=1.3, zipf_s_tipo=1.0):
        self.distribucion = distribucion
        self.zipf_s_zona = zipf_s_zona
        self.zipf_s_tipo = zipf_s_tipo

    def _zipf_choice(self, items, s):
        n = len(items)
        ranks = np.arange(1, n + 1)
        weights = 1.0 / np.power(ranks, s)
        weights /= weights.sum()
        idx = np.random.choice(n, p=weights)
        return items[idx]

    def _choose(self, items, s=1.0):
        if self.distribucion == 'zipf':
            return self._zipf_choice(items, s)
        return items[np.random.randint(0, len(items))]

    def generate(self):
        zona = self._choose(self.ZONAS, self.zipf_s_zona)
        tipo = self._choose(self.TIPOS, self.zipf_s_tipo)
        conf = float(np.random.choice(self.CONFIDENCIAS))

        zona_b = None
        bins = None
        if tipo == 'Q4':
            zona_b = str(np.random.choice([z for z in self.ZONAS if z != zona]))
        if tipo == 'Q5':
            bins = int(np.random.choice([3, 5, 7, 10]))

        return Query(tipo, zona, conf, zona_b, bins)
