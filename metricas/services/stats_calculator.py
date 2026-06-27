import numpy as np
from collections import defaultdict


class StatsCalculator:
    def __init__(self, repository):
        self.repo = repository

    def calculate(self):
        events = self.repo.all()
        if not events:
            return {"total_eventos": 0}

        total = len(events)
        outcomes = defaultdict(int)
        latencias_ok = []
        por_tipo = defaultdict(lambda: {"total": 0, "hit": 0, "miss_ok": 0, "retry": 0, "dlq": 0, "lats": []})
        retried_ids = set()
        recovered_ids = set()
        ts_min = float('inf')
        ts_max = 0.0
        unique_ids = set()
        dlq_ids = set()

        for e in events:
            outcome = e.get('outcome', 'unknown')
            outcomes[outcome] += 1
            qid = e.get('id')
            tipo = e.get('tipo', 'unknown')
            lat = float(e.get('latencia_ms', 0))
            ts = float(e.get('ts', 0))

            if qid:
                unique_ids.add(qid)
            if ts:
                ts_min = min(ts_min, ts)
                ts_max = max(ts_max, ts)

            por_tipo[tipo]['total'] += 1
            if outcome in ('hit', 'miss_ok'):
                if lat > 0:
                    latencias_ok.append(lat)
                    por_tipo[tipo]['lats'].append(lat)
                if outcome == 'hit':
                    por_tipo[tipo]['hit'] += 1
                else:
                    por_tipo[tipo]['miss_ok'] += 1
                if e.get('retried'):
                    recovered_ids.add(qid)
            elif outcome == 'retry':
                por_tipo[tipo]['retry'] += 1
                retried_ids.add(qid)
            elif outcome == 'dlq':
                por_tipo[tipo]['dlq'] += 1
                dlq_ids.add(qid)

        duracion = max(1e-6, ts_max - ts_min) if ts_max > 0 else 1e-6
        ok_total = outcomes.get('hit', 0) + outcomes.get('miss_ok', 0)
        throughput = ok_total / duracion

        p50 = float(np.percentile(latencias_ok, 50)) if latencias_ok else 0.0
        p95 = float(np.percentile(latencias_ok, 95)) if latencias_ok else 0.0

        retry_rate = (outcomes.get('retry', 0) / total * 100.0) if total else 0.0
        dlq_rate = (len(dlq_ids) / max(1, len(unique_ids)) * 100.0)
        recovered = len(recovered_ids - dlq_ids)
        recovery_rate = (recovered / max(1, len(retried_ids | recovered_ids)) * 100.0) if retried_ids or recovered_ids else 0.0

        por_tipo_out = {}
        for t, d in por_tipo.items():
            lats = d['lats']
            por_tipo_out[t] = {
                "total": d['total'],
                "hit": d['hit'],
                "miss_ok": d['miss_ok'],
                "retry": d['retry'],
                "dlq": d['dlq'],
                "p50_ms": float(np.percentile(lats, 50)) if lats else 0.0,
                "p95_ms": float(np.percentile(lats, 95)) if lats else 0.0,
            }

        return {
            "total_eventos": total,
            "consultas_unicas": len(unique_ids),
            "duracion_s": duracion,
            "throughput_ok_per_s": throughput,
            "p50_ms": p50,
            "p95_ms": p95,
            "outcomes": dict(outcomes),
            "retry_rate_pct": retry_rate,
            "dlq_rate_pct": dlq_rate,
            "recovery_rate_pct": recovery_rate,
            "recovered_count": recovered,
            "dlq_count": len(dlq_ids),
            "por_tipo": por_tipo_out,
        }
