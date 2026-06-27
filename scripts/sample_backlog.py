#!/usr/bin/env python3
"""
Muestrea periodicamente el backlog (consumer lag de Kafka) y las estadisticas
globales del servicio de metricas, escribiendo una serie temporal en CSV.

Esto cubre dos metricas de la rubrica que NO se calculan en metrics-topic:
  - backlog_size  : tamano de la cola pendiente (lag) por topico.
  - recovery_time : se deriva luego a partir de esta serie (ver export_metrics.py).

Uso:
  python3 scripts/sample_backlog.py --out resultados/normal_backlog.csv \
        --interval 1 --duration 180
"""
import argparse
import csv
import time
import urllib.request
import json

METRICS_URL = "http://localhost:5001"


def get_json(path):
    try:
        with urllib.request.urlopen(f"{METRICS_URL}{path}", timeout=4) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception:
        return {}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--interval", type=float, default=1.0)
    ap.add_argument("--duration", type=float, default=180.0)
    args = ap.parse_args()

    fields = [
        "t_rel", "ts",
        "lag_main", "lag_retry", "lag_dlq", "lag_total",
        "throughput_ok_per_s", "p50_ms", "p95_ms",
        "retry_rate_pct", "dlq_rate_pct", "recovery_rate_pct",
        "ok_total", "total_eventos",
    ]
    t0 = time.time()
    with open(args.out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        while time.time() - t0 < args.duration:
            now = time.time()
            backlog = get_json("/backlog")
            stats = get_json("/stats")

            def lag(topic):
                d = backlog.get(topic, {})
                return d.get("lag", 0) if isinstance(d, dict) else 0

            lag_main = lag("queries.main")
            lag_retry = lag("queries.retry")
            lag_dlq = lag("queries.dlq")
            outcomes = stats.get("outcomes", {}) if isinstance(stats, dict) else {}
            ok_total = outcomes.get("hit", 0) + outcomes.get("miss_ok", 0)

            w.writerow({
                "t_rel": round(now - t0, 2),
                "ts": round(now, 3),
                "lag_main": lag_main,
                "lag_retry": lag_retry,
                "lag_dlq": lag_dlq,
                "lag_total": lag_main + lag_retry + lag_dlq,
                "throughput_ok_per_s": round(stats.get("throughput_ok_per_s", 0.0), 3),
                "p50_ms": round(stats.get("p50_ms", 0.0), 3),
                "p95_ms": round(stats.get("p95_ms", 0.0), 3),
                "retry_rate_pct": round(stats.get("retry_rate_pct", 0.0), 3),
                "dlq_rate_pct": round(stats.get("dlq_rate_pct", 0.0), 3),
                "recovery_rate_pct": round(stats.get("recovery_rate_pct", 0.0), 3),
                "ok_total": ok_total,
                "total_eventos": stats.get("total_eventos", 0),
            })
            f.flush()
            time.sleep(args.interval)
    print(f"[sample_backlog] serie escrita en {args.out}")


if __name__ == "__main__":
    main()
