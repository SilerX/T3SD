#!/usr/bin/env python3
"""
Mide la LATENCIA DE VISUALIZACION del pipeline de observabilidad, es decir,
cuanto tarda un evento en estar disponible/consultable en Elasticsearch
(y por lo tanto en Kibana) desde que ocurrio.

Para cada ventana en metrics-aggregated calcula:
    viz_latency = ingested_at - window_end

Esto responde directamente al requisito de la rubrica:
"Evaluacion de la latencia de visualizacion y la capacidad de reflejar fallas".

Uso:
  python3 scripts/measure_viz_latency.py --out resultados/viz_latency.csv
"""
import argparse
import csv
import json
import os
import urllib.request
from datetime import datetime

ES_URL = os.getenv("ES_URL", "http://localhost:9200")
ES_INDEX = os.getenv("ES_INDEX", "metrics-aggregated")


def parse_ts(s):
    if not s:
        return None
    s = s.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s)
    except Exception:
        try:
            return datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")
        except Exception:
            return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="resultados/viz_latency.csv")
    args = ap.parse_args()
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)

    body = json.dumps({"size": 10000, "sort": [{"window_end": "asc"}],
                       "query": {"match_all": {}}}).encode()
    req = urllib.request.Request(f"{ES_URL}/{ES_INDEX}/_search", data=body,
                                 headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=15) as r:
        hits = json.loads(r.read().decode())["hits"]["hits"]

    lats = []
    with open(args.out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["window_end", "ingested_at", "viz_latency_s"])
        for h in hits:
            src = h["_source"]
            we = parse_ts(src.get("window_end"))
            ia = parse_ts(src.get("ingested_at"))
            if we and ia:
                lat = (ia - we).total_seconds()
                lats.append(lat)
                w.writerow([src.get("window_end"), src.get("ingested_at"), round(lat, 2)])

    if lats:
        lats.sort()
        n = len(lats)
        print(f"[viz] muestras={n}  min={min(lats):.1f}s  "
              f"p50={lats[n//2]:.1f}s  p95={lats[int(n*0.95)]:.1f}s  max={max(lats):.1f}s")
    else:
        print("[viz] sin datos en el indice.")


if __name__ == "__main__":
    main()
