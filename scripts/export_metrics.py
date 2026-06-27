#!/usr/bin/env python3
"""
Exporta las metricas agregadas por Spark desde Elasticsearch a CSV y calcula
las metricas resumen exigidas por la rubrica (incluye recovery_time y backlog).

Modos:
  --scenario <name>   Vuelca el indice metrics-aggregated a
                      resultados/<name>_aggregated.csv y calcula un resumen
                      en resultados/<name>_resumen.json (usa tambien el CSV de
                      backlog si existe).
  --summary           Combina todos los *_resumen.json en
                      resultados/resumen_escenarios.csv (tabla comparativa).

Uso:
  python3 scripts/export_metrics.py --scenario normal --out-dir resultados
  python3 scripts/export_metrics.py --summary --out-dir resultados
"""
import argparse
import csv
import glob
import json
import os
import urllib.request

ES_URL = os.getenv("ES_URL", "http://localhost:9200")
ES_INDEX = os.getenv("ES_INDEX", "metrics-aggregated")

AGG_FIELDS = [
    "window_start", "window_end",
    "total_events", "ok_events", "hit_events", "miss_ok_events",
    "retry_events", "dlq_count",
    "throughput_per_min", "latency_p50_ms", "latency_p95_ms",
    "hit_rate", "retry_rate",
]


def es_search(index, size=10000):
    body = json.dumps({
        "size": size,
        "sort": [{"window_end": {"order": "asc"}}],
        "query": {"match_all": {}},
    }).encode("utf-8")
    req = urllib.request.Request(
        f"{ES_URL}/{index}/_search",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read().decode("utf-8"))
        return [h["_source"] for h in data.get("hits", {}).get("hits", [])]
    except Exception as e:
        print(f"[export] No se pudo consultar ES ({e}). Indice vacio?")
        return []


def percentile(values, p):
    if not values:
        return 0.0
    vs = sorted(values)
    k = (len(vs) - 1) * p
    f = int(k)
    c = min(f + 1, len(vs) - 1)
    if f == c:
        return float(vs[f])
    return float(vs[f] + (vs[c] - vs[f]) * (k - f))


def dump_aggregated(name, out_dir):
    rows = es_search(ES_INDEX)
    out_csv = os.path.join(out_dir, f"{name}_aggregated.csv")
    with open(out_csv, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=AGG_FIELDS, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"[export] {len(rows)} ventanas -> {out_csv}")
    return rows


def backlog_summary(name, out_dir):
    """Calcula backlog maximo/medio y recovery_time a partir de la serie de lag."""
    path = os.path.join(out_dir, f"{name}_backlog.csv")
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        rows = list(csv.DictReader(f))
    if not rows:
        return {}
    t = [float(r["t_rel"]) for r in rows]
    lag = [float(r["lag_total"]) for r in rows]
    lag_main = [float(r["lag_main"]) for r in rows]

    max_lag = max(lag) if lag else 0.0
    avg_lag = sum(lag) / len(lag) if lag else 0.0

    # recovery_time: tiempo desde el pico de backlog hasta volver a <= 5% del pico
    recovery_time = 0.0
    if max_lag > 0:
        idx_peak = lag.index(max_lag)
        thr = max(1.0, 0.05 * max_lag)
        recovered_t = None
        for i in range(idx_peak, len(lag)):
            if lag[i] <= thr:
                recovered_t = t[i]
                break
        if recovered_t is not None:
            recovery_time = round(recovered_t - t[idx_peak], 2)
        else:
            recovery_time = round(t[-1] - t[idx_peak], 2)  # no alcanzo a recuperarse

    # tiempo de vaciado de cola principal (drain time): de pico a 0
    drain_time = 0.0
    if max(lag_main) > 0:
        ip = lag_main.index(max(lag_main))
        drained = None
        for i in range(ip, len(lag_main)):
            if lag_main[i] == 0:
                drained = t[i]
                break
        drain_time = round((drained - t[ip]) if drained is not None else (t[-1] - t[ip]), 2)

    return {
        "backlog_max": round(max_lag, 1),
        "backlog_avg": round(avg_lag, 1),
        "recovery_time_s": recovery_time,
        "drain_time_main_s": drain_time,
    }


def scenario_summary(name, rows, out_dir):
    thr = [float(r.get("throughput_per_min", 0) or 0) for r in rows]
    p50 = [float(r.get("latency_p50_ms", 0) or 0) for r in rows]
    p95 = [float(r.get("latency_p95_ms", 0) or 0) for r in rows]
    hit = [float(r.get("hit_rate", 0) or 0) for r in rows]
    ret = [float(r.get("retry_rate", 0) or 0) for r in rows]
    dlq = sum(int(r.get("dlq_count", 0) or 0) for r in rows)

    summary = {
        "escenario": name,
        "ventanas": len(rows),
        "throughput_max_per_min": round(max(thr), 2) if thr else 0.0,
        "throughput_avg_per_min": round(sum(thr) / len(thr), 2) if thr else 0.0,
        "p50_ms_avg": round(sum(p50) / len(p50), 2) if p50 else 0.0,
        "p95_ms_max": round(max(p95), 2) if p95 else 0.0,
        "hit_rate_avg": round(sum(hit) / len(hit), 4) if hit else 0.0,
        "retry_rate_max": round(max(ret), 4) if ret else 0.0,
        "dlq_total": dlq,
    }
    summary.update(backlog_summary(name, out_dir))

    out_json = os.path.join(out_dir, f"{name}_resumen.json")
    with open(out_json, "w") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    print(f"[export] resumen -> {out_json}")
    return summary


def build_summary_table(out_dir):
    files = sorted(glob.glob(os.path.join(out_dir, "*_resumen.json")))
    rows = []
    for fp in files:
        with open(fp) as f:
            rows.append(json.load(f))
    if not rows:
        print("[export] no hay *_resumen.json todavia")
        return
    cols = ["escenario", "ventanas", "throughput_max_per_min", "throughput_avg_per_min",
            "p50_ms_avg", "p95_ms_max", "hit_rate_avg", "retry_rate_max", "dlq_total",
            "backlog_max", "backlog_avg", "recovery_time_s", "drain_time_main_s"]
    out_csv = os.path.join(out_dir, "resumen_escenarios.csv")
    with open(out_csv, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"[export] tabla comparativa -> {out_csv}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scenario")
    ap.add_argument("--summary", action="store_true")
    ap.add_argument("--out-dir", default="resultados")
    args = ap.parse_args()
    os.makedirs(args.out_dir, exist_ok=True)

    if args.scenario:
        rows = dump_aggregated(args.scenario, args.out_dir)
        scenario_summary(args.scenario, rows, args.out_dir)
    if args.summary:
        build_summary_table(args.out_dir)


if __name__ == "__main__":
    main()
