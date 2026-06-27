#!/usr/bin/env bash
#
# Orquestador de los 5 escenarios de evaluacion de la Tarea 3.
# Para cada escenario:
#   1. Reinicia el stack con las variables de entorno del escenario.
#   2. Muestrea el backlog (consumer lag) y /stats durante la ejecucion.
#   3. Espera a que el generador de trafico termine y las colas se vacien.
#   4. Exporta las metricas agregadas desde Elasticsearch a CSV.
#
# Uso:
#   bash scripts/run_scenarios.sh                # corre todos
#   bash scripts/run_scenarios.sh normal spike   # corre solo algunos
#
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
RESULTS="$ROOT/resultados"
mkdir -p "$RESULTS"

DC="docker compose"
METRICS_URL="http://localhost:5001"

# ---------------------------------------------------------------------------
# Definicion de escenarios (variables de entorno)
# ---------------------------------------------------------------------------
declare -A SCN_ENV
SCN_ENV[normal]="DIST=zipf N_CONSULTAS=2000 RATE=50 NUM_CONSUMERS=2 FAILURE_RATE=0.0 SPIKE=0"
SCN_ENV[un_consumidor]="DIST=zipf N_CONSULTAS=2000 RATE=50 NUM_CONSUMERS=1 FAILURE_RATE=0.0 SPIKE=0"
SCN_ENV[multi_consumidor]="DIST=zipf N_CONSULTAS=2000 RATE=50 NUM_CONSUMERS=4 FAILURE_RATE=0.0 SPIKE=0"
SCN_ENV[falla_temporal]="DIST=zipf N_CONSULTAS=2000 RATE=50 NUM_CONSUMERS=2 DOWN_AT=15 DOWN_DURATION=15 SPIKE=0"
SCN_ENV[reintentos_dlq]="DIST=zipf N_CONSULTAS=2000 RATE=50 NUM_CONSUMERS=2 FAILURE_RATE=0.30 MAX_RETRIES=3 SPIKE=0"
SCN_ENV[spike]="DIST=zipf N_CONSULTAS=2000 RATE=50 NUM_CONSUMERS=2 SPIKE=1 SPIKE_FACTOR=10 SPIKE_AT=0.5 SPIKE_DURATION=5"

ALL="normal un_consumidor multi_consumidor falla_temporal reintentos_dlq spike"
TARGETS="${*:-$ALL}"

run_scenario () {
  local name="$1"
  local env="${SCN_ENV[$name]:-}"
  if [ -z "$env" ]; then echo "[WARN] escenario desconocido: $name"; return; fi

  echo "=============================================================="
  echo "[ESCENARIO] $name -> $env"
  echo "=============================================================="

  # numero de consumidores
  local nconsumers
  nconsumers=$(echo "$env" | tr ' ' '\n' | grep '^NUM_CONSUMERS=' | cut -d= -f2)
  nconsumers="${nconsumers:-2}"

  # baja el stack previo
  $DC down -v >/dev/null 2>&1

  # levanta infra + observabilidad (sin trafico todavia)
  env $env $DC up -d --build \
      zookeeper kafka kafka-init redis-cache elasticsearch kibana \
      spark-streaming generador-respuestas metricas \
      consumidor consumidor-retry --scale consumidor="$nconsumers"

  echo "[..] esperando readiness (45s)"; sleep 45

  # limpia metricas locales
  curl -s -X POST "$METRICS_URL/reset" >/dev/null 2>&1 || true

  # inicia muestreo de backlog en background
  python3 "$ROOT/scripts/sample_backlog.py" \
      --out "$RESULTS/${name}_backlog.csv" \
      --interval 1 --duration 180 &
  local sampler_pid=$!

  # lanza el generador de trafico (corre una vez y termina)
  env $env $DC up -d generador-trafico
  echo "[..] generando trafico..."
  $DC logs -f generador-trafico 2>/dev/null | sed '/consultas publicadas en/q'

  echo "[..] drenando colas (40s)"; sleep 40

  # detiene el sampler
  kill "$sampler_pid" >/dev/null 2>&1 || true
  wait "$sampler_pid" 2>/dev/null || true

  # exporta agregados de Elasticsearch
  python3 "$ROOT/scripts/export_metrics.py" \
      --scenario "$name" \
      --out-dir "$RESULTS"

  echo "[OK] escenario $name finalizado. Resultados en $RESULTS/${name}_*.csv"
}

for t in $TARGETS; do
  run_scenario "$t"
done

echo "[DONE] generando tabla resumen comparativa..."
python3 "$ROOT/scripts/export_metrics.py" --summary --out-dir "$RESULTS"
echo "[DONE] Revisa $RESULTS/resumen_escenarios.csv"
