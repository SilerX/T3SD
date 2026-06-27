#!/usr/bin/env bash
# Aplica el index template de Elasticsearch (tipos de campo correctos).
# Uso: bash scripts/setup_es.sh
set -e
ES_URL="${ES_URL:-http://localhost:9200}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "[setup_es] Esperando a Elasticsearch en ${ES_URL} ..."
for i in $(seq 1 40); do
  if curl -s "${ES_URL}/_cluster/health" | grep -qE 'green|yellow'; then break; fi
  sleep 3
done

echo "[setup_es] Aplicando index template metrics-aggregated ..."
curl -s -X PUT "${ES_URL}/_index_template/metrics-aggregated-template" \
  -H "Content-Type: application/json" \
  --data-binary "@${ROOT}/elasticsearch/index_template.json"
echo
echo "[setup_es] Listo."
