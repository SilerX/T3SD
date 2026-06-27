#!/usr/bin/env bash
# Importa la data view, las visualizaciones y el dashboard de la Tarea 3 en Kibana.
# Uso:  bash kibana/setup_kibana.sh   (con el stack levantado)
set -e

KIBANA_URL="${KIBANA_URL:-http://localhost:5601}"
NDJSON="$(dirname "$0")/dashboards.ndjson"

echo "[setup] Esperando a que Kibana este disponible en ${KIBANA_URL} ..."
for i in $(seq 1 60); do
  if curl -s "${KIBANA_URL}/api/status" | grep -q '"level":"available"'; then
    echo "[setup] Kibana disponible."
    break
  fi
  sleep 5
done

echo "[setup] Importando objetos guardados desde ${NDJSON} ..."
curl -s -X POST "${KIBANA_URL}/api/saved_objects/_import?overwrite=true" \
  -H "kbn-xsrf: true" \
  --form file=@"${NDJSON}" | tee /tmp/kibana_import_result.json
echo
echo "[setup] Listo. Abre ${KIBANA_URL}/app/dashboards y busca 'T3 - Dashboard de Observabilidad'."
