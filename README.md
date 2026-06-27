# Tarea 3 - Sistemas Distribuidos

## Procesamiento Streaming de Métricas con Apache Spark y visualización con Elasticsearch + Kibana

Extiende la arquitectura Kafka de la Tarea 2 con un **plano de observabilidad
en tiempo real**: las métricas se publican en un tópico dedicado de Kafka, se
procesan con **Apache Spark Structured Streaming** en ventanas de tiempo, se
almacenan en **Elasticsearch** y se visualizan en **Kibana**.

## Arquitectura

```
PLANO DE PROCESAMIENTO (Tarea 2)
  generador-trafico -> queries.main -> consumidores -> Redis (hit)
                                                     -> generador-respuestas (miss)
                                                     -> queries.retry / queries.dlq
                                       consumidores -> metricas (/record)

PLANO DE OBSERVABILIDAD (Tarea 3 - nuevo)
  metricas -> metrics-topic -> Spark Structured Streaming
           -> Elasticsearch (metrics-aggregated) -> Kibana (dashboards)
```

## Componentes nuevos respecto a la Tarea 2

- **metricas (mejorado)**: además de persistir en Redis, publica cada evento en
  `metrics-topic` (`metricas/producers/metrics_publisher.py`). Ya **no** calcula
  agregaciones: eso lo hace Spark.
- **spark-streaming**: job PySpark (`spark_streaming/streaming_job.py`) con
  ventanas deslizantes (60 s / slide 30 s), watermark 2 min, percentiles p50/p95,
  hit rate y retry rate; escribe en Elasticsearch vía conector ES-Hadoop.
- **elasticsearch** + **kibana**: almacenamiento y visualización.

## Métricas calculadas por ventana

throughput (consultas exitosas/min), latencia **p50** y **p95**, **hit rate**,
**retry rate**, conteos por outcome y **dlq_count**. Además, vía scripts:
**backlog size** (lag de Kafka), **recovery time**, **drain time** y
**latencia de visualización**.

## Requisitos

- Docker + Docker Compose.
- ~4 GB de RAM libres (Elasticsearch + Spark).
- Coloca el dataset `967_buildings.csv.gz` en `data/` (si falta, se genera uno
  sintético automáticamente).

## Despliegue

```bash
# 1. Levantar el stack completo (9 servicios)
docker compose up -d --build
docker compose ps                 # verificar healthy

# 2. Index template + dashboards de Kibana
bash scripts/setup_es.sh
bash kibana/setup_kibana.sh

# 3. Abrir Kibana
#    http://localhost:5601  -> Dashboards -> "T3 - Dashboard de Observabilidad"
```

Si prefieres importar los dashboards a mano: Kibana → *Stack Management* →
*Saved Objects* → *Import* → `kibana/dashboards.ndjson`.

## Ejecutar los escenarios de evaluación

```bash
# Todos los escenarios (normal, 1/4 consumidores, falla, reintentos/DLQ, spike)
bash scripts/run_scenarios.sh

# O algunos
bash scripts/run_scenarios.sh normal spike falla_temporal
```

Esto genera en `resultados/`:
- `<escenario>_aggregated.csv` — series de ventanas desde Elasticsearch.
- `<escenario>_backlog.csv` — serie temporal de backlog + stats.
- `<escenario>_resumen.json` — resumen por escenario.
- `resumen_escenarios.csv` — **tabla comparativa** (la del informe).

Latencia de visualización:
```bash
python3 scripts/measure_viz_latency.py --out resultados/viz_latency.csv
```

## Variables de entorno (escenarios)

| Variable | Descripción | Default |
|---|---|---|
| `N_CONSULTAS` | Número total de consultas | 1000 |
| `DIST` | `zipf` o `uniform` | `zipf` |
| `RATE` | Consultas por segundo | 50 |
| `NUM_CONSUMERS` | Réplicas de consumidor (`--scale consumidor=N`) | 2 |
| `FAILURE_RATE` | Probabilidad de falla en respuestas | 0.0 |
| `DOWN_AT` / `DOWN_DURATION` | Caída programada del gen. respuestas (s) | -1 / 0 |
| `SPIKE` / `SPIKE_FACTOR` | Spike de tráfico | 0 / 10 |
| `WINDOW_DURATION` / `SLIDE_DURATION` | Ventana Spark | 60s / 30s |
| `WATERMARK` | Tolerancia a desorden | 2 minutes |
| `TRIGGER_INTERVAL` | Micro-batch de Spark | 10 seconds |

## Puertos

- Kafka: 9092 (interno) / 29092 (host)
- Redis: 6379
- Métricas (API): 5001 (`/stats`, `/backlog`, `/raw`)
- Elasticsearch: 9200
- Kibana: 5601
