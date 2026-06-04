# Tarea 2 - Sistemas Distribuidos

Este repositorio contiene la implementación de una arquitectura distribuida basada en **Apache Kafka** y **Redis** para procesar consultas geoespaciales de forma asíncrona, incorporando mecanismos de **reintentos**, **Dead Letter Queue (DLQ)**, **monitoreo de backlog**, **recovery time** y **registro de métricas experimentales**.

La solución extiende el sistema desarrollado en la Tarea 1, donde las consultas se procesaban directamente contra Redis y el generador de respuestas. En esta segunda tarea, Kafka se incorpora como una capa intermedia de mensajería para desacoplar el productor de consultas de los consumidores encargados del procesamiento.

---

## Integrantes

- Matías Mora
- Cristofer Pérez

---

## 1. Ubicación inicial

Todos los comandos de este README asumen que el usuario se encuentra en la **raíz del proyecto**, es decir, en la carpeta donde están:

```text
docker-compose.yml
requirements.txt
README.md
app/
data/
```

Si el repositorio fue clonado desde GitHub, primero ingresar a la carpeta del proyecto:

```bash
cd Tarea-2-Sistemas-Distribuidos
```

> Si la carpeta tiene otro nombre, entrar a la carpeta correspondiente. Lo importante es ejecutar los comandos desde la raíz del repositorio.

---

## 2. Arquitectura general

El flujo principal del sistema es:

```text
Kafka Producer
    ↓
queries-topic
    ↓
Kafka Consumer principal
    ↓
Redis Cache
    ↓
ResponseGenerator
```

Cuando ocurre una falla controlada durante el procesamiento, el sistema deriva la consulta al tópico de reintentos:

```text
Kafka Consumer principal
    ↓
retry-topic
    ↓
Retry Consumer
```

Si la consulta no puede recuperarse después del máximo de reintentos configurado, se envía a la Dead Letter Queue:

```text
retry-topic
    ↓
Retry Consumer falla
    ↓
dlq-topic
    ↓
DLQ Consumer
```

---

## 3. Estructura del proyecto

```text
.
├── app/
│   ├── cache/
│   │   └── redis_cache.py
│   ├── kafka_app/
│   │   ├── kafka_config.py
│   │   ├── kafka_producer.py
│   │   ├── kafka_consumer.py
│   │   ├── retry_consumer.py
│   │   ├── dlq_consumer.py
│   │   └── response_adapter.py
│   └── metrics/
│       ├── metrics_collector.py
│       ├── analyze_metrics.py
│       └── backlog_monitor.py
├── data/
├── results/
│   └── tarea2/
├── docker-compose.yml
├── requirements.txt
└── README.md
```

### Archivos principales

| Archivo | Descripción |
|---|---|
| `docker-compose.yml` | Define los servicios Docker: Redis, Redis Insight, Zookeeper, Kafka y Kafka UI. |
| `requirements.txt` | Contiene las dependencias Python necesarias. |
| `app/cache/redis_cache.py` | Implementa la conexión con Redis y la construcción de claves de caché. |
| `app/kafka_app/kafka_config.py` | Centraliza la configuración de tópicos, grupos de consumo y parámetros generales. |
| `app/kafka_app/kafka_producer.py` | Genera consultas Q1-Q5 y las publica en `queries-topic`. |
| `app/kafka_app/kafka_consumer.py` | Consume consultas desde Kafka, usa Redis y envía fallas a `retry-topic`. |
| `app/kafka_app/retry_consumer.py` | Reprocesa consultas fallidas desde `retry-topic`. |
| `app/kafka_app/dlq_consumer.py` | Monitorea consultas enviadas a `dlq-topic`. |
| `app/kafka_app/response_adapter.py` | Adapta las consultas Kafka al formato del generador de respuestas. |
| `app/metrics/metrics_collector.py` | Registra eventos del sistema en `events.csv`. |
| `app/metrics/analyze_metrics.py` | Calcula métricas agregadas en `summary.csv`. |
| `app/metrics/backlog_monitor.py` | Monitorea backlog y recovery time mediante offsets de Kafka. |

---

## 4. Dependencias

El proyecto fue desarrollado usando Python 3.10.

Crear y activar entorno virtual desde la raíz del proyecto:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Dependencias principales:

```text
kafka-python
redis
numpy
pandas
```

---

## 5. Infraestructura Docker

Levantar los servicios:

```bash
docker compose up -d
```

Verificar contenedores activos:

```bash
docker ps
```

Servicios utilizados:

| Servicio | Descripción | Puerto |
|---|---|---|
| `redis` | Caché en memoria | `6379` |
| `redis-insight` | Interfaz web para Redis | `5540` |
| `zookeeper` | Coordinador de Kafka | `2181` |
| `kafka` | Broker de mensajería | `9092` |
| `kafka-ui` | Interfaz web para Kafka | `8080` |

Interfaces web:

```text
Kafka UI: http://localhost:8080
Redis Insight: http://localhost:5540
```

---

## 6. Tópicos Kafka

El sistema utiliza tres tópicos principales:

| Tópico | Función |
|---|---|
| `queries-topic` | Tópico principal donde el producer publica consultas. |
| `retry-topic` | Tópico de reintentos para consultas que fallan temporalmente. |
| `dlq-topic` | Dead Letter Queue para consultas no recuperables. |

Listar tópicos:

```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list
```

Describir un tópico:

```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic queries-topic
```

Configuración usada en las pruebas:

```text
queries-topic: 4 particiones
retry-topic: 4 particiones
dlq-topic: 1 partición
```

Si es necesario recrear tópicos:

```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic queries-topic
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic retry-topic
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic dlq-topic

sleep 3

docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --create --topic queries-topic --partitions 4 --replication-factor 1
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --create --topic retry-topic --partitions 4 --replication-factor 1
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --create --topic dlq-topic --partitions 1 --replication-factor 1
```

---

## 7. Variables de entorno

| Variable | Descripción | Valor típico |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | Dirección del broker Kafka | `localhost:9092` |
| `TOPIC_QUERIES` | Tópico principal | `queries-topic` |
| `TOPIC_RETRY` | Tópico de reintentos | `retry-topic` |
| `TOPIC_DLQ` | Tópico DLQ | `dlq-topic` |
| `CONSUMER_GROUP` | Grupo de consumidores principales | `query-consumers` |
| `MAX_RETRIES` | Máximo número de reintentos | `3` |
| `NUM_MESSAGES` | Cantidad de consultas generadas | `1000` |
| `DEFAULT_PRODUCER_DELAY` | Delay entre mensajes del producer | `0`, `0.1`, `0.2` |
| `TRAFFIC_DISTRIBUTION` | Distribución de tráfico | `uniform` o `zipf` |
| `ZIPF_ALPHA` | Parámetro de concentración Zipf | `1.2` |
| `FAILURE_RATE` | Probabilidad de falla del consumer principal | `0.0` a `1.0` |
| `RETRY_FAILURE_RATE` | Probabilidad de falla del retry consumer | `0.0` a `1.0` |
| `RETRY_DELAY_SECONDS` | Delay entre reintentos | configurable |
| `CACHE_TTL_SECONDS` | TTL de Redis | `1800` |
| `EXPERIMENT_NAME` | Nombre del experimento para guardar métricas | `exp_01_uniform_1consumer` |

---

## 8. Ejecución básica

### Consumer principal

Desde la raíz del proyecto:

```bash
cd app/kafka_app
source ../../venv/bin/activate
python3 kafka_consumer.py
```

### Producer

Desde la raíz del proyecto:

```bash
cd app/kafka_app
source ../../venv/bin/activate
NUM_MESSAGES=20 TRAFFIC_DISTRIBUTION=zipf python3 kafka_producer.py
```

### Retry Consumer

Desde la raíz del proyecto:

```bash
cd app/kafka_app
source ../../venv/bin/activate
python3 retry_consumer.py
```

### DLQ Consumer

Desde la raíz del proyecto:

```bash
cd app/kafka_app
source ../../venv/bin/activate
python3 dlq_consumer.py
```

---

## 9. Registro de métricas por experimento

El sistema permite guardar resultados separados por experimento usando `EXPERIMENT_NAME`.

Ejemplo:

```bash
cd app/kafka_app
source ../../venv/bin/activate
EXPERIMENT_NAME=exp_01_uniform_1consumer python3 kafka_consumer.py
```

Los resultados se guardan en:

```text
results/tarea2/<EXPERIMENT_NAME>/events.csv
results/tarea2/<EXPERIMENT_NAME>/summary.csv
results/tarea2/<EXPERIMENT_NAME>/backlog.csv
results/tarea2/<EXPERIMENT_NAME>/recovery_time.csv
```

Para generar el resumen de métricas, ejecutar desde la raíz del proyecto:

```bash
source venv/bin/activate
EXPERIMENT_NAME=exp_01_uniform_1consumer python3 app/metrics/analyze_metrics.py
```

---

## 10. Métricas calculadas

Eventos registrados:

| Evento | Descripción |
|---|---|
| `cache_hit` | Consulta respondida desde Redis. |
| `cache_miss_processed` | Consulta procesada exitosamente tras cache miss. |
| `sent_to_retry` | Consulta enviada a `retry-topic`. |
| `recovered_from_retry` | Consulta recuperada correctamente desde `retry-topic`. |
| `sent_to_dlq` | Consulta enviada a `dlq-topic`. |

Métricas calculadas:

| Métrica | Descripción |
|---|---|
| `total_queries` | Cantidad total de consultas únicas procesadas. |
| `successful_events` | Eventos exitosos. |
| `failed_events` | Eventos asociados a fallas. |
| `cache_hits` | Consultas respondidas desde Redis. |
| `cache_miss_processed` | Consultas calculadas y guardadas en Redis. |
| `throughput_successful_queries_per_second` | Consultas exitosas por segundo. |
| `latency_avg_ms` | Latencia promedio. |
| `latency_p50_ms` | Percentil 50 de latencia. |
| `latency_p95_ms` | Percentil 95 de latencia. |
| `retry_rate` | Proporción de eventos enviados a retry. |
| `recovery_rate` | Proporción de mensajes recuperados desde retry. |
| `dlq_count` | Cantidad de mensajes enviados a DLQ. |
| `dlq_rate` | Proporción de consultas enviadas a DLQ. |
| `failure_rate` | Proporción de eventos fallidos. |

---

## 11. Monitoreo de backlog y recovery time

El backlog se mide usando el lag de Kafka:

```text
Lag = EndOffset - CommittedOffset
```

Ejecutar desde la raíz del proyecto:

```bash
source venv/bin/activate
EXPERIMENT_NAME=demo_backlog python3 app/metrics/backlog_monitor.py --watch --until-zero --interval 1 --max-seconds 90
```

El monitor genera:

```text
results/tarea2/<EXPERIMENT_NAME>/backlog.csv
results/tarea2/<EXPERIMENT_NAME>/recovery_time.csv
```

---

## 12. Pruebas experimentales realizadas

| Experimento | Objetivo | Consultas |
|---|---|---:|
| `exp_01_uniform_1consumer` | Kafka con 1 consumer y distribución uniforme | 1000 |
| `exp_02_zipf_1consumer` | Kafka con 1 consumer y distribución Zipf | 1000 |
| `exp_03_zipf_2consumers` | Escalamiento con 2 consumers | 1000 |
| `exp_04_zipf_4consumers` | Escalamiento con 4 consumers | 1000 |
| `exp_05_retry_success` | Reintentos exitosos | 50 |
| `exp_06_dlq` | Envío a DLQ ante fallas persistentes | 50 |
| `exp_07_backlog_recovery` | Backlog y recovery time | 1000 |
| `exp_08_spike_traffic` | Spike de tráfico | 1000 |

---

## 13. Comandos de pruebas principales

> Todos los comandos parten desde la raíz del proyecto, salvo que se indique lo contrario.

### 13.1. Flujo normal con 1 consumer y distribución uniforme

Terminal 1:

```bash
cd app/kafka_app
source ../../venv/bin/activate
EXPERIMENT_NAME=exp_01_uniform_1consumer CONSUMER_CLIENT_ID=consumer-1 python3 kafka_consumer.py
```

Terminal 2:

```bash
cd app/kafka_app
source ../../venv/bin/activate
EXPERIMENT_NAME=exp_01_uniform_1consumer NUM_MESSAGES=1000 TRAFFIC_DISTRIBUTION=uniform DEFAULT_PRODUCER_DELAY=0 python3 kafka_producer.py
```

Resumen, desde la raíz:

```bash
source venv/bin/activate
EXPERIMENT_NAME=exp_01_uniform_1consumer python3 app/metrics/analyze_metrics.py
```

---

### 13.2. Flujo normal con 1 consumer y distribución Zipf

Terminal 1:

```bash
cd app/kafka_app
source ../../venv/bin/activate
EXPERIMENT_NAME=exp_02_zipf_1consumer CONSUMER_CLIENT_ID=consumer-1 python3 kafka_consumer.py
```

Terminal 2:

```bash
cd app/kafka_app
source ../../venv/bin/activate
EXPERIMENT_NAME=exp_02_zipf_1consumer NUM_MESSAGES=1000 TRAFFIC_DISTRIBUTION=zipf DEFAULT_PRODUCER_DELAY=0 python3 kafka_producer.py
```

Resumen, desde la raíz:

```bash
source venv/bin/activate
EXPERIMENT_NAME=exp_02_zipf_1consumer python3 app/metrics/analyze_metrics.py
```

---

### 13.3. Escalamiento con 2 consumers

Terminal 1:

```bash
cd app/kafka_app
source ../../venv/bin/activate
EXPERIMENT_NAME=exp_03_zipf_2consumers CONSUMER_CLIENT_ID=consumer-1 python3 kafka_consumer.py
```

Terminal 2:

```bash
cd app/kafka_app
source ../../venv/bin/activate
EXPERIMENT_NAME=exp_03_zipf_2consumers CONSUMER_CLIENT_ID=consumer-2 python3 kafka_consumer.py
```

Terminal 3:

```bash
cd app/kafka_app
source ../../venv/bin/activate
EXPERIMENT_NAME=exp_03_zipf_2consumers NUM_MESSAGES=1000 TRAFFIC_DISTRIBUTION=zipf DEFAULT_PRODUCER_DELAY=0 python3 kafka_producer.py
```

Resumen, desde la raíz:

```bash
source venv/bin/activate
EXPERIMENT_NAME=exp_03_zipf_2consumers python3 app/metrics/analyze_metrics.py
```

---

### 13.4. Escalamiento con 4 consumers

Terminal 1:

```bash
cd app/kafka_app
source ../../venv/bin/activate
EXPERIMENT_NAME=exp_04_zipf_4consumers CONSUMER_CLIENT_ID=consumer-1 python3 kafka_consumer.py
```

Terminal 2:

```bash
cd app/kafka_app
source ../../venv/bin/activate
EXPERIMENT_NAME=exp_04_zipf_4consumers CONSUMER_CLIENT_ID=consumer-2 python3 kafka_consumer.py
```

Terminal 3:

```bash
cd app/kafka_app
source ../../venv/bin/activate
EXPERIMENT_NAME=exp_04_zipf_4consumers CONSUMER_CLIENT_ID=consumer-3 python3 kafka_consumer.py
```

Terminal 4:

```bash
cd app/kafka_app
source ../../venv/bin/activate
EXPERIMENT_NAME=exp_04_zipf_4consumers CONSUMER_CLIENT_ID=consumer-4 python3 kafka_consumer.py
```

Terminal 5:

```bash
cd app/kafka_app
source ../../venv/bin/activate
EXPERIMENT_NAME=exp_04_zipf_4consumers NUM_MESSAGES=1000 TRAFFIC_DISTRIBUTION=zipf DEFAULT_PRODUCER_DELAY=0 python3 kafka_producer.py
```

Resumen, desde la raíz:

```bash
source venv/bin/activate
EXPERIMENT_NAME=exp_04_zipf_4consumers python3 app/metrics/analyze_metrics.py
```

---

### 13.5. Retry exitoso

Terminal 1:

```bash
cd app/kafka_app
source ../../venv/bin/activate
EXPERIMENT_NAME=exp_05_retry_success FAILURE_RATE=1.0 python3 kafka_consumer.py
```

Terminal 2:

```bash
cd app/kafka_app
source ../../venv/bin/activate
EXPERIMENT_NAME=exp_05_retry_success RETRY_FAILURE_RATE=0.0 python3 retry_consumer.py
```

Terminal 3:

```bash
cd app/kafka_app
source ../../venv/bin/activate
EXPERIMENT_NAME=exp_05_retry_success NUM_MESSAGES=50 TRAFFIC_DISTRIBUTION=zipf DEFAULT_PRODUCER_DELAY=0 python3 kafka_producer.py
```

Resumen, desde la raíz:

```bash
source venv/bin/activate
EXPERIMENT_NAME=exp_05_retry_success python3 app/metrics/analyze_metrics.py
```

---

### 13.6. DLQ

Terminal 1:

```bash
cd app/kafka_app
source ../../venv/bin/activate
EXPERIMENT_NAME=exp_06_dlq FAILURE_RATE=1.0 python3 kafka_consumer.py
```

Terminal 2:

```bash
cd app/kafka_app
source ../../venv/bin/activate
EXPERIMENT_NAME=exp_06_dlq RETRY_FAILURE_RATE=1.0 python3 retry_consumer.py
```

Terminal 3:

```bash
cd app/kafka_app
source ../../venv/bin/activate
EXPERIMENT_NAME=exp_06_dlq python3 dlq_consumer.py
```

Terminal 4:

```bash
cd app/kafka_app
source ../../venv/bin/activate
EXPERIMENT_NAME=exp_06_dlq NUM_MESSAGES=50 TRAFFIC_DISTRIBUTION=zipf DEFAULT_PRODUCER_DELAY=0 python3 kafka_producer.py
```

Resumen, desde la raíz:

```bash
source venv/bin/activate
EXPERIMENT_NAME=exp_06_dlq python3 app/metrics/analyze_metrics.py
```

---

### 13.7. Backlog y recovery time

Terminal 1, monitor desde la raíz:

```bash
source venv/bin/activate
EXPERIMENT_NAME=exp_07_backlog_recovery python3 app/metrics/backlog_monitor.py --watch --until-zero --interval 1 --max-seconds 180
```

Terminal 2, producer sin consumer activo:

```bash
cd app/kafka_app
source ../../venv/bin/activate
EXPERIMENT_NAME=exp_07_backlog_recovery NUM_MESSAGES=1000 TRAFFIC_DISTRIBUTION=zipf DEFAULT_PRODUCER_DELAY=0 python3 kafka_producer.py
```

Terminal 3, consumer para vaciar backlog:

```bash
cd app/kafka_app
source ../../venv/bin/activate
EXPERIMENT_NAME=exp_07_backlog_recovery CONSUMER_CLIENT_ID=consumer-1 python3 kafka_consumer.py
```

Revisar recovery time desde la raíz:

```bash
cat results/tarea2/exp_07_backlog_recovery/recovery_time.csv
```

---

### 13.8. Spike de tráfico

Terminal 1, monitor desde la raíz:

```bash
source venv/bin/activate
EXPERIMENT_NAME=exp_08_spike_traffic python3 app/metrics/backlog_monitor.py --watch --interval 1 --max-seconds 120
```

Terminal 2, consumer activo:

```bash
cd app/kafka_app
source ../../venv/bin/activate
EXPERIMENT_NAME=exp_08_spike_traffic CONSUMER_CLIENT_ID=consumer-1 python3 kafka_consumer.py
```

Terminal 3, producer rápido:

```bash
cd app/kafka_app
source ../../venv/bin/activate
EXPERIMENT_NAME=exp_08_spike_traffic NUM_MESSAGES=1000 TRAFFIC_DISTRIBUTION=zipf DEFAULT_PRODUCER_DELAY=0 python3 kafka_producer.py
```

Resumen, desde la raíz:

```bash
source venv/bin/activate
EXPERIMENT_NAME=exp_08_spike_traffic python3 app/metrics/analyze_metrics.py
```

---

## 14. Limpieza de resultados y Redis

Limpiar resultados de un experimento:

```bash
rm -rf results/tarea2/<EXPERIMENT_NAME>
```

Limpiar Redis:

```bash
docker exec -it redis redis-cli FLUSHALL
```

Revisar grupos de consumo Kafka:

```bash
docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list
```

Revisar lag de un grupo:

```bash
docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group query-consumers
```

---

## 15. Resultados finales obtenidos

| Experimento | Throughput | p50 ms | p95 ms | Hits | Misses | Retry rate | Recovery rate | DLQ count |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `exp_01_uniform_1consumer` | 164.6353 | 3.2611 | 8.7204 | 887 | 113 | 0.0 | 0.0 | 0 |
| `exp_02_zipf_1consumer` | 148.9373 | 3.8586 | 9.5056 | 903 | 97 | 0.0 | 0.0 | 0 |
| `exp_03_zipf_2consumers` | 179.9990 | 5.1762 | 12.4461 | 901 | 99 | 0.0 | 0.0 | 0 |
| `exp_04_zipf_4consumers` | 192.4123 | 10.3566 | 21.8457 | 894 | 106 | 0.0 | 0.0 | 0 |
| `exp_05_retry_success` | 0.9869 | 517.4950 | 1016.3082 | 0 | 0 | 0.5 | 1.0 | 0 |
| `exp_06_dlq` | 0.0000 | 5.6469 | 11.1488 | 0 | 0 | 0.5 | 0.0 | 50 |
| `exp_07_backlog_recovery` | 181.8951 | 3.6769 | 8.9945 | 898 | 102 | 0.0 | 0.0 | 0 |
| `exp_08_spike_traffic` | 149.4137 | 3.4428 | 10.7054 | 900 | 100 | 0.0 | 0.0 | 0 |

Backlog observado:

| Experimento | Backlog máximo | Recovery time |
|---|---:|---:|
| `exp_07_backlog_recovery` | 1000 mensajes | 20.0267 s |
| `exp_08_spike_traffic` | 734 mensajes | No aplica |

---

## 16. Notas adicionales

- El directorio `results/tarea2/` se crea automáticamente al registrar métricas.
- Los archivos `events.csv`, `summary.csv`, `backlog.csv` y `recovery_time.csv` no deberían subirse al repositorio si solo corresponden a resultados locales.
- Para las pruebas del informe se recomienda usar `DEFAULT_PRODUCER_DELAY=0`.
- Para el video de demostración se recomienda usar menos consultas y un delay pequeño, por ejemplo `DEFAULT_PRODUCER_DELAY=0.1`, para que el flujo sea visible.
- Si se reinicia Kafka, puede ser necesario recrear los tópicos antes de repetir pruebas.
