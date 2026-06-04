# Tarea 2 - Sistemas Distribuidos

Este repositorio contiene una solución basada en Apache Kafka y Redis para procesamiento de consultas distribuidas con reintentos y un DLQ (Dead Letter Queue). Incluye generación de tráfico Kafka, consumidores principales, reintentos, monitoreo de backlog y métricas.

## Estructura del proyecto

- `docker-compose.yml`: configuración de infraestructura Docker para Redis, Redis Insight, Zookeeper, Kafka y Kafka UI.
- `requirements.txt`: dependencias de Python.
- `app/`: código de la aplicación.
  - `cache/redis_cache.py`: cliente Redis y gestión de llaves de caché.
  - `kafka_app/`: productores y consumidores Kafka.
    - `kafka_config.py`: configuración de tópicos, servidores y parámetros de reintentos.
    - `kafka_producer.py`: genera consultas y publica mensajes en `queries-topic`.
    - `kafka_consumer.py`: consumidor principal que usa Redis para cache hit/miss y envía fallos a `retry-topic`.
    - `retry_consumer.py`: consumidor de reintentos que reprocesa mensajes y envía a DLQ si supera el máximo de reintentos.
    - `dlq_consumer.py`: monitor de mensajes en `dlq-topic`.
    - `response_adapter.py`: adapta consultas Kafka al formato requerido por `ResponseGenerator`.
  - `metrics/`: recolecta y analiza métricas.
    - `metrics_collector.py`: escribe eventos en `results/tarea2/events.csv`.
    - `backlog_monitor.py`: calcula backlog y recovery time para grupos de consumidores.
    - `analyze_metrics.py`: resume métricas desde el archivo de eventos.
- `tests/`: scripts de ejemplo para Kafka y entorno de pruebas.
  - `docker-compose.yml`: configuración de prueba idéntica a la raíz.
  - `kafka/`: ejemplos de producer/consumer de Kafka para pruebas.

## Dependencias

Instala las dependencias con:

```bash
pip install -r requirements.txt
```

Dependencias principales:
- `kafka-python`
- `redis`
- `numpy`
- `pandas`

## Infraestructura Docker

Arranca los servicios necesarios con:

```bash
docker-compose up -d
```

Servicios incluidos:
- `redis`: almacenamiento de caché.
- `redis-insight`: interfaz de administración de Redis.
- `zookeeper`: requisito de Kafka.
- `kafka`: broker Kafka.
- `kafka-ui`: interfaz web para explorar tópicos y consumidores.

## Componentes principales

### Productor de tráfico

`app/kafka_app/kafka_producer.py`
- Genera consultas `Q1` a `Q5`.
- Publica en `queries-topic`.
- Soporta distribución uniforme o Zipf mediante la variable de entorno `TRAFFIC_DISTRIBUTION`.

### Consumidor principal

`app/kafka_app/kafka_consumer.py`
- Escucha `queries-topic`.
- Busca respuestas en Redis usando una clave construida por tipo de consulta y parámetros.
- Si hay cache hit, no reprocesa.
- En caso de cache miss, genera respuesta y guarda el resultado en Redis.
- Si ocurre un error controlado, envía la consulta a `retry-topic`.
- Registra métricas en `results/tarea2/events.csv`.

### Consumidor de reintentos

`app/kafka_app/retry_consumer.py`
- Escucha `retry-topic`.
- Reintenta procesar consultas fallidas.
- Usa `retry_count` y `MAX_RETRIES` para decidir cuándo enviar a `dlq-topic`.
- Respeta un retardo configurado con `RETRY_DELAY_SECONDS`.

### Monitor de DLQ

`app/kafka_app/dlq_consumer.py`
- Escucha `dlq-topic`.
- Muestra los mensajes que no pudieron procesarse luego de agotar reintentos.

### Métricas y monitoreo

- `app/metrics/metrics_collector.py`: guarda eventos como `cache_hit`, `cache_miss_processed`, `sent_to_retry`, `recovered_from_retry` y `sent_to_dlq`.
- `app/metrics/backlog_monitor.py`: calcula profundidad de backlog y recovery time para grupos de consumidores.
- `app/metrics/analyze_metrics.py`: agrega y resume métricas desde `results/tarea2/events.csv`.

## Ejecución recomendada

Ejemplo de ejecución de componentes:

```bash
python -m app.kafka_app.kafka_producer
python -m app.kafka_app.kafka_consumer
python -m app.kafka_app.retry_consumer
python -m app.kafka_app.dlq_consumer
python -m app.metrics.backlog_monitor --watch --until-zero
python -m app.metrics.analyze_metrics
```

> Nota: los módulos dependen del paquete `response_generator` y de `data/buildings_zones.csv`. Estos archivos no se encuentran en el repositorio actual, por lo que el procesamiento de consultas fallará si no se agregan.

## Variables de entorno importantes

- `KAFKA_BOOTSTRAP_SERVERS`: broker Kafka, por defecto `localhost:9092`
- `TOPIC_QUERIES`: tópico de consultas
- `TOPIC_RETRY`: tópico de reintentos
- `TOPIC_DLQ`: tópico DLQ
- `CONSUMER_GROUP`: grupo de consumidores
- `MAX_RETRIES`: reintentos máximos
- `NUM_MESSAGES`: número de mensajes iniciales
- `DEFAULT_PRODUCER_DELAY`: retraso entre mensajes del productor
- `TRAFFIC_DISTRIBUTION`: `uniform` o `zipf`
- `FAILURE_RATE`: tasa de falla del consumidor principal
- `RETRY_FAILURE_RATE`: tasa de falla del consumidor de reintentos
- `RETRY_DELAY_SECONDS`: retraso entre reintentos
- `EXPERIMENT_NAME`: nombre de experimento para carpetas de resultados

## Notas adicionales

- El directorio `results/tarea2` se crea automáticamente al registrar métricas.
- Los scripts en `tests/kafka/` son ejemplos de prueba que repiten la lógica de generación y consumo de mensajes con Kafka.
- El archivo `app/__init__.py` está vacío y sirve para marcar el paquete Python.
