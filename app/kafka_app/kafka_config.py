"""
Este módulo define los nombres de tópicos, grupo de consumidores,servidor Kafka y parámetros generales que serán utilizados por producer, consumers, sistema de reintentos y DLQ.
"""
import os

# Servidor Kafka en ejecución desde localhost:9092
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Tópicos principales
TOPIC_QUERIES = os.getenv("TOPIC_QUERIES", "queries-topic")
TOPIC_RETRY = os.getenv("TOPIC_RETRY", "retry-topic")
TOPIC_DLQ = os.getenv("TOPIC_DLQ", "dlq-topic")

# Grupo de consumidores. Todos los consumidores principales deben usar el mismo grupo para que Kafka distribuya automáticamente las consultas
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "query-consumers")

# Parámetros de reintento
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))

# Parámetros de generación de tráfico inicial
DEFAULT_NUM_MESSAGES = int(os.getenv("NUM_MESSAGES", "20"))
DEFAULT_PRODUCER_DELAY = float(os.getenv("DEFAULT_PRODUCER_DELAY", "0.2"))

# Parámetros de cliente.
PRODUCER_CLIENT_ID = os.getenv("PRODUCER_CLIENT_ID", "traffic-producer")
CONSUMER_CLIENT_ID = os.getenv("CONSUMER_CLIENT_ID", "query-consumer")
