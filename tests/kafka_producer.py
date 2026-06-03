"""
Kafka Producer inicial para la Tarea 2 de Sistemas Distribuidos.

Este producer genera consultas simuladas Q1-Q5 y las publica en el
tópico principal de Kafka. Cada consulta incluye:
- ID único.
- Timestamp de creación.
- Tipo de consulta.
- Zona.
- Parámetros.
- Número de reintentos inicializado en 0.

Más adelante, este archivo se conectará con el generador de tráfico
real implementado en la Tarea 1.
"""

import json
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from kafka import KafkaProducer

from kafka_config import (
    DEFAULT_NUM_MESSAGES,
    DEFAULT_PRODUCER_DELAY,
    KAFKA_BOOTSTRAP_SERVERS,
    PRODUCER_CLIENT_ID,
    TOPIC_QUERIES,
)


QUERY_TYPES = ["Q1", "Q2", "Q3", "Q4", "Q5"]
ZONES = ["Z1", "Z2", "Z3", "Z4", "Z5"]
CONFIDENCE_VALUES = [0.0, 0.25, 0.5, 0.75]
BIN_VALUES = [5, 10, 15, 20]


def create_query() -> dict[str, Any]:
    """
    Crea una consulta simulada con la estructura requerida para Kafka.

    Returns:
        dict: Mensaje de consulta listo para ser enviado a Kafka.
    """
    query_type = random.choice(QUERY_TYPES)
    zone = random.choice(ZONES)

    query: dict[str, Any] = {
        "id": str(uuid.uuid4()),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "query_type": query_type,
        "zone": zone,
        "params": {
            "confidence": random.choice(CONFIDENCE_VALUES),
        },
        "retry_count": 0,
    }

    if query_type == "Q4":
        possible_zones = [current_zone for current_zone in ZONES if current_zone != zone]
        query["params"]["zone_b"] = random.choice(possible_zones)

    if query_type == "Q5":
        query["params"]["bins"] = random.choice(BIN_VALUES)

    return query


def create_producer() -> KafkaProducer:
    """
    Crea una instancia de KafkaProducer con serialización JSON.

    Returns:
        KafkaProducer: Producer configurado para enviar mensajes a Kafka.
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        client_id=PRODUCER_CLIENT_ID,
        key_serializer=lambda key: key.encode("utf-8") if key else None,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        retries=3,
    )


def publish_queries(num_messages: int, delay_seconds: float) -> None:
    """
    Publica un conjunto de consultas en el tópico principal de Kafka.

    Args:
        num_messages: Cantidad de consultas a publicar.
        delay_seconds: Tiempo de espera entre mensajes.
    """
    producer = create_producer()

    print(f"Publicando {num_messages} consultas en el tópico '{TOPIC_QUERIES}'")
    print(f"Servidor Kafka: {KAFKA_BOOTSTRAP_SERVERS}")

    for index in range(1, num_messages + 1):
        query = create_query()

        producer.send(
            TOPIC_QUERIES,
            key=query["id"],
            value=query,
        )

        print(f"[{index}/{num_messages}] Consulta enviada: {query}")
        time.sleep(delay_seconds)

    producer.flush()
    producer.close()

    print("Producer finalizado correctamente.")


def main() -> None:
    """
    Punto de entrada del producer.
    """
    publish_queries(
        num_messages=DEFAULT_NUM_MESSAGES,
        delay_seconds=DEFAULT_PRODUCER_DELAY,
    )


if __name__ == "__main__":
    main()
