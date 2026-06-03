"""
Consumer de reintentos:
Lee consultas desde retry-topic, intenta reprocesarlas y envía a DLQ aquellas que superan el máximo de reintentos permitido.
"""
import json
import os
import random
import sys
import time
from pathlib import Path
from typing import Any
from kafka import KafkaConsumer, KafkaProducer
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))
from app.cache.redis_cache import (
    build_cache_key,
    create_redis_client,
    save_to_cache,
)

from kafka_config import (
    KAFKA_BOOTSTRAP_SERVERS,
    MAX_RETRIES,
    TOPIC_DLQ,
    TOPIC_RETRY,
)


RETRY_FAILURE_RATE = float(os.getenv("RETRY_FAILURE_RATE", "0.0"))
RETRY_DELAY_SECONDS = float(os.getenv("RETRY_DELAY_SECONDS", "1.0"))


def create_retry_consumer() -> KafkaConsumer:
    """
    Crea el consumer del tópico de reintentos.
    """
    return KafkaConsumer(
        TOPIC_RETRY,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="retry-consumers",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        key_deserializer=lambda key: key.decode("utf-8") if key else None,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )


def create_producer() -> KafkaProducer:
    """
    Crea un producer para reenviar a retry-topic o enviar a DLQ.
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda key: key.encode("utf-8") if key else None,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def should_fail() -> bool:
    """
    Determina si el reintento falla según la tasa configurada.
    """
    return random.random() < RETRY_FAILURE_RATE


def simulate_response_generator(query: dict[str, Any]) -> dict[str, Any]:
    """
    Simula el reprocesamiento de una consulta fallida.
    """
    time.sleep(0.05)
    if should_fail():
        raise RuntimeError("Falla simulada durante reintento")

    return {
        "query_id": query.get("id"),
        "query_type": query.get("query_type"),
        "zone": query.get("zone"),
        "params": query.get("params", {}),
        "result": "simulated_response_after_retry",
        "source": "retry_consumer",
    }

def send_to_dlq(
    producer: KafkaProducer,
    query: dict[str, Any],
    error_message: str,
) -> None:
    """
    Envía una consulta a DLQ.
    """
    query["last_error"] = error_message
    query["dlq_reason"] = "max_retries_exceeded"
    producer.send(
        TOPIC_DLQ,
        key=query["id"],
        value=query,
    )
    producer.flush()
    print("Consulta enviada a DLQ")

def send_back_to_retry(
    producer: KafkaProducer,
    query: dict[str, Any],
    error_message: str,
) -> None:
    """
    Reenvía una consulta al tópico de reintentos.
    """
    query["retry_count"] = int(query.get("retry_count", 0)) + 1
    query["last_error"] = error_message
    producer.send(
        TOPIC_RETRY,
        key=query["id"],
        value=query,
    )
    producer.flush()
    print(f"Consulta reenviada a retry-topic con retry_count={query['retry_count']}")


def process_retry_message(
    query: dict[str, Any],
    producer: KafkaProducer,
) -> None:
    """
    Procesa una consulta recibida desde retry-topic
    """
    redis_client = create_redis_client()

    retry_count = int(query.get("retry_count", 0))
    cache_key = build_cache_key(query)

    print("Consulta recibida desde retry-topic")
    print(f"ID: {query.get('id')}")
    print(f"Tipo: {query.get('query_type')}")
    print(f"Zona: {query.get('zone')}")
    print(f"Retry count actual: {retry_count}")
    print(f"Clave caché: {cache_key}")

    if retry_count >= MAX_RETRIES:
        send_to_dlq(
            producer=producer,
            query=query,
            error_message="Máximo de reintentos alcanzado antes de reprocesar",
        )
        return
    time.sleep(RETRY_DELAY_SECONDS)

    try:
        response = simulate_response_generator(query)
        save_to_cache(redis_client, cache_key, response)
        print("Consulta recuperada exitosamente desde retry-topic")
        print(f"Respuesta guardada en Redis: {response}")

    except Exception as error:
        print(f"Error durante reintento: {error}")

        if retry_count + 1 >= MAX_RETRIES:
            query["retry_count"] = retry_count + 1
            send_to_dlq(
                producer=producer,
                query=query,
                error_message=str(error),
            )
        else:
            send_back_to_retry(
                producer=producer,
                query=query,
                error_message=str(error),
            )

def main() -> None:
    """
    Ejecuta el consumer de reintentos.
    """
    consumer = create_retry_consumer()
    producer = create_producer()
    print(f"Retry consumer escuchando tópico '{TOPIC_RETRY}'")
    print(f"Servidor Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"MAX_RETRIES: {MAX_RETRIES}")
    print(f"RETRY_FAILURE_RATE: {RETRY_FAILURE_RATE}")
    for message in consumer:
        print("-" * 80)
        print(f"Offset: {message.offset}")
        print(f"Key: {message.key}")
        process_retry_message(message.value, producer)

if __name__ == "__main__":
    main()