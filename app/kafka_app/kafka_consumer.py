"""
Consumer principal de Kafka:
Procesa consultas, revisa Redis para detectar cache hit/cache miss y envía consultas fallidas al tópico de reintentos.
"""
import json
import sys
import time
import random
import os
from pathlib import Path
from typing import Any
from kafka import KafkaConsumer, KafkaProducer

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

from app.cache.redis_cache import (
    build_cache_key,
    create_redis_client,
    get_from_cache,
    save_to_cache,
)

from kafka_config import (
    CONSUMER_CLIENT_ID,
    CONSUMER_GROUP,
    KAFKA_BOOTSTRAP_SERVERS,
    MAX_RETRIES,
    TOPIC_QUERIES,
    TOPIC_RETRY,
)
FAILURE_RATE = float(os.getenv("FAILURE_RATE", "0.0"))

def create_consumer() -> KafkaConsumer:
    """
    Crea el consumidor del tópico principal de consultas.
    """
    return KafkaConsumer(
        TOPIC_QUERIES,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        client_id=CONSUMER_CLIENT_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        key_deserializer=lambda key: key.decode("utf-8") if key else None,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

def create_retry_producer() -> KafkaProducer:
    """
    Crea un producer para enviar consultas fallidas al tópico de reintentos.
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda key: key.encode("utf-8") if key else None,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

def should_fail() -> bool:
    """
    Determina si una consulta debe fallar según la tasa configurada.
    """
    return random.random() < FAILURE_RATE

def simulate_response_generator(query: dict[str, Any]) -> dict[str, Any]:
    """
    Simula temporalmente el Generador de Respuestas.
    """
    time.sleep(0.05)
    if should_fail():
        raise RuntimeError("Falla simulada del Generador de Respuestas")
    
    return {
        "query_id": query.get("id"),
        "query_type": query.get("query_type"),
        "zone": query.get("zone"),
        "params": query.get("params", {}),
        "result": "simulated_response",
        "source": "response_generator_simulated",
    }

def send_to_retry(
    producer: KafkaProducer,
    query: dict[str, Any],
    error_message: str,
) -> None:
    """
    Envía una consulta fallida al tópico de reintentos.
    """
    retry_count = int(query.get("retry_count", 0)) + 1
    query["retry_count"] = retry_count
    query["last_error"] = error_message
    producer.send(
        TOPIC_RETRY,
        key=query["id"],
        value=query,
    )
    producer.flush()
    print(f"Consulta enviada a retry-topic con retry_count={retry_count}")


def process_message(
    message_value: dict[str, Any],
    retry_producer: KafkaProducer,
) -> None:
    """
    Procesa una consulta desde Kafka usando Redis como caché.
    """
    redis_client = create_redis_client()
    query_id = message_value.get("id")
    query_type = message_value.get("query_type")
    zone = message_value.get("zone")
    retry_count = int(message_value.get("retry_count", 0))
    cache_key = build_cache_key(message_value)
    cached_response = get_from_cache(redis_client, cache_key)

    print("Consulta recibida correctamente")
    print(f"ID: {query_id}")
    print(f"Tipo: {query_type}")
    print(f"Zona: {zone}")
    print(f"Reintentos: {retry_count}")
    print(f"Clave caché: {cache_key}")

    if cached_response is not None:
        print("Resultado: CACHE HIT")
        print(f"Respuesta desde Redis: {cached_response}")
        return
    
    print("Resultado: CACHE MISS")
    try:
        response = simulate_response_generator(message_value)
        save_to_cache(redis_client, cache_key, response)

        print("Respuesta generada y almacenada en Redis")
        print(f"Respuesta: {response}")

    except Exception as error:
        print(f"Error durante el procesamiento: {error}")

        if retry_count >= MAX_RETRIES:
            print("La consulta ya alcanzó el máximo de reintentos permitidos.")
            return

        send_to_retry(
            producer=retry_producer,
            query=message_value,
            error_message=str(error),
        )

def main() -> None:
    """
    Ejecuta el consumer principal.
    """
    consumer = create_consumer()
    retry_producer = create_retry_producer()

    print(f"Consumer escuchando tópico '{TOPIC_QUERIES}'")
    print(f"Servidor Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Grupo de consumo: {CONSUMER_GROUP}")
    print(f"Failure rate: {FAILURE_RATE}")

    for message in consumer:
        print("-" * 80)
        print(f"Partición: {message.partition}")
        print(f"Offset: {message.offset}")
        print(f"Key: {message.key}")
        process_message(message.value, retry_producer)

if __name__ == "__main__":
    main()
