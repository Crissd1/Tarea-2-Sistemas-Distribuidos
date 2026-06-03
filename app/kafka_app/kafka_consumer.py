import json
import sys
import time
from pathlib import Path
from typing import Any
from kafka import KafkaConsumer

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
    TOPIC_QUERIES,
)

def create_consumer() -> KafkaConsumer:
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

def simulate_response_generator(query: dict[str, Any]) -> dict[str, Any]:
    time.sleep(0.05)
    return {
        "query_id": query.get("id"),
        "query_type": query.get("query_type"),
        "zone": query.get("zone"),
        "params": query.get("params", {}),
        "result": "simulated_response",
        "source": "response_generator_simulated",
    }


def process_message(message_value: dict[str, Any]) -> None:
    redis_client = create_redis_client()
    query_id = message_value.get("id")
    query_type = message_value.get("query_type")
    zone = message_value.get("zone")
    retry_count = message_value.get("retry_count", 0)
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
    response = simulate_response_generator(message_value)
    save_to_cache(redis_client, cache_key, response)
    print("Respuesta generada y almacenada en Redis")
    print(f"Respuesta: {response}")

def main() -> None:
    consumer = create_consumer()
    print(f"Consumer escuchando tópico '{TOPIC_QUERIES}'")
    print(f"Servidor Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Grupo de consumo: {CONSUMER_GROUP}")

    for message in consumer:
        print("-" * 80)
        print(f"Partición: {message.partition}")
        print(f"Offset: {message.offset}")
        print(f"Key: {message.key}")
        process_message(message.value)

if __name__ == "__main__":
    main()