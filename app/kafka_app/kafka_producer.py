import json
import random
import time
import uuid
import os
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
CONFIDENCE_VALUES = [0.0, 0.5, 0.7]
BIN_VALUES = [5, 10]
TRAFFIC_DISTRIBUTION = os.getenv("TRAFFIC_DISTRIBUTION", "uniform").lower()
ZIPF_ALPHA = float(os.getenv("ZIPF_ALPHA", "1.2"))

def weighted_choice_zipf(items: list[str], alpha: float) -> str:
    """
    Selecciona un elemento usando pesos tipo Zipf.

    Los primeros elementos de la lista tienen mayor probabilidad de aparecer.
    """
    ranks = list(range(1, len(items) + 1))
    weights = [1 / (rank ** alpha) for rank in ranks]
    return random.choices(items, weights=weights, k=1)[0]

def choose_value(items: list[Any]) -> Any:
    """
    Selecciona un valor usando la distribución elegida.
    """
    if TRAFFIC_DISTRIBUTION == "zipf":
        return weighted_choice_zipf(items, ZIPF_ALPHA)

    return random.choice(items)

def create_query() -> dict[str, Any]:
    """
    Crea una consulta Q1-Q5 compatible con Kafka.
    """
    query_type = choose_value(QUERY_TYPES)
    zone = choose_value(ZONES)
    confidence = choose_value(CONFIDENCE_VALUES)

    query: dict[str, Any] = {
        "id": str(uuid.uuid4()),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "query_type": query_type,
        "zone": zone,
        "params": {
            "confidence": confidence,
        },
        "retry_count": 0,
        "traffic_distribution": TRAFFIC_DISTRIBUTION,
    }

    if query_type == "Q4":
        possible_zones = [current_zone for current_zone in ZONES if current_zone != zone]
        query["params"]["zone_b"] = choose_value(possible_zones)

    if query_type == "Q5":
        query["params"]["bins"] = choose_value(BIN_VALUES)
    return query

def create_producer() -> KafkaProducer:
    """
    Crea un KafkaProducer con serialización JSON.
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
    Publica consultas en el tópico principal de Kafka.
    """
    producer = create_producer()
    print(f"Publicando {num_messages} consultas en el tópico '{TOPIC_QUERIES}'")
    print(f"Servidor Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Distribución: {TRAFFIC_DISTRIBUTION}")

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
    publish_queries(
        num_messages=DEFAULT_NUM_MESSAGES,
        delay_seconds=DEFAULT_PRODUCER_DELAY,
    )

if __name__ == "__main__":
    main()
