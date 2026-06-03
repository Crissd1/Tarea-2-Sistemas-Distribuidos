import json
from typing import Any
from kafka import KafkaConsumer
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

def process_message(message_value: dict[str, Any]) -> None:
    query_id = message_value.get("id")
    query_type = message_value.get("query_type")
    zone = message_value.get("zone")
    retry_count = message_value.get("retry_count", 0)
    print("Consulta recibida correctamente")
    print(f"ID: {query_id}")
    print(f"Tipo: {query_type}")
    print(f"Zona: {zone}")
    print(f"Reintentos: {retry_count}")
    print(f"Mensaje completo: {message_value}")

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
