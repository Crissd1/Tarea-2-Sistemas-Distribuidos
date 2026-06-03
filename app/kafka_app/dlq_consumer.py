"""
Consumer de DLQ:
Permite revisar las consultas que no pudieron procesarse luego de alcanzar el máximo de reintentos.
"""
import json
from kafka import KafkaConsumer
from kafka_config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_DLQ

def create_dlq_consumer() -> KafkaConsumer:
    """
    Crea el consumer encargado de monitorear dlq-topic.
    """
    return KafkaConsumer(
        TOPIC_DLQ,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="dlq-monitor",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        key_deserializer=lambda key: key.decode("utf-8") if key else None,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

def main() -> None:
    """
    Ejecuta el monitor de la DLQ.
    """
    consumer = create_dlq_consumer()
    print(f"DLQ consumer escuchando tópico '{TOPIC_DLQ}'")
    print(f"Servidor Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    for message in consumer:
        print("-" * 80)
        print("Consulta recibida en DLQ")
        print(f"Offset: {message.offset}")
        print(f"Key: {message.key}")
        print(f"Mensaje: {message.value}")

if __name__ == "__main__":
    main()