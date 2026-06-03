"""
Backlog y recovery time:
Calcula el lag de un consumer group. El lag representa la cantidad de mensajes pendientes.
Se puede ejecutar en modo 'watch' para medir el tiempo que tarda el backlog en volver a cero después de una falla o spike de tráfico.
"""
import argparse
import csv
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from kafka import KafkaConsumer, TopicPartition

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

from app.kafka_app.kafka_config import (  # noqa: E402
    CONSUMER_GROUP,
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_QUERIES,
    TOPIC_RETRY,
)
RESULTS_DIR = PROJECT_ROOT / "results" / "tarea2"
BACKLOG_FILE = RESULTS_DIR / "backlog.csv"
RECOVERY_FILE = RESULTS_DIR / "recovery_time.csv"
BACKLOG_FIELDS = [
    "timestamp",
    "topic",
    "group_id",
    "partition",
    "end_offset",
    "committed_offset",
    "lag",
    "total_lag",
]
RECOVERY_FIELDS = [
    "timestamp",
    "topic",
    "group_id",
    "recovery_time_seconds",
    "max_lag_observed",
]

def ensure_results_files() -> None:
    """
    Crea los archivos CSV de backlog y recovery time.
    """
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    if not BACKLOG_FILE.exists():
        with BACKLOG_FILE.open("w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=BACKLOG_FIELDS)
            writer.writeheader()

    if not RECOVERY_FILE.exists():
        with RECOVERY_FILE.open("w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=RECOVERY_FIELDS)
            writer.writeheader()

def create_monitor_consumer(group_id: str) -> KafkaConsumer:
    """
    Crea un consumer usado solo para offsets y lag.
    """
    return KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )

def get_topic_partitions(consumer: KafkaConsumer, topic: str) -> list[TopicPartition]:
    """
    Obtiene las particiones de un tópico.
    """
    partitions = consumer.partitions_for_topic(topic)
    if not partitions:
        return []
    return [TopicPartition(topic, partition) for partition in partitions]

def calculate_lag(topic: str, group_id: str) -> tuple[int, list[dict[str, int | str]]]:
    """
    Calcula el backlog total de un tópico para un consumer group.
    """
    consumer = create_monitor_consumer(group_id)
    topic_partitions = get_topic_partitions(consumer, topic)
    if not topic_partitions:
        consumer.close()
        return 0, []

    consumer.assign(topic_partitions)
    end_offsets = consumer.end_offsets(topic_partitions)
    rows = []
    total_lag = 0
    for topic_partition in topic_partitions:
        end_offset = end_offsets.get(topic_partition, 0)
        committed_offset = consumer.committed(topic_partition)

        if committed_offset is None:
            committed_offset = 0

        lag = max(end_offset - committed_offset, 0)
        total_lag += lag
        rows.append(
            {
                "topic": topic,
                "group_id": group_id,
                "partition": topic_partition.partition,
                "end_offset": end_offset,
                "committed_offset": committed_offset,
                "lag": lag,
            }
        )

    consumer.close()
    return total_lag, rows


def log_backlog_snapshot(topic: str, group_id: str) -> int:
    """
    Guarda una medición de backlog en results/tarea2/backlog.csv
    """
    ensure_results_files()
    total_lag, rows = calculate_lag(topic, group_id)
    timestamp = datetime.now(timezone.utc).isoformat()

    with BACKLOG_FILE.open("a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=BACKLOG_FIELDS)

        if not rows:
            writer.writerow(
                {
                    "timestamp": timestamp,
                    "topic": topic,
                    "group_id": group_id,
                    "partition": "",
                    "end_offset": "",
                    "committed_offset": "",
                    "lag": 0,
                    "total_lag": 0,
                }
            )
        else:
            for row in rows:
                writer.writerow(
                    {
                        "timestamp": timestamp,
                        "topic": row["topic"],
                        "group_id": row["group_id"],
                        "partition": row["partition"],
                        "end_offset": row["end_offset"],
                        "committed_offset": row["committed_offset"],
                        "lag": row["lag"],
                        "total_lag": total_lag,
                    }
                )

    print(f"[{timestamp}] topic={topic} group={group_id} total_lag={total_lag}")
    return total_lag

def log_recovery_time(
    topic: str,
    group_id: str,
    recovery_time_seconds: float,
    max_lag_observed: int,
) -> None:
    """
    Guarda el recovery time.
    """
    ensure_results_files()

    with RECOVERY_FILE.open("a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=RECOVERY_FIELDS)
        writer.writerow(
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "topic": topic,
                "group_id": group_id,
                "recovery_time_seconds": round(recovery_time_seconds, 4),
                "max_lag_observed": max_lag_observed,
            }
        )

def watch_backlog(
    topic: str,
    group_id: str,
    interval_seconds: float,
    max_seconds: float,
    until_zero: bool,
) -> None:
    """
    Monitorea backlog de forma periódica.
    """
    monitor_start_time = time.perf_counter()
    backlog_start_time = None
    max_lag_observed = 0
    saw_backlog = False
    while True:
        total_lag = log_backlog_snapshot(topic, group_id)
        max_lag_observed = max(max_lag_observed, total_lag)

        if total_lag > 0 and not saw_backlog:
            saw_backlog = True
            backlog_start_time = time.perf_counter()
            print("Backlog detectado. Iniciando recovery time.")
        if until_zero and saw_backlog and total_lag == 0:
            recovery_time = time.perf_counter() - backlog_start_time

            log_recovery_time(
                topic=topic,
                group_id=group_id,
                recovery_time_seconds=recovery_time,
                max_lag_observed=max_lag_observed,
            )
            print(f"Recovery time: {recovery_time:.4f} segundos")
            break
        elapsed = time.perf_counter() - monitor_start_time
        if elapsed >= max_seconds:
            print("Tiempo máximo de monitoreo alcanzado.")
            break
        time.sleep(interval_seconds)

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Monitor de backlog y recovery time."
    )
    parser.add_argument(
        "--topic",
        default=TOPIC_QUERIES,
        help="Tópico Kafka a monitorear.",
    )
    parser.add_argument(
        "--group-id",
        default=CONSUMER_GROUP,
        help="Consumer group asociado al tópico.",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Monitorea el backlog de forma periódica.",
    )
    parser.add_argument(
        "--until-zero",
        action="store_true",
        help="Registra recovery time cuando el backlog vuelve a cero.",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Intervalo de monitoreo en segundos.",
    )
    parser.add_argument(
        "--max-seconds",
        type=float,
        default=120.0,
        help="Tiempo máximo de monitoreo.",
    )
    args = parser.parse_args()
    if args.watch:
        watch_backlog(
            topic=args.topic,
            group_id=args.group_id,
            interval_seconds=args.interval,
            max_seconds=args.max_seconds,
            until_zero=args.until_zero,
        )
    else:
        log_backlog_snapshot(topic=args.topic, group_id=args.group_id)

if __name__ == "__main__":
    main()