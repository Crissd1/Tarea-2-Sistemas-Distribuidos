"""
Análisis de métricas:
Lee results/tarea2/events.csv y genera un resumen en results/tarea2/summary.csv con métricas de latencia, throughput, reintentos, recuperación y DLQ.
"""
import csv
import os
from datetime import datetime
from pathlib import Path
from statistics import mean, median
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BASE_RESULTS_DIR = PROJECT_ROOT / "results" / "tarea2"
EXPERIMENT_NAME = os.getenv("EXPERIMENT_NAME", "").strip()
if EXPERIMENT_NAME:
    RESULTS_DIR = BASE_RESULTS_DIR / EXPERIMENT_NAME
else:
    RESULTS_DIR = BASE_RESULTS_DIR

EVENTS_FILE = RESULTS_DIR / "events.csv"
SUMMARY_FILE = RESULTS_DIR / "summary.csv"

def parse_timestamp(value: str) -> datetime:
    """
    Convierte un timestamp ISO a datetime.
    """
    return datetime.fromisoformat(value)

def percentile(values: list[float], percent: float) -> float:
    """
    Calcula percentil usando interpolación simple.
    """
    if not values:
        return 0.0

    sorted_values = sorted(values)
    if len(sorted_values) == 1:
        return sorted_values[0]

    index = (len(sorted_values) - 1) * percent
    lower = int(index)
    upper = min(lower + 1, len(sorted_values) - 1)
    weight = index - lower
    return sorted_values[lower] * (1 - weight) + sorted_values[upper] * weight

def read_events() -> list[dict[str, Any]]:
    """
    Lee el archivo de eventos generado por metrics_collector.py.
    """
    if not EVENTS_FILE.exists():
        raise FileNotFoundError(
            f"No existe {EVENTS_FILE}. Primero ejecuta una prueba para generar eventos."
        )

    with EVENTS_FILE.open("r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        return list(reader)

def safe_float(value: str) -> float | None:
    """
    Convierte un valor a float si es posible.
    """
    if value == "" or value is None:
        return None

    try:
        return float(value)
    except ValueError:
        return None

def calculate_summary(events: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Calcula métricas a partir de eventos.
    """
    total_events = len(events)
    unique_queries = {event["query_id"] for event in events if event.get("query_id")}

    cache_hits = sum(1 for event in events if event["event_type"] == "cache_hit")
    cache_miss_processed = sum(
        1 for event in events if event["event_type"] == "cache_miss_processed"
    )
    sent_to_retry = sum(1 for event in events if event["event_type"] == "sent_to_retry")
    recovered_from_retry = sum(
        1 for event in events if event["event_type"] == "recovered_from_retry"
    )
    sent_to_dlq = sum(1 for event in events if event["event_type"] == "sent_to_dlq")

    latencies = [
        latency
        for event in events
        if (latency := safe_float(event.get("latency_ms", ""))) is not None
    ]

    timestamps = [
        parse_timestamp(event["timestamp"])
        for event in events
        if event.get("timestamp")
    ]

    if len(timestamps) >= 2:
        elapsed_seconds = (max(timestamps) - min(timestamps)).total_seconds()
    else:
        elapsed_seconds = 0.0

    successful_events = cache_hits + cache_miss_processed + recovered_from_retry
    failed_events = sent_to_retry + sent_to_dlq

    throughput_events = total_events / elapsed_seconds if elapsed_seconds > 0 else 0.0
    throughput_successful_queries = (
        successful_events / elapsed_seconds if elapsed_seconds > 0 else 0.0
    )

    retry_rate = sent_to_retry / total_events if total_events > 0 else 0.0
    recovery_rate = recovered_from_retry / sent_to_retry if sent_to_retry > 0 else 0.0
    dlq_count = sent_to_dlq
    dlq_rate = dlq_count / len(unique_queries) if unique_queries else 0.0
    failure_rate = failed_events / total_events if total_events > 0 else 0.0

    summary = {
        "experiment_name": EXPERIMENT_NAME or "default",
        "total_events": total_events,
        "total_queries": len(unique_queries),
        "successful_events": successful_events,
        "failed_events": failed_events,
        "cache_hits": cache_hits,
        "cache_miss_processed": cache_miss_processed,
        "sent_to_retry": sent_to_retry,
        "recovered_from_retry": recovered_from_retry,
        "elapsed_seconds": round(elapsed_seconds, 4),
        "throughput_events_per_second": round(throughput_events, 4),
        "throughput_successful_queries_per_second": round(throughput_successful_queries, 4),
        "latency_avg_ms": round(mean(latencies), 4) if latencies else 0.0,
        "latency_p50_ms": round(median(latencies), 4) if latencies else 0.0,
        "latency_p95_ms": round(percentile(latencies, 0.95), 4) if latencies else 0.0,
        "retry_rate": round(retry_rate, 4),
        "recovery_rate": round(recovery_rate, 4),
        "sent_to_dlq": sent_to_dlq,
        "dlq_count": dlq_count,
        "dlq_rate": round(dlq_rate, 4),
        "failure_rate": round(failure_rate, 4),
    }

    return summary

def write_summary(summary: dict[str, Any]) -> None:
    """
    Guarda el resumen en summary.csv.
    """
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    with SUMMARY_FILE.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(summary.keys()))
        writer.writeheader()
        writer.writerow(summary)

def print_summary(summary: dict[str, Any]) -> None:
    """
    Muestra el resumen por consola.
    """
    print("\nResumen de métricas:")
    print(f"Experimento: {EXPERIMENT_NAME or 'default'}")
    print(f"Archivo leído: {EVENTS_FILE}")
    print("")

    for key, value in summary.items():
        print(f"{key}: {value}")

    print("")
    print(f"Resumen guardado en: {SUMMARY_FILE}")


def main() -> None:
    events = read_events()
    summary = calculate_summary(events)
    write_summary(summary)
    print_summary(summary)

if __name__ == "__main__":
    main()
