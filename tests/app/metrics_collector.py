"""
Registro de métricas del sistema:
Guarda eventos en results/tarea2/events.csv para el análisis de latencia, throughput, reintentos, recuperación y DLQ.
"""
import csv
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RESULTS_DIR = PROJECT_ROOT / os.getenv("RESULTS_DIR", "results/tarea2")
EVENTS_FILE = RESULTS_DIR / "events.csv"
EVENT_FIELDS = [
    "timestamp",
    "event_type",
    "query_id",
    "query_type",
    "zone",
    "cache_key",
    "retry_count",
    "latency_ms",
    "topic",
    "status",
    "error",
]

def ensure_events_file() -> None:
    """
    Crea results/tarea2/events.csv si no existe.
    """
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    if not EVENTS_FILE.exists():
        with EVENTS_FILE.open("w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=EVENT_FIELDS)
            writer.writeheader()

def log_event(
    event_type: str,
    query: dict[str, Any],
    cache_key: str = "",
    retry_count: int | None = None,
    latency_ms: float | None = None,
    topic: str = "",
    status: str = "",
    error: str = "",
) -> None:
    """
    Registra un evento del sistema en results/tarea2/events.csv.
    """
    ensure_events_file()
    row = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event_type": event_type,
        "query_id": query.get("id", ""),
        "query_type": query.get("query_type", ""),
        "zone": query.get("zone", ""),
        "cache_key": cache_key,
        "retry_count": retry_count if retry_count is not None else query.get("retry_count", 0),
        "latency_ms": round(latency_ms, 4) if latency_ms is not None else "",
        "topic": topic,
        "status": status,
        "error": error,
    }

    with EVENTS_FILE.open("a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=EVENT_FIELDS)
        writer.writerow(row)
