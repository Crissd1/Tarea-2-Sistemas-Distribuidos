"""
Adaptador de mensajes entre Kafka y el generador de respuestas: Convierte las consultas recibidas desde Kafka al formato de ResponseGenerator.
"""
import sys
from pathlib import Path
from typing import Any
PROJECT_ROOT = Path(__file__).resolve().parents[2]
APP_DIR = PROJECT_ROOT / "app"
sys.path.append(str(APP_DIR))
from response_generator import ResponseGenerator

_response_generator: ResponseGenerator | None = None

def get_response_generator() -> ResponseGenerator:
    global _response_generator

    if _response_generator is None:
        dataset_path = PROJECT_ROOT / "data" / "buildings_zones.csv"
        _response_generator = ResponseGenerator(dataset_path=str(dataset_path))

    return _response_generator

def adapt_kafka_query(kafka_query: dict[str, Any]) -> dict[str, Any]:
    """
    Convierte una consulta Kafka al formato usado por ResponseGenerator.
    """
    query_type = kafka_query.get("query_type")
    params = kafka_query.get("params", {})

    adapted_query: dict[str, Any] = {
        "query_type": query_type,
        "zone_id": kafka_query.get("zone"),
    }

    if query_type in ["Q1", "Q2", "Q3"]:
        adapted_query["confidence_min"] = params.get("confidence", 0.0)

    if query_type == "Q4":
        adapted_query["confidence_min"] = params.get("confidence", 0.0)
        adapted_query["zone_b"] = params.get("zone_b")

    if query_type == "Q5":
        adapted_query["bins"] = params.get("bins", 5)

    return adapted_query

def generate_response(kafka_query: dict[str, Any]) -> dict[str, Any]:
    """
    Ejecuta consultas Q1-Q5 usando ResponseGenerator.
    """
    response_generator = get_response_generator()
    adapted_query = adapt_kafka_query(kafka_query)
    response = response_generator.process_query(adapted_query)

    return {
        "query_id": kafka_query.get("id"),
        "query_type": kafka_query.get("query_type"),
        "zone": kafka_query.get("zone"),
        "params": kafka_query.get("params", {}),
        "result": response,
        "source": "response_generator",
    }