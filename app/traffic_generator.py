import random
import numpy as np

QUERY_TYPES = ["Q1", "Q2", "Q3", "Q4", "Q5"]
ZONE_IDS = ["Z1", "Z2", "Z3", "Z4", "Z5"]


def generate_uniform_query():
    query_type = random.choice(QUERY_TYPES)
    zone_id = random.choice(ZONE_IDS)

    if query_type == "Q4":
        zone_b = random.choice([z for z in ZONE_IDS if z != zone_id])
        return {
            "query_type": query_type,
            "zone_id": zone_id,
            "zone_b": zone_b,
            "confidence_min": random.choice([0.0, 0.5, 0.7])
        }

    if query_type == "Q5":
        return {
            "query_type": query_type,
            "zone_id": zone_id,
            "bins": random.choice([5, 10])
        }

    return {
        "query_type": query_type,
        "zone_id": zone_id,
        "confidence_min": random.choice([0.0, 0.5, 0.7])
    }


def generate_zipf_query():
    query_type = np.random.choice(
        QUERY_TYPES,
        p=[0.40, 0.25, 0.20, 0.10, 0.05]
    )

    zone_id = np.random.choice(
        ZONE_IDS,
        p=[0.35, 0.25, 0.15, 0.15, 0.10]
    )

    if query_type == "Q4":
        zone_b = random.choice([z for z in ZONE_IDS if z != zone_id])
        return {
            "query_type": query_type,
            "zone_id": zone_id,
            "zone_b": zone_b,
            "confidence_min": random.choice([0.0, 0.5, 0.7])
        }

    if query_type == "Q5":
        return {
            "query_type": query_type,
            "zone_id": zone_id,
            "bins": random.choice([5, 10])
        }

    return {
        "query_type": query_type,
        "zone_id": zone_id,
        "confidence_min": random.choice([0.0, 0.5, 0.7])
    }


def generate_query(distribution):
    if distribution == "zipf":
        return generate_zipf_query()

    return generate_uniform_query()