import json
import os
from typing import Any
import redis
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "1800"))

def create_redis_client() -> redis.Redis:
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
    )

def build_cache_key(query: dict[str, Any]) -> str:
    
    query_type = query.get("query_type")
    zone = query.get("zone")
    params = query.get("params", {})
    confidence = params.get("confidence", "none")

    if query_type == "Q4":
        zone_b = params.get("zone_b", "none")
        return f"{query_type}:zone={zone}:zone_b={zone_b}:conf={confidence}"

    if query_type == "Q5":
        bins = params.get("bins", "none")
        return f"{query_type}:zone={zone}:bins={bins}"

    return f"{query_type}:zone={zone}:conf={confidence}"

def get_from_cache(redis_client: redis.Redis, cache_key: str) -> dict[str, Any] | None:
    cached_value = redis_client.get(cache_key)

    if cached_value is None:
        return None
    return json.loads(cached_value)

def save_to_cache(
    redis_client: redis.Redis,
    cache_key: str,
    response: dict[str, Any],
    ttl_seconds: int = CACHE_TTL_SECONDS,
) -> None:
    redis_client.setex(
        cache_key,
        ttl_seconds,
        json.dumps(response),
    )