import json
import time
import redis

from config import REDIS_HOST, REDIS_PORT, CACHE_TTL, PADDING_SIZE


class CacheService:
    def __init__(self, response_generator):
        self.response_generator = response_generator

        self.redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            decode_responses=True
        )

        self.redis_client.ping()
        print("Conectado correctamente a Redis.")

    def build_cache_key(self, query):
        query_type = query["query_type"]

        if query_type in ["Q1", "Q2", "Q3"]:
            return (
                f"{query_type}:"
                f"zone={query['zone_id']}:"
                f"conf={query.get('confidence_min', 0.0)}"
            )

        if query_type == "Q4":
            return (
                f"{query_type}:"
                f"zone_a={query['zone_id']}:"
                f"zone_b={query['zone_b']}:"
                f"conf={query.get('confidence_min', 0.0)}"
            )

        if query_type == "Q5":
            return (
                f"{query_type}:"
                f"zone={query['zone_id']}:"
                f"bins={query.get('bins', 5)}"
            )

        raise ValueError(f"Tipo de consulta no reconocido: {query_type}")

    def handle_query(self, query):
        start_time = time.time()

        cache_key = self.build_cache_key(query)

        cached_response = self.redis_client.get(cache_key)

        if cached_response is not None:
            end_time = time.time()

            return {
                "cache_status": "HIT",
                "cache_key": cache_key,
                "response": json.loads(cached_response),
                "latency_ms": (end_time - start_time) * 1000
            }

        response = self.response_generator.process_query(query)

        response_to_cache = response.copy()

        if PADDING_SIZE > 0:
            response_to_cache["padding"] = "x" * PADDING_SIZE

        self.redis_client.setex(
            cache_key,
            CACHE_TTL,
            json.dumps(response_to_cache)
        )

        end_time = time.time()

        return {
            "cache_status": "MISS",
            "cache_key": cache_key,
            "response": response,
            "latency_ms": (end_time - start_time) * 1000
        }
    
    def get_redis_stats(self):
        stats = self.redis_client.info("stats")
        memory = self.redis_client.info("memory")

        return {
            "keyspace_hits": stats.get("keyspace_hits", 0),
            "keyspace_misses": stats.get("keyspace_misses", 0),
            "evicted_keys": stats.get("evicted_keys", 0),
            "expired_keys": stats.get("expired_keys", 0),
            "used_memory_human": memory.get("used_memory_human", "N/A"),
            "maxmemory_human": memory.get("maxmemory_human", "N/A"),
            "mem_fragmentation_ratio": memory.get("mem_fragmentation_ratio", "N/A")
        }