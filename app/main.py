import os
import time
import pandas as pd

from config import NUM_REQUESTS, TRAFFIC_DISTRIBUTION
from response_generator import ResponseGenerator
from cache_service import CacheService
from metrics import MetricsCollector
from traffic_generator import generate_query

def calculate_summary(metrics_file, total_time, redis_stats):
    df = pd.read_csv(metrics_file)

    total_requests = len(df)

    hits_df = df[df["cache_status"] == "HIT"]
    misses_df = df[df["cache_status"] == "MISS"]

    hits = len(hits_df)
    misses = len(misses_df)
    total = hits + misses

    hit_rate = hits / total_requests if total_requests > 0 else 0
    miss_rate = misses / total_requests if total_requests > 0 else 0
    throughput = total_requests / total_time if total_time > 0 else 0

    latency_p50 = df["latency_ms"].quantile(0.50)
    latency_p95 = df["latency_ms"].quantile(0.95)

    t_cache = hits_df["latency_ms"].mean() if hits > 0 else 0
    t_db = misses_df["latency_ms"].mean() if misses > 0 else 0

    cache_efficiency = (
        ((hits * t_cache) - (misses * t_db)) / total
        if total > 0
        else 0
    )

    evicted_keys = redis_stats.get("evicted_keys", 0)

    total_time_minutes = total_time / 60
    eviction_rate = (
        evicted_keys / total_time_minutes
        if total_time_minutes > 0
        else 0
    )

    print("\n===== RESUMEN DEL EXPERIMENTO =====")
    print(f"Distribución de tráfico: {TRAFFIC_DISTRIBUTION}")
    print(f"Consultas ejecutadas: {total_requests}")
    print(f"Hits: {hits}")
    print(f"Misses: {misses}")
    print(f"Hit rate: {hit_rate:.4f}")
    print(f"Miss rate: {miss_rate:.4f}")
    print(f"Throughput: {throughput:.2f} consultas/segundo")
    print(f"Latencia p50: {latency_p50:.4f} ms")
    print(f"Latencia p95: {latency_p95:.4f} ms")

    print("\n===== CACHE EFFICIENCY =====")
    print("Fórmula: (hits * t_cache - misses * t_db) / total")
    print(f"hits: {hits}")
    print(f"misses: {misses}")
    print(f"total: {total}")
    print(f"t_cache promedio HIT: {t_cache:.4f} ms")
    print(f"t_db promedio MISS: {t_db:.4f} ms")
    print(f"Cache efficiency: {cache_efficiency:.4f}")

    print("\n===== EVICTIONS Y MEMORIA =====")
    print(f"Evicted keys: {evicted_keys}")
    print(f"Eviction rate: {eviction_rate:.2f} evictions/min")
    print(f"Memoria usada: {redis_stats.get('used_memory_human', 'N/A')}")
    print(f"Memoria máxima: {redis_stats.get('maxmemory_human', 'N/A')}")
    print(f"Fragmentación de memoria: {redis_stats.get('mem_fragmentation_ratio', 'N/A')}")

def main():
    metrics_file = "results/metrics.csv"

    if os.path.exists(metrics_file):
        os.remove(metrics_file)

    print("Iniciando sistema...")
    print(f"Distribución de tráfico: {TRAFFIC_DISTRIBUTION}")
    print(f"Número de consultas: {NUM_REQUESTS}")

    response_generator = ResponseGenerator()
    cache_service = CacheService(response_generator)
    metrics = MetricsCollector(metrics_file)

    start_time = time.time()

    for i in range(NUM_REQUESTS):
        query = generate_query(TRAFFIC_DISTRIBUTION)

        result = cache_service.handle_query(query)

        metrics.record(query, result)

        if (i + 1) % 100 == 0:
            print(f"Consultas procesadas: {i + 1}/{NUM_REQUESTS}")

    end_time = time.time()
    total_time = end_time - start_time

    redis_stats = cache_service.get_redis_stats()

    calculate_summary(metrics_file, total_time, redis_stats)

    print(f"\nMétricas guardadas en: {metrics_file}")


if __name__ == "__main__":
    main()