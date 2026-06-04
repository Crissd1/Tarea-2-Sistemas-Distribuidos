import csv
import os
import time


class MetricsCollector:
    def __init__(self, output_file="results/metrics.csv"):
        self.output_file = output_file

        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)

        self.fieldnames = [
            "timestamp",
            "query_type",
            "zone_id",
            "zone_b",
            "cache_status",
            "cache_key",
            "latency_ms"
        ]

        if not os.path.exists(self.output_file):
            self._create_file()

    def _create_file(self):
        with open(self.output_file, mode="w", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=self.fieldnames)
            writer.writeheader()

    def record(self, query, result):
        row = {
            "timestamp": time.time(),
            "query_type": query.get("query_type"),
            "zone_id": query.get("zone_id"),
            "zone_b": query.get("zone_b", ""),
            "cache_status": result.get("cache_status"),
            "cache_key": result.get("cache_key"),
            "latency_ms": result.get("latency_ms")
        }

        with open(self.output_file, mode="a", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=self.fieldnames)
            writer.writerow(row)