import os

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

CACHE_TTL = int(os.getenv("CACHE_TTL", 120))
PADDING_SIZE = int(os.getenv("PADDING_SIZE", 0))

NUM_REQUESTS = int(os.getenv("NUM_REQUESTS", 1000))
TRAFFIC_DISTRIBUTION = os.getenv("TRAFFIC_DISTRIBUTION", "uniform")