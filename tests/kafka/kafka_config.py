import os
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_QUERIES = os.getenv("TOPIC_QUERIES", "queries-topic")
TOPIC_RETRY = os.getenv("TOPIC_RETRY", "retry-topic")
TOPIC_DLQ = os.getenv("TOPIC_DLQ", "dlq-topic")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "query-consumers")

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))

DEFAULT_NUM_MESSAGES = int(os.getenv("DEFAULT_NUM_MESSAGES", "20"))
DEFAULT_PRODUCER_DELAY = float(os.getenv("DEFAULT_PRODUCER_DELAY", "0.2"))

PRODUCER_CLIENT_ID = os.getenv("PRODUCER_CLIENT_ID", "traffic-producer")
CONSUMER_CLIENT_ID = os.getenv("CONSUMER_CLIENT_ID", "query-consumer")