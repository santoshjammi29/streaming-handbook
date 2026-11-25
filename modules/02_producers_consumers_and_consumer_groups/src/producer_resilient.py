import json
from kafka import KafkaProducer

def create_resilient_producer(bootstrap_servers: list[str]) -> KafkaProducer:
    # Production producer: Idempotent with maximum persistence
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        acks='all',
        enable_idempotence=True,
        retries=10,
        max_in_flight_requests_per_connection=5,
        compression_type='zstd',
        linger_ms=20,
        batch_size=32768,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
