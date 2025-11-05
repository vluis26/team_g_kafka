from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "transactions",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="logger-service",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

print("Logger service consuming from 'transactions'...")
for msg in consumer:
    print("[LOGGER]", msg.value)
