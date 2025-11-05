from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "transactions",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="dashboard-service",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

print("Dashboard service consuming from 'transactions'...")
for msg in consumer:
    print("[DASHBOARD]", msg.value)
