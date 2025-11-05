from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "alerts",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="alerts-dashboard",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

print("Fraud alerts dashboard listening on 'alerts'...")
for msg in consumer:
    print("🚨 FRAUD ALERT:", msg.value)
