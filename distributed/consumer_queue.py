from kafka import KafkaConsumer
import json
import socket

host = socket.gethostname()

consumer = KafkaConsumer(
    "transactions",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="worker-group",
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

print(f"[{host}] Worker consuming from 'transactions'...")

for msg in consumer:
    value = msg.value
    print(f"[{host}] processed transaction_id={value['transaction_id']} sequence={value['sequence']} fraud_score={value['fraud_score']}")
