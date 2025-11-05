from kafka import KafkaConsumer, KafkaProducer
import json

consumer = KafkaConsumer(
    "transactions",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="latest",
    group_id="stream-processor",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

FRAUD_THRESHOLD = 75.0

print("Stream processor watching for fraudulent transactions...")

for msg in consumer:
    transaction = msg.value
    if transaction["fraud_score"] > FRAUD_THRESHOLD:
        alert = {
            "transaction_id": transaction["transaction_id"],
            "sequence": transaction["sequence"],
            "fraud_score": transaction["fraud_score"],
            "alert": "FRAUD_DETECTED",
        }
        print("ALERT GENERATED:", alert)
        producer.send("alerts", alert)
