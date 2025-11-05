from kafka import KafkaProducer
import json
import time
import random
import uuid

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def main():
    print("Producing transactions to topic 'transactions'...")
    sequence = 0
    while True:
        transaction_id = str(uuid.uuid4())
        fraud_score = round(random.uniform(0, 100), 2)
        msg = {
            "transaction_id": transaction_id,
            "sequence": sequence,
            "fraud_score": fraud_score,
        }
        producer.send("transactions", msg)
        print("sent:", msg)
        sequence += 1
        time.sleep(1)

if __name__ == "__main__":
    main()
