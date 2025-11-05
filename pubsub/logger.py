from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    # Topic name.
    "transactions",
    bootstrap_servers="localhost:9092",
    # The consumer will start reading from the earliest message in the topic.
    auto_offset_reset="earliest",
    # Name of the consumer group. Two consumers can have the same group id, but will cover this later.
    group_id="logger-service",
    # Deserialize from plain text to JSON.
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

print("Logger service consuming from 'transactions'...")
# This is a blocking call. The consumer will wait for new messages to be published to the topic.
for msg in consumer:
    print("[LOGGER]", msg.value)
