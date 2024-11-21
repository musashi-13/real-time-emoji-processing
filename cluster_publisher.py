import os
from kafka import KafkaConsumer, KafkaProducer
import json

KAFKA_BROKER = "localhost:9092"
MAIN_TOPIC = "main-pub-topic"

# Use environment variable or argument to identify the cluster
CLUSTER_TOPIC = os.getenv("CLUSTER_TOPIC", "cluster1-emoji-topic")

consumer = KafkaConsumer(
    MAIN_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    group_id=CLUSTER_TOPIC,  # Shared group ensures all clusters consume same data
    auto_offset_reset="earliest"
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def distribute_data():
    for message in consumer:
        # Consume from main-pub-topic and forward to cluster-specific topic
        data = json.loads(message.value.decode('utf-8'))
        producer.send(CLUSTER_TOPIC, value=data)
        producer.flush()
        print(f"Forwarded data to {CLUSTER_TOPIC}: {data}")

if __name__ == "__main__":
    distribute_data()
