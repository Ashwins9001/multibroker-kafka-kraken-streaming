import json
import time
from kafka import KafkaConsumer, KafkaAdminClient
from kafka.errors import KafkaError, NoBrokersAvailable
import os

BOOTSTRAP_SERVERS = ["kafka-1:9092", "kafka-2:9094"]
TOPIC = "kraken-trades"
output_file = os.path.join("data", "kraken_trades.txt")
os.makedirs(os.path.dirname(output_file), exist_ok=True)

def wait_for_admin():
    while True:
        try:
            admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
            print("[ADMIN] Connected to Kafka brokers")
            return admin
        except NoBrokersAvailable:
            print("[ADMIN] Brokers not ready, retrying in 5s...")
            time.sleep(5)

def wait_for_topic(admin_client, topic_name):
    while True:
        try:
            topics = admin_client.list_topics()
            if topic_name in topics:
                print(f"[INFO] Topic '{topic_name}' exists and is active")
                return
            else:
                print(f"[INFO] Topic '{topic_name}' not yet created, retrying in 2s...")
        except KafkaError as e:
            print(f"[ERROR] Could not list topics: {e}")
        time.sleep(2)

def tail_topic(topic_name):
    # Create a KafkaConsumer without committing offsets, starts from earliest
    while True:
        try:
            consumer = KafkaConsumer(
                topic_name,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                auto_offset_reset="earliest",
                group_id=None,
                enable_auto_commit=False,
                value_deserializer=lambda v: json.loads(v.decode("utf-8"))
            )
            print("[CONSUMER] Connected to Kafka, now tailing messages...\n")
            break
        except NoBrokersAvailable:
            print("[CONSUMER] Brokers not ready, retrying in 5s...")
            time.sleep(5)

    # Continuously print new messages
    for message in consumer:
        trade = message.value
        print(f"[MESSAGE] offset={message.offset} partition={message.partition} value={trade}")

        with open(output_file, "a") as f:
            f.write(f"[MESSAGE] offset={message.offset} partition={message.partition} value={trade}\n")

if __name__ == "__main__":
    admin_client = wait_for_admin()
    wait_for_topic(admin_client, TOPIC)
    tail_topic(TOPIC)
