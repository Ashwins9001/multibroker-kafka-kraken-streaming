import json
import time
import uuid
from kafka import KafkaConsumer, KafkaAdminClient
from kafka.errors import KafkaError, NoBrokersAvailable

BOOTSTRAP_SERVERS = ["kafka-1:9092", "kafka-2:9094"]
TOPIC = "kraken-trades"
OUTPUT_FILE = "/app/data/kraken_trades.txt"

# Retry creating KafkaAdminClient until brokers are available
while True:
    try:
        admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
        print("Connected to Kafka brokers for admin tasks")
        break
    except NoBrokersAvailable:
        print("Kafka brokers not ready, retrying admin connection in 5s...")
        time.sleep(5)

# Wait for topic to exist
while True:
    try:
        topics = admin.list_topics()
        if TOPIC in topics:
            print(f"Topic '{TOPIC}' exists. Starting consumer...")
            break
        else:
            print(f"Topic '{TOPIC}' not yet created, retrying in 2s...")
    except KafkaError as e:
        print(f"Error checking topics, retrying in 5s... ({e})")
    time.sleep(2)

# Always read from beginning using unique consumer group
group_id = f"kraken-consumer-{uuid.uuid4()}"

# Retry creating KafkaConsumer until brokers are available
while True:
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            auto_offset_reset="earliest",
            group_id=group_id,
            value_deserializer=lambda v: json.loads(v.decode("utf-8"))
        )
        print("Kafka consumer connected and ready")
        break
    except NoBrokersAvailable:
        print("Kafka brokers not ready, retrying consumer connection in 5s...")
        time.sleep(5)

with open(OUTPUT_FILE, "a") as f:
    for message in consumer:
        trade = message.value
        line = f"{trade['timestamp']} {trade['pair']} {trade['side']} {trade['price']} {trade['volume']}\n"
        f.write(line)
        f.flush()
        print(f"Written trade: {line.strip()}")
