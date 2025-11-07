import json
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError, TopicAlreadyExistsError
from kafka.admin import KafkaAdminClient, NewTopic
from websocket import WebSocketApp

BOOTSTRAP_SERVERS = ["kafka-1:9092", "kafka-2:9094"]
TOPIC = "kraken-trades"

# Retry Kafka connection & admin client
while True:
    try:
        admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
        print("Connected to Kafka brokers")
        break
    except KafkaError as e:
        print(f"Kafka not ready, retrying in 5s... ({e})")
        time.sleep(5)

# Ensure topic exists
try:
    if TOPIC not in admin.list_topics():
        admin.create_topics([NewTopic(name=TOPIC, num_partitions=3, replication_factor=2)])
        print(f"Topic '{TOPIC}' created")
    else:
        print(f"Topic '{TOPIC}' already exists")
except TopicAlreadyExistsError:
    print(f"Topic '{TOPIC}' already exists (caught exception)")

# Kafka producer
while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        print("Kafka producer ready")
        break
    except KafkaError as e:
        print(f"Kafka not ready for producer, retrying in 5s... ({e})")
        time.sleep(5)

# WebSocket callbacks
def on_open(ws):
    print("WebSocket connection opened")
    ws.send(json.dumps({
        "event": "subscribe",
        "pair": ["BTC/USD", "ETH/USD"],
        "subscription": {"name": "trade"}
    }))

def on_message(ws, message):
    data = json.loads(message)
    if isinstance(data, list) and len(data) >= 4:
        trades = data[1]
        pair = data[3]
        for trade in trades:
            trade_message = {
                "pair": pair,
                "price": trade[0],
                "volume": trade[1],
                "timestamp": trade[2],
                "side": trade[3]
            }
            producer.send(TOPIC, trade_message)

def on_error(ws, error):
    print(f"WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    print(f"WebSocket closed: {close_status_code}, {close_msg}")

ws_app = WebSocketApp(
    "wss://ws.kraken.com",
    on_open=on_open,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close
)

ws_app.run_forever()
