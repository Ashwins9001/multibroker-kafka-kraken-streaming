import json
from kafka import KafkaProducer
from websocket import WebSocketApp

# ---------- Kafka Producer ----------
producer = KafkaProducer(
    bootstrap_servers=["kafka-1:9092", "kafka-2:9094"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

topic_name = "kraken-trades"

# ---------- WebSocket Callbacks ----------
def on_open(ws):
    print("WebSocket connection opened")
    subscribe_message = {
        "event": "subscribe",
        "pair": ["BTC/USD", "ETH/USD"],
        "subscription": {"name": "trade"}
    }
    ws.send(json.dumps(subscribe_message))

def on_message(ws, message):
    data = json.loads(message)
    if isinstance(data, list):
        trades = data[1]
        pair = data[3] if len(data) > 3 else "unknown"
        for trade in trades:
            trade_message = {
                "pair": pair,
                "price": trade[0],
                "volume": trade[1],
                "timestamp": trade[2],
                "side": trade[3]
            }
            producer.send(topic_name, trade_message)
            print(f"Sent trade to Kafka: {trade_message}")

def on_error(ws, error):
    print(f"WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    print(f"WebSocket closed: {close_status_code}, {close_msg}")

# ---------- Start WebSocket ----------
ws_url = "wss://ws.kraken.com"
ws_app = WebSocketApp(
    ws_url,
    on_open=on_open,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close
)

ws_app.run_forever()
