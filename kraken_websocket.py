import json
import time
from kafka import KafkaProducer
from websocket import WebSocketApp

# ---------- 1. Kafka Producer ----------
producer = KafkaProducer(
    bootstrap_servers=["kafka-1:9092", "kafka-2:9094"],  # list all brokers
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

topic_name = "kraken-trades"

# ---------- 2. WebSocket Callback Functions ----------

def on_open(ws):
    print("WebSocket connection opened")
    # Subscribe to BTC/USD and ETH/USD trades as an example
    subscribe_message = {
        "event": "subscribe",
        "pair": ["BTC/USD", "ETH/USD"],
        "subscription": {"name": "trade"}
    }
    ws.send(json.dumps(subscribe_message))

def on_message(ws, message):
    data = json.loads(message)
    
    # Kraken sends heartbeat or system messages as dict, trades as list
    if isinstance(data, list):
        # data[0] = channel id, data[1] = trades, data[2] = pair
        trades = data[1]
        pair = data[3] if len(data) > 3 else "unknown"
        for trade in trades:
            trade_message = {
                "pair": pair,
                "price": trade[0],
                "volume": trade[1],
                "timestamp": trade[2],
                "side": trade[3]  # 'b' = buy, 's' = sell
            }
            # Send to Kafka
            producer.send(topic_name, trade_message)
            print(f"Sent trade to Kafka: {trade_message}")

def on_error(ws, error):
    print(f"WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    print(f"WebSocket closed: {close_status_code}, {close_msg}")

# ---------- 3. Start WebSocket Connection ----------
ws_url = "wss://ws.kraken.com"
ws_app = WebSocketApp(
    ws_url,
    on_open=on_open,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close
)

# Keep running
ws_app.run_forever()
