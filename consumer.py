import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "kraken-trades",
    bootstrap_servers=["kafka-1:9092", "kafka-2:9094"],
    auto_offset_reset="earliest",
    group_id="kraken-consumer-group",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

output_file = "kraken_trades.txt"

print("Starting Kafka consumer... writing trades to file.")

with open(output_file, "a") as f:
    for message in consumer:
        trade = message.value
        trade_line = f"{trade['timestamp']} {trade['pair']} {trade['side']} {trade['price']} {trade['volume']}\n"
        f.write(trade_line)
        f.flush()
        print(f"Written trade: {trade_line.strip()}")
