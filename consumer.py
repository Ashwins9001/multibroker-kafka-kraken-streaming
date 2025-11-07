import json
from kafka import KafkaConsumer

# ---------- Kafka Consumer ----------
consumer = KafkaConsumer(
    "kraken-trades",
    bootstrap_servers=["kafka-1:9092", "kafka-2:9094"],
    auto_offset_reset="earliest",
    group_id="kraken-consumer-group",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

# ---------- Output File ----------
output_file = "kraken_trades.txt"

print("Starting Kafka consumer... writing trades to file.")

# ---------- Consume messages ----------
with open(output_file, "a") as f:
    for message in consumer:
        trade = message.value
        trade_line = f"{trade['timestamp']} {trade['pair']} {trade['side']} {trade['price']} {trade['volume']}\n"
        
        # Write to file
        f.write(trade_line)
        f.flush()  # make sure it’s written immediately

        # Also print to console
        print(f"Written trade: {trade_line.strip()}")
