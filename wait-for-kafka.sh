#!/bin/sh
# Wait until both Kafka brokers are ready and create topic

MAX_TRIES=30
BROKERS=("kafka-1:9092" "kafka-2:9094")

for broker in "${BROKERS[@]}"; do
  for i in $(seq 1 $MAX_TRIES); do
    if kafka-topics --bootstrap-server $broker --list >/dev/null 2>&1; then
      echo "$broker is ready"
      break
    else
      echo "Waiting for $broker..."
      sleep 5
    fi
    if [ $i -eq $MAX_TRIES ]; then
      echo "ERROR: $broker not ready after $MAX_TRIES tries"
      exit 1
    fi
  done
done

# Create topic if it does not exist
kafka-topics --create --topic kraken-trades \
  --bootstrap-server kafka-1:9092,kafka-2:9094 \
  --partitions 3 --replication-factor 2 || echo "Topic already exists"

exec "$@"  # Keep container running if needed
