#!/bin/sh

# Exit immediately if a command exits with a non-zero status
set -e

KAFKA_BROKER="kafka:29092"

echo "===================================================="
echo "Starting Kafka Topic Initialization Script..."
echo "===================================================="

# Loop until Kafka is responsive to metadata requests
until kafka-topics --bootstrap-server "$KAFKA_BROKER" --list > /dev/null 2>&1; do
  echo "Kafka broker at $KAFKA_BROKER is not responsive yet. Retrying in 2 seconds..."
  sleep 2
done

echo "Kafka broker is responsive. Creating topics..."

# 1. user-events (standard, 3 partitions, 1 replica)
echo "Creating topic: user-events..."
kafka-topics --bootstrap-server "$KAFKA_BROKER" \
  --create --if-not-exists \
  --topic user-events \
  --partitions 3 \
  --replication-factor 1 \
  --config min.insync.replicas=1

# 2. content-metadata (compacted, 1 partition, 1 replica)
echo "Creating topic: content-metadata (compacted)..."
kafka-topics --bootstrap-server "$KAFKA_BROKER" \
  --create --if-not-exists \
  --topic content-metadata \
  --partitions 1 \
  --replication-factor 1 \
  --config cleanup.policy=compact \
  --config delete.retention.ms=100 \
  --config min.cleanable.dirty.ratio=0.01 \
  --config segment.ms=10000

# 3. feature-store (compacted, 1 partition, 1 replica)
echo "Creating topic: feature-store (compacted)..."
kafka-topics --bootstrap-server "$KAFKA_BROKER" \
  --create --if-not-exists \
  --topic feature-store \
  --partitions 1 \
  --replication-factor 1 \
  --config cleanup.policy=compact \
  --config delete.retention.ms=100 \
  --config min.cleanable.dirty.ratio=0.01 \
  --config segment.ms=10000

# 4. flink-metrics (standard, 1 partition, 1 replica)
echo "Creating topic: flink-metrics..."
kafka-topics --bootstrap-server "$KAFKA_BROKER" \
  --create --if-not-exists \
  --topic flink-metrics \
  --partitions 1 \
  --replication-factor 1

echo "===================================================="
echo "Kafka topics initialized successfully!"
kafka-topics --bootstrap-server "$KAFKA_BROKER" --list
echo "===================================================="
