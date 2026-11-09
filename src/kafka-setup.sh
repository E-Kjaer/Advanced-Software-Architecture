#!/bin/bash

set -e

BOOTSTRAP_SERVER="kafka:9092"

TOPICS=(
  "sealers-messages"
  "sealers-commands"
  "tappers-messages"
  "tappers-commands"
)

echo "Creating Kafka topics..."

for TOPIC in "${TOPICS[@]}"; do
    echo "Creating topic: ${TOPIC}"

    /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server "$BOOTSTRAP_SERVER" \
        --create \
        --if-not-exists \
        --topic "$TOPIC"
done

echo "All Kafka topics have been created successfully!"