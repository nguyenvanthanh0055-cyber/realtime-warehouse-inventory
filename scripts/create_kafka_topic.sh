#!/bin/bash

TOPIC_NAME="${KAFKA_TOPIC:-inventory-events}"
BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS}"

kafka-topics \
  --bootstrap-server "$BOOTSTRAP_SERVERS" \
  --create \
  --if-not-exists \
  --topic "$TOPIC_NAME" \
  --partitions 3 \
  --replication-factor 2

kafka-topics \
  --bootstrap-server "$BOOTSTRAP_SERVERS" \
  --describe \
  --topic "$TOPIC_NAME"