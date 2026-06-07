#!/bin/bash

TOPIC_NAME="${KAFKA_TOPIC:-inventory-events}"
BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS}"

kafka-topics.sh \
  --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
  --command-config client.properties \
  --create \
  --if-not-exists \
  --topic "$KAFKA_TOPIC" \
  --partitions 3
