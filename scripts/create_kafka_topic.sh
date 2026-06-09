#!/bin/bash

TOPIC_NAME="${KAFKA_TOPIC:-inventory-events}"
BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS is required}"
CLIENT_CONFIG="${CLIENT_CONFIG:-$HOME/msk-config/client.properties}"

kafka-topics.sh \
  --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
  --command-config "${CLIENT_CONFIG}" \
  --create \
  --if-not-exists \
  --topic "$KAFKA_TOPIC" \
  --partitions 3
