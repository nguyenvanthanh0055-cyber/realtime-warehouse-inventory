#!/bin/bash

TOPIC_NAME="inventory-events"

docker exec inventory-kafka kafka-topics \
  --bootstrap-server inventory-kafka:29092 \
  --create \
  --if-not-exists \
  --topic ${TOPIC_NAME} \
  --partitions 3 \
  --replication-factor 1

docker exec inventory-kafka kafka-topics \
  --bootstrap-server inventory-kafka:29092 \
  --describe \
  --topic ${TOPIC_NAME}