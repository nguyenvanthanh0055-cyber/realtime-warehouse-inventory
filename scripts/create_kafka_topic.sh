#!/bin/bash

TOPIC_NAME="inventory-events"

docker exec realtime-warehouse-inventory-kafka-1 kafka-topics \
  --bootstrap-server realtime-warehouse-inventory-kafka-1:29092 \
  --create \
  --if-not-exists \
  --topic ${TOPIC_NAME} \
  --partitions 3 \
  --replication-factor 1

docker exec realtime-warehouse-inventory-kafka-1 kafka-topics \
  --bootstrap-server realtime-warehouse-inventory-kafka-1:29092 \
  --describe \
  --topic ${TOPIC_NAME}