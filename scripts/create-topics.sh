#!/bin/bash

KAFKA_HOME="$HOME/apps/kafka"
BOOTSTRAP="localhost:9092"

"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server "$BOOTSTRAP" \
  --create \
  --if-not-exists \
  --topic orders_raw \
  --partitions 3 \
  --replication-factor 1

"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server "$BOOTSTRAP" \
  --create \
  --if-not-exists \
  --topic orders_dlq \
  --partitions 3 \
  --replication-factor 1

"$KAFKA_HOME/bin/kafka-topics.sh" \
  --bootstrap-server "$BOOTSTRAP" \
  --list
