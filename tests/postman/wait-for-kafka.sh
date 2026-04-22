#!/bin/sh
set -e

echo "Waiting for Kafka to be ready..."
# Проверяем, что Kafka отвечает на --list
until docker exec cinemaabyss-kafka kafka-topics --bootstrap-server kafka:9092 --list > /dev/null 2>&1; do
  echo "Kafka not ready, sleeping 2s..."
  sleep 2
done

echo "Kafka is ready. Ensuring topics exist..."
for topic in movie-events user-events payment-events; do
  docker exec cinemaabyss-kafka kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic "$topic" --partitions 1 --replication-factor 1
done

echo "All topics ready."
