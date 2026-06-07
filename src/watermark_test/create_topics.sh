#!/bin/bash
# Create Kafka topics for watermark testing

echo "Creating Kafka topics..."

docker exec broker kafka-topics --create \
    --topic impressions \
    --bootstrap-server localhost:9092 \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists

docker exec broker kafka-topics --create \
    --topic clicks \
    --bootstrap-server localhost:9092 \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists

echo ""
echo "Topics created. Listing all topics:"
docker exec broker kafka-topics --list --bootstrap-server localhost:9092
