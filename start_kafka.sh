#!/bin/bash
KAFKA_DIR="/usr/local/kafka"

ZOOKEEPER_LOG="$KAFKA_DIR/logs/zookeeper.log"
KAFKA_LOG="$KAFKA_DIR/logs/kafka.log"

start_zookeeper() {
    echo "Starting ZooKeeper..."
    nohup $KAFKA_DIR/bin/zookeeper-server-start.sh $KAFKA_DIR/config/zookeeper.properties > $ZOOKEEPER_LOG 2>&1 &
    sleep 5
    echo "ZooKeeper started."
}

# Function to start Kafka
start_kafka() {
    echo "Starting Kafka..."
    nohup $KAFKA_DIR/bin/kafka-server-start.sh $KAFKA_DIR/config/server.properties > $KAFKA_LOG 2>&1 &
    sleep 5
    echo "Kafka started."
}

start_zookeeper
start_kafka

echo "ZooKeeper and Kafka have been started successfully!"