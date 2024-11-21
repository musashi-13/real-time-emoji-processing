#!/bin/bash

# Kafka installation directory
KAFKA_DIR="/usr/local/kafka"

# Kafka broker details
BROKER="localhost:9092"

# Topic names
TOPIC_NAME_1="emoji-topic"
TOPIC_NAME_2="main-pub-topic"
TOPIC_NAME_3="cluster1-emoji-topic"
TOPIC_NAME_4="cluster2-emoji-topic"
TOPIC_NAME_5="cluster3-emoji-topic"

# Function to delete a Kafka topic
delete_topic() {
    echo "Clearing existing topic: $1"
    $KAFKA_DIR/bin/kafka-topics.sh --delete --topic $1 --bootstrap-server $BROKER > /dev/null 2>&1
    sleep 2  # Give Kafka time to process the deletion
}

# Function to create a Kafka topic
create_topic() {
    echo "Creating topic: $1"
    $KAFKA_DIR/bin/kafka-topics.sh --create --topic $1 --bootstrap-server $BROKER --partitions 3 --replication-factor 1
}

# Delete existing topics
delete_topic $TOPIC_NAME_1
delete_topic $TOPIC_NAME_2
delete_topic $TOPIC_NAME_3
delete_topic $TOPIC_NAME_4
delete_topic $TOPIC_NAME_5

# Create the topics with replication-factor 1
create_topic $TOPIC_NAME_1
create_topic $TOPIC_NAME_2
create_topic $TOPIC_NAME_3
create_topic $TOPIC_NAME_4
create_topic $TOPIC_NAME_5

# List all Kafka topics
$KAFKA_DIR/bin/kafka-topics.sh --list --bootstrap-server $BROKER
