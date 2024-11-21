from flask import Flask, jsonify, request
from kafka import KafkaConsumer
import json
import threading
import requests
import os

app = Flask(__name__)

KAFKA_BROKER = "localhost:9092"
CLUSTER_TOPIC = os.getenv("CLUSTER_TOPIC", "cluster1-emoji-topic")
if CLUSTER_TOPIC == 'cluster1-emoji-topic':
    PORT=7001
if CLUSTER_TOPIC == 'cluster2-emoji-topic':
    PORT=7011
if CLUSTER_TOPIC == 'cluster3-emoji-topic':
    PORT=7021
MAX_CLIENTS = 2
# List to store registered client endpoints
registered_clients = []

# Kafka Consumer Thread
def kafka_consumer_thread():
    consumer = KafkaConsumer(
        CLUSTER_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    for message in consumer:
        data = message.value
        print(f"New message received: {data}")
        notify_clients(data)

# Function to notify all registered clients
def notify_clients(data):
    global registered_clients
    for client_url in registered_clients:
        try:
            response = requests.post(client_url, json=data)
            if response.status_code == 200:
                print(f"Data sent to {client_url}: {data}")
            else:
                print(f"Failed to send data to {client_url}. Status: {response.status_code}")
        except Exception as e:
            print(f"Error sending data to {client_url}: {e}")

@app.route('/register', methods=['POST'])
def register_client():
    global registered_clients
    data = request.json
    client_url = data.get("client_url")
    if not client_url:
        return jsonify({"error": "client_url required"}), 400

    if client_url not in registered_clients:
        registered_clients.append(client_url)
        print("A new client has subscribed. Client URL:", client_url)
        return jsonify({"status": "registered", "client_url": client_url}), 200
    else:
        return jsonify({"status": "already_registered", "client_url": client_url}), 200

@app.route('/deregister', methods=['POST'])
def deregister_client():
    global registered_clients
    data = request.json
    client_url = data.get("client_url")
    if not client_url:
        return jsonify({"error": "client_url required"}), 400

    if client_url in registered_clients:
        registered_clients.remove(client_url)
        print("A client has unsubscribed. Client URL:", client_url)
        return jsonify({"status": "deregistered", "client_url": client_url}), 200
    else:
        return jsonify({"status": "not_found", "client_url": client_url}), 404

@app.route("/status", methods=["GET"])
def status():
    """
    Endpoint to provide subscriber capacity status.
    """
    return jsonify({"available_slots": MAX_CLIENTS - len(registered_clients)})

if __name__ == "__main__":
    # Start the Kafka consumer in a separate thread
    threading.Thread(target=kafka_consumer_thread, daemon=True).start()
    # Start the Flask server
    app.run(host="0.0.0.0", port=PORT)
