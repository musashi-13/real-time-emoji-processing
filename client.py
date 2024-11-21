from flask import Flask, request, jsonify
import requests
import random
from datetime import datetime
import time
import threading
import logging
import os

app = Flask(__name__)

SUBSCRIBERS = [
    {"url": "http://localhost:7001/status", "register_url": "http://localhost:7001/register"},
    {"url": "http://localhost:7011/status", "register_url": "http://localhost:7011/register"},
    {"url": "http://localhost:7021/status", "register_url": "http://localhost:7021/register"},
    {"url": "http://localhost:7002/status", "register_url": "http://localhost:7002/register"},
    {"url": "http://localhost:7012/status", "register_url": "http://localhost:7012/register"},
    {"url": "http://localhost:7022/status", "register_url": "http://localhost:7022/register"},
]  # Add more subscribers as needed

PORT = int(os.getenv("PORT", 8001))  # Client's port
CLIENT_URL = f"http://localhost:{PORT}"  # Client URL
current_subscriber = None

# Suppress Flask logs
log = logging.getLogger("werkzeug")
log.setLevel(logging.ERROR)

EMOJIS = ["😊", "😂", "❤️", "🔥", "🎉"]  # Emojis to simulate
SEND_EMOJI_URL = "http://localhost:5000/send-emoji"  # Endpoint for sending emojis


@app.route("/", methods=["POST"])
def receive_emoji():
    """
    Endpoint to receive emoji data from the subscriber.
    """
    data = request.json
    if not data:
        return jsonify({"error": "Invalid data"}), 400

    emoji = data.get("emoji")
    count = data.get("count")
    if emoji:
        print(emoji * count)  # Print emoji `count` times for better visibility
    else:
        print("Received data without an emoji.")
    return jsonify({"status": "received"}), 200


def find_available_subscriber():
    """
    Finds a subscriber with available capacity.
    """
    for subscriber in SUBSCRIBERS:
        try:
            response = requests.get(subscriber["url"])
            if response.status_code == 200:
                available_slots = response.json().get("available_slots", 0)
                if available_slots > 0:
                    return subscriber["register_url"]
        except Exception as e:
            print(f"Error checking subscriber status: {e}")
    return None


def register_with_subscriber():
    """
    Registers the client with an available subscriber.
    """
    global current_subscriber
    current_subscriber = find_available_subscriber()
    if not current_subscriber:
        print("No subscribers available. Exiting...")
        exit(1)

    try:
        payload = {"client_url": CLIENT_URL}
        response = requests.post(current_subscriber, json=payload)
        if response.status_code in [200, 201]:
            print("Registered successfully with subscriber:", response.json())
        else:
            print(f"Failed to register. Status code: {response.status_code}, Response: {response.text}")
            exit(1)
    except Exception as e:
        print(f"Error while registering with subscriber: {e}")
        exit(1)


def listen_for_unsub():
    """
    Listens for the 'unsub' command to deregister the client.
    """
    global current_subscriber
    while True:
        command = input().strip().lower()
        if command == "unsub" and current_subscriber:
            try:
                payload = {"client_url": CLIENT_URL}
                requests.post(current_subscriber.replace("/register", "/deregister"), json=payload)
                print("Unsubscribed successfully.")
                exit(0)
            except Exception as e:
                print(f"Error while unsubscribing: {e}")
                exit(1)


def send_emojis():
    """
    Function to send emojis to the main app
    """
    print("Starting to send emojis...")
    try:
        for _ in range(100):  # Simulate sending 100 emoji events
            payload = {
                "user_id": f"user{random.randint(1, 100)}",
                "emoji_type": random.choice(EMOJIS),
                "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")  # UTC timestamp
            }
            response = requests.post(SEND_EMOJI_URL, json=payload)
            if response.status_code == 200:
                print(f"Emoji sent successfully: {payload}")
            else:
                print(f"Failed to send emoji: {response.status_code}, {response.text}")
            time.sleep(0.1)  # 100 ms delay between requests
    except Exception as e:
        print(f"Error during emoji sending: {e}")
    print("Finished sending emojis.")


if __name__ == "__main__":
    # Register the client with a subscriber
    register_with_subscriber()

    # Start the unsubscribe listener in a separate thread
    threading.Thread(target=listen_for_unsub, daemon=True).start()

    # Start the emoji sending function in a separate thread
    threading.Thread(target=send_emojis, daemon=True).start()

    # Start the Flask server
    print(f"Listening for emojis on {CLIENT_URL}...")
    app.run(host="0.0.0.0", port=PORT)
