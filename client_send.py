import requests
import random
from datetime import datetime
import time

# List of emojis to simulate
EMOJIS = ["😊", "😂", "❤️", "🔥", "🎉"]

# Endpoint for sending emojis
SEND_EMOJI_URL = "http://localhost:5000/send-emoji"  # API for sending emojis

# Function to send emojis to the main app
def send_emojis():
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
            time.sleep(00.1)  # 100 ms delay between requests
    except Exception as e:
        print(f"Error during emoji sending: {e}")
    print("Finished sending emojis.")

if __name__ == "__main__":
    send_emojis()
