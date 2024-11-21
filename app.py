from flask import Flask, request, jsonify
from kafka import KafkaProducer
import json
from datetime import datetime

app = Flask(__name__)

KAFKA_BROKER = "localhost:9092"
TOPIC = "emoji-topic"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=500
)

@app.route('/send-emoji', methods=['POST'])
def send_emoji():
    try:
        data = request.json
        user_id = data.get("user_id")
        emoji_type = data.get("emoji_type")
        timestamp = data.get("timestamp")

        if not user_id or not emoji_type or not timestamp:
            return jsonify({"error": "Invalid input, ensure user_id, emoji_type, and timestamp are provided."}), 400

        try:
            timestamp = datetime.fromisoformat(timestamp).isoformat()
        except ValueError:
            return jsonify({"error": "Invalid timestamp format. Use ISO 8601 format."}), 400

        message = {
            "user_id": user_id,
            "emoji": emoji_type,
            "timestamp": timestamp
        }

        producer.send(TOPIC, value=message)

        return jsonify({"status": "success", "message": "Emoji sent to Kafka"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
