from flask import Flask, request, jsonify
from google.cloud import pubsub_v1
import json

app = Flask(__name__)
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path("de2024-collin", "payment-events")

@app.route("/", methods=["POST"])
def publish_ticket_request():
    try:
        data = request.get_json()
        session_id = data["session_id"]
        user_id = data["user_id"]
        ticket_type = data["ticket_type"]

        message = {
            "session_id": session_id,
            "user_id": user_id,
            "ticket_type": ticket_type
        }

        future = publisher.publish(topic_path, json.dumps(message).encode("utf-8"))
        message_id = future.result()

        return jsonify({"status": "published", "message_id": message_id}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 400
