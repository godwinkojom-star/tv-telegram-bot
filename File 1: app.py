import os
import logging
from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "PUT_YOUR_BOT_TOKEN_HERE")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "PUT_YOUR_CHAT_ID_HERE")

TELEGRAM_API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

logging.basicConfig(level=logging.INFO)


@app.route("/", methods=["GET"])
def home():
    return "TradingView -> Telegram bridge is running.", 200


@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        if request.is_json:
            data = request.get_json()
            message = data.get("message") if isinstance(data, dict) and "message" in data else str(data)
        else:
            message = request.data.decode("utf-8")

        if not message:
            message = "Received an empty alert from TradingView."

        send_to_telegram(message)
        logging.info(f"Forwarded alert: {message}")
        return jsonify({"status": "ok"}), 200

    except Exception as e:
        logging.error(f"Error processing webhook: {e}")
        return jsonify({"status": "error", "detail": str(e)}), 500


def send_to_telegram(message: str):
    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    response = requests.post(TELEGRAM_API_URL, json=payload, timeout=10)
    response.raise_for_status()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
