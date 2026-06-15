
import os
import logging
import imaplib
import email
from email.header import decode_header
from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

# --- TELEGRAM CONFIG ---
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "PUT_YOUR_BOT_TOKEN_HERE")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "PUT_YOUR_CHAT_ID_HERE")
TELEGRAM_API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

# --- GMAIL CONFIG (for free email-to-webhook bridge) ---
GMAIL_ADDRESS = os.environ.get("GMAIL_ADDRESS", "PUT_YOUR_GMAIL_HERE")
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "PUT_YOUR_APP_PASSWORD_HERE")
IMAP_SERVER = "imap.gmail.com"

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
        logging.info(f"Forwarded webhook alert: {message}")
        return jsonify({"status": "ok"}), 200

    except Exception as e:
        logging.error(f"Error processing webhook: {e}")
        return jsonify({"status": "error", "detail": str(e)}), 500


@app.route("/check-email", methods=["GET"])
def check_email():
    try:
        forwarded = 0
        mail = imaplib.IMAP4_SSL(IMAP_SERVER)
        mail.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
        mail.select("inbox")

        status, messages = mail.search(None, '(UNSEEN FROM "noreply@tradingview.com")')

        if status == "OK":
            email_ids = messages[0].split()
            for eid in email_ids:
                status, msg_data = mail.fetch(eid, "(RFC822)")
                if status != "OK":
                    continue

                msg = email.message_from_bytes(msg_data[0][1])

                subject, encoding = decode_header(msg["Subject"])[0]
                if isinstance(subject, bytes):
                    subject = subject.decode(encoding or "utf-8")

                body = ""
                if msg.is_multipart():
                    for part in msg.walk():
                        content_type = part.get_content_type()
                        if content_type == "text/plain":
                            body = part.get_payload(decode=True).decode(errors="ignore")
                            break
                else:
                    body = msg.get_payload(decode=True).decode(errors="ignore")

                full_message = f"<b>{subject}</b>\n\n{body.strip()}"
                send_to_telegram(full_message)
                forwarded += 1

                mail.store(eid, "+FLAGS", "\\Seen")

        mail.logout()
        logging.info(f"Checked email. Forwarded {forwarded} new alert(s).")
        return jsonify({"status": "ok", "forwarded": forwarded}), 200

    except Exception as e:
        logging.error(f"Error checking email: {e}")
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
