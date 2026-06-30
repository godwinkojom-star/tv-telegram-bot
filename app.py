import os
import time
import logging
from flask import Flask, jsonify
import requests
from smc_analysis import analyze_candles

app = Flask(__name__)

# --- CONFIGURATION ---
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
PUBLIC_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "YOUR_PUBLIC_CHANNEL_ID_HERE")
PRIVATE_USER_ID = "8662582348"
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
TWELVE_DATA_API_KEY = os.environ.get("TWELVE_DATA_API_KEY", "")

logging.basicConfig(level=logging.INFO)

# --- ANALYTICS ENGINE (Phase 7) ---
STATS = {"signals_sent": 0, "crypto_signals": 0, "forex_signals": 0}
LAST_SIGNALS = {}

# --- WATCHLISTS ---
CRYPTO_PAIRS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
FOREX_PAIRS = ["EUR/USD", "GBP/USD", "XAU/USD"]
CRYPTO_TIMEFRAMES = {"15M": "15m", "1H": "1h"}
FOREX_TIMEFRAMES = {"15M": "15min", "1H": "1h"}

# --- SENDING FUNCTIONS ---
def send_to_channel(text):
    payload = {"chat_id": PUBLIC_CHAT_ID, "text": text, "parse_mode": "HTML"}
    requests.post(TELEGRAM_API_URL, json=payload, timeout=10)

def send_to_private(text):
    payload = {"chat_id": PRIVATE_USER_ID, "text": text, "parse_mode": "HTML"}
    requests.post(TELEGRAM_API_URL, json=payload, timeout=10)

# --- ROUTES ---
@app.route("/", methods=["GET"])
def home():
    return "SmartFX Signal Bot is running.", 200

@app.route("/test-telegram", methods=["GET"])
def test_telegram():
    send_to_private("✅ Private Command Center is active!")
    return jsonify({"status": "ok"}), 200

@app.route("/system-heartbeat", methods=["GET"])
def system_heartbeat():
    send_to_private("🤖 <b>System Heartbeat:</b> Operational.")
    return jsonify({"status": "ok"}), 200

@app.route("/analyze/crypto", methods=["GET"])
def analyze_crypto():
    results = 0
    for symbol in CRYPTO_PAIRS:
        for tf_label, tf_interval in CRYPTO_TIMEFRAMES.items():
            try:
                candles_4h = get_binance_candles(symbol, "4h", limit=200)
                trend = get_trend_direction(candles_4h)
                candles = get_binance_candles(symbol, tf_interval, limit=100)
                signal = analyze_candles(candles, trend_4h=trend)
                if signal and should_send_signal(symbol, tf_label, signal):
                    send_to_channel(format_signal_message(symbol, tf_label, "Crypto", signal))
                    STATS["signals_sent"] += 1
                    STATS["crypto_signals"] += 1
                    results += 1
            except Exception as e:
                logging.error(f"Error: {e}")
    return jsonify({"status": "ok", "signals_sent": results}), 200

@app.route("/analyze/forex", methods=["GET"])
def analyze_forex():
    results = 0
    for symbol in FOREX_PAIRS:
        for tf_label, tf_interval in FOREX_TIMEFRAMES.items():
            try:
                candles_4h = get_twelvedata_candles(symbol, "4h", limit=200)
                trend = get_trend_direction(candles_4h)
                candles = get_twelvedata_candles(symbol, tf_interval, limit=100)
                if not candles: continue
                signal = analyze_candles(candles, trend_4h=trend)
                if signal and should_send_signal(symbol, tf_label, signal):
                    send_to_channel(format_signal_message(symbol, tf_label, "Forex", signal))
                    STATS["signals_sent"] += 1
                    STATS["forex_signals"] += 1
                    results += 1
            except Exception as e:
                logging.error(f"Error: {e}")
    return jsonify({"status": "ok", "signals_sent": results}), 200

@app.route("/daily-summary", methods=["GET"])
def daily_summary():
    msg = (
        f"📊 <b>Daily Performance Report</b>\n\n"
        f"🚀 Total Signals Sent: {STATS['signals_sent']}\n"
        f"🪙 Crypto Signals: {STATS['crypto_signals']}\n"
        f"💱 Forex Signals: {STATS['forex_signals']}\n\n"
        f"<i>Bot status: Fully Operational.</i>"
    )
    send_to_private(msg)
    return jsonify({"status": "ok", "stats": STATS}), 200

# --- HELPER FUNCTIONS ---
def get_binance_candles(symbol, interval, limit=100):
    url = "https://data-api.binance.vision/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    resp = requests.get(url, params=params, timeout=15)
    raw = resp.json()
    return [{"open": float(c[1]), "high": float(c[2]), "low": float(c[3]), "close": float(c[4])} for c in raw]

def get_twelvedata_candles(symbol, interval, limit=100):
    if not TWELVE_DATA_API_KEY: return None
    url = "https://api.twelvedata.com/time_series"
    params = {"symbol": symbol, "interval": interval, "outputsize": limit, "apikey": TWELVE_DATA_API_KEY}
    resp = requests.get(url, params=params, timeout=15)
    data = resp.json()
    if "values" not in data: return None
    values = list(reversed(data["values"]))
    return [{"open": float(c["open"]), "high": float(c["high"]), "low": float(c["low"]), "close": float(c["close"])} for c in values]

def format_signal_message(symbol, timeframe, market, signal):
    return f"🚀 <b>Signal: {symbol}</b>\nTimeframe: {timeframe}\nMarket: {market}\nDirection: {signal.get('direction')}"

def should_send_signal(symbol, timeframe, signal):
    key = f"{symbol}_{timeframe}"
    if LAST_SIGNALS.get(key) == signal["direction"]: return False
    LAST_SIGNALS[key] = signal["direction"]
    return True

def get_trend_direction(candles):
    return "UP" if candles[-1]['close'] > candles[0]['close'] else "DOWN"
