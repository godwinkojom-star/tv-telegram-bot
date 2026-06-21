import os
import time
import logging
from flask import Flask, jsonify
import requests

from smc_analysis import analyze_candles

app = Flask(__name__)

# --- TELEGRAM CONFIG ---
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "PUT_YOUR_BOT_TOKEN_HERE")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "PUT_YOUR_CHAT_ID_HERE")
TELEGRAM_API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

# --- TWELVE DATA CONFIG (for forex) ---
TWELVE_DATA_API_KEY = os.environ.get("TWELVE_DATA_API_KEY", "")

logging.basicConfig(level=logging.INFO)

LAST_SIGNALS = {}

# --- WATCHLISTS ---
CRYPTO_PAIRS = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
]

FOREX_PAIRS = [
    "EUR/USD",
    "GBP/USD",
    "XAU/USD",
]

CRYPTO_TIMEFRAMES = {
    "15M": "15m",
    "1H": "1h",
}

FOREX_TIMEFRAMES = {
    "15M": "15min",
    "1H": "1h",
}


@app.route("/", methods=["GET"])
def home():
    return "SmartFX Signal Bot is running.", 200
    
@app.route("/test-telegram", methods=["GET"])
def test_telegram():
    """Sends a simple test message to Telegram to confirm the connection works."""
    try:
        send_to_telegram("✅ Test message from SmartFX Signal Bot. If you see this, the connection works!")
        return jsonify({"status": "ok", "message": "Test message sent"}), 200
    except Exception as e:
        return jsonify({"status": "error", "detail": str(e)}), 500

def get_binance_candles(symbol, interval, limit=100):
    """Fetch OHLC candles from Binance public API (no account needed)."""
    url = "https://data-api.binance.vision/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    resp = requests.get(url, params=params, headers=headers, timeout=15)
    resp.raise_for_status()
    raw = resp.json()
    candles = []
    for c in raw:
        candles.append({
            "open": float(c[1]),
            "high": float(c[2]),
            "low": float(c[3]),
            "close": float(c[4]),
        })
    return candles


def get_twelvedata_candles(symbol, interval, limit=100):
    """Fetch OHLC candles from Twelve Data (free tier, requires API key)."""
    if not TWELVE_DATA_API_KEY:
        return None

    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": symbol,
        "interval": interval,
        "outputsize": limit,
        "apikey": TWELVE_DATA_API_KEY,
    }
    resp = requests.get(url, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    if "values" not in data:
        logging.warning(f"Twelve Data error for {symbol}: {data}")
        return None

    values = list(reversed(data["values"]))
    candles = []
    for c in values:
        candles.append({
            "open": float(c["open"]),
            "high": float(c["high"]),
            "low": float(c["low"]),
            "close": float(c["close"]),
        })
    return candles


def format_signal_message(symbol, timeframe, market, signal):
    direction = signal["direction"]
    emoji = "🟢" if direction == "BUY" else "🔴"
    side = "LONG" if direction == "BUY" else "SHORT"
    
    confidence = signal.get("confidence", 0)

    entry = signal["entry"]
    sl = signal["sl"]
    tp1 = signal["tp1"]
    tp2 = signal["tp2"]
    tp3 = signal["tp3"]

    sl_pct = abs((sl - entry) / entry) * 100
    tp1_pct = abs((tp1 - entry) / entry) * 100
    tp2_pct = abs((tp2 - entry) / entry) * 100
    tp3_pct = abs((tp3 - entry) / entry) * 100

    msg = (
        f"{emoji} <b>SmartFX Alert</b> — <b>{direction} / {side}</b>\n\n"
        f"<b>Ticker:</b> {symbol}\n"
        f"<b>Market:</b> {market}\n"
        f"<b>Timeframe:</b> {timeframe}\n"
        f"<b>Strategy:</b> SMC Confluence\n\n"
        f"<b>Confidence:</b> {confidence}%\n"
        f"🎯 <b>Entry:</b> {entry}\n"
        f"🛑 <b>Stop Loss:</b> {sl} (-{sl_pct:.1f}%)\n"
        f"💚 <b>TP1:</b> {tp1} (+{tp1_pct:.1f}%)\n"
        f"💛 <b>TP2:</b> {tp2} (+{tp2_pct:.1f}%)\n"
        f"🏆 <b>TP3:</b> {tp3} (+{tp3_pct:.1f}%)\n\n"
        f"<i>Trading involves risk. Not financial advice.</i>"
    )
    return msg


def send_to_telegram(message: str):
    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    response = requests.post(TELEGRAM_API_URL, json=payload, timeout=10)
    response.raise_for_status()

def should_send_signal(symbol, timeframe, signal):
    key = f"{symbol}_{timeframe}"

    current_direction = signal["direction"]
    previous_direction = LAST_SIGNALS.get(key)

    if previous_direction == current_direction:
        return False

    LAST_SIGNALS[key] = current_direction
    return True


@app.route("/analyze/crypto", methods=["GET"])
def analyze_crypto():
    """Analyze all crypto pairs across all timeframes, send signals found."""
    results = []
    for symbol in CRYPTO_PAIRS:
        for tf_label, tf_interval in CRYPTO_TIMEFRAMES.items():
            try:
                candles = get_binance_candles(symbol, tf_interval, limit=100)
                signal = analyze_candles(candles)
                if signal and should_send_signal(symbol, tf_label, signal):
                    msg = format_signal_message(symbol, tf_label, "Crypto", signal)
                    send_to_telegram(msg)
                    results.append({"symbol": symbol, "timeframe": tf_label, "signal": signal})
            except Exception as e:
                logging.error(f"Error analyzing {symbol} {tf_label}: {e}")

    return jsonify({"status": "ok", "signals_sent": len(results)}), 200


@app.route("/analyze/forex", methods=["GET"])
def analyze_forex():
    """Analyze all forex pairs across all timeframes, send signals found."""
    if not TWELVE_DATA_API_KEY:
        return jsonify({"status": "error", "detail": "TWELVE_DATA_API_KEY not set"}), 400

    results = []
    for symbol in FOREX_PAIRS:
        for tf_label, tf_interval in FOREX_TIMEFRAMES.items():
            try:
                candles = get_twelvedata_candles(symbol, tf_interval, limit=100)
                time.sleep(0)
                if not candles:
                    continue
                signal = analyze_candles(candles)
                if signal and should_send_signal(symbol, tf_label, signal):
                    msg = format_signal_message(symbol, tf_label, "Forex", signal)
                    send_to_telegram(msg)
                    results.append({"symbol": symbol, "timeframe": tf_label, "signal": signal})
            except Exception as e:
                logging.error(f"Error analyzing {symbol} {tf_label}: {e}")

    return jsonify({"status": "ok", "signals_sent": len(results)}), 200


@app.route("/daily-summary", methods=["GET"])
def daily_summary():
    """
    Lightweight 'is the bot alive' ping.

    NOTE: This used to loop over all 10 crypto pairs x 3 timeframes (30 calls)
    plus all 5 forex pairs (5 calls, 4s sleep each) = 35 sequential network
    round-trips in one request. On Render's free tier that occasionally
    stacked up enough latency to blow past the gunicorn worker timeout,
    which kills the worker mid-request (no clean error, no log output) -
    that's what was causing the Internal Server Error.

    Fix: just check ONE crypto pair and ONE forex pair as a connectivity
    check, instead of re-checking everything the scheduled /analyze routes
    already check every 15 min / hour.
    """
    crypto_test_symbol = CRYPTO_PAIRS[0]
    forex_test_symbol = FOREX_PAIRS[0]

    crypto_ok = False
    try:
        get_binance_candles(crypto_test_symbol, "1h", limit=2)
        crypto_ok = True
    except Exception as e:
        logging.error(f"Daily summary crypto check failed for {crypto_test_symbol}: {e}")

    forex_ok = False
    forex_status = "not configured"
    if TWELVE_DATA_API_KEY:
        try:
            candles = get_twelvedata_candles(forex_test_symbol, "1day", limit=2)
            forex_ok = bool(candles)
            forex_status = "OK" if forex_ok else "FAILED"
        except Exception as e:
            forex_status = "FAILED"
            logging.error(f"Daily summary forex check failed for {forex_test_symbol}: {e}")

    msg = (
        f"📊 <b>Daily Bot Status</b>\n\n"
        f"✅ Crypto data source: {'OK' if crypto_ok else 'FAILED'} ({crypto_test_symbol})\n"
        f"✅ Forex data source: {forex_status} ({forex_test_symbol})\n\n"
        f"Monitoring {len(CRYPTO_PAIRS)} crypto pairs every 15 min and "
        f"{len(FOREX_PAIRS)} forex pairs every hour. "
        f"You'll get a separate alert the moment a real BUY/SELL setup appears."
    )
    send_to_telegram(msg)
    return jsonify({"status": "ok", "crypto_ok": crypto_ok, "forex_ok": forex_ok}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
                       
