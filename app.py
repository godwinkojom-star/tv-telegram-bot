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
    direction = signal.get("direction", "")
    confidence = signal.get("confidence", 0)
    risk = signal.get("risk", "UNKNOWN")

    entry = signal["entry"]
    sl = signal["sl"]
    
    # Calculate Risk distance
    risk_distance = abs(entry - sl)
    
    # Set TPs based on Risk/Reward Ratio (1.5x and 2.0x)
    # If Buy: entry + distance | If Sell: entry - distance
    if "BUY" in direction:
        tp1 = entry + (risk_distance * 1.5)
        tp2 = entry + (risk_distance * 2.0)
        tp3 = entry + (risk_distance * 3.0)
    else:
        tp1 = entry - (risk_distance * 1.5)
        tp2 = entry - (risk_distance * 2.0)
        tp3 = entry - (risk_distance * 3.0)

    side = "LONG" if "BUY" in direction else "SHORT"

    sl_pct = abs((sl - entry) / entry) * 100
    tp1_pct = abs((tp1 - entry) / entry) * 100
    tp2_pct = abs((tp2 - entry) / entry) * 100
    tp3_pct = abs((tp3 - entry) / entry) * 100

    msg = (
        f"🚀 <b>SmartFX SIGNAL</b>\n\n"
        f"📊 <b>Pair:</b> {symbol}\n"
        f"⏱ <b>Timeframe:</b> {timeframe}\n"
        f"🏦 <b>Market:</b> {market}\n\n"
        f"🧠 <b>Confidence:</b> {confidence}%\n"
        f"⚠️ <b>Risk Level:</b> {risk}\n"
        f"🎯 <b>Direction:</b> {direction} ({side})\n\n"
        f"💰 <b>Entry:</b> {entry}\n"
        f"🛑 <b>Stop Loss:</b> {sl} (-{sl_pct:.2f}%)\n\n"
        f"💚 <b>TP1:</b> {tp1:.5f} (+{tp1_pct:.2f}%)\n"
        f"💛 <b>TP2:</b> {tp2:.5f} (+{tp2_pct:.2f}%)\n"
        f"🏆 <b>TP3:</b> {tp3:.5f} (+{tp3_pct:.2f}%)\n\n"
        f"⚡ <b>Signal Strength:</b> {'🔥 HIGH' if confidence >= 80 else '✅ GOOD'}\n\n"
        f"🤖 <i>SmartFX Automated Signal</i>\n"
        f"⚠️ <i>Trade safely. Manage risk.</i>"
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
    results = []
    for symbol in CRYPTO_PAIRS:
        for tf_label, tf_interval in CRYPTO_TIMEFRAMES.items():
            try:
                # 1. Fetch 4H data to determine the "Global Trend"
                candles_4h = get_binance_candles(symbol, "4h", limit=200)
                trend = get_trend_direction(candles_4h) 
                
                # 2. Fetch the 15M/1H data for the actual signal
                candles = get_binance_candles(symbol, tf_interval, limit=100)
                
                # 3. Pass the trend into the analysis
                signal = analyze_candles(candles, trend_4h=trend)
                
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
                # 1. You must fetch the candles first!
                candles = get_twelvedata_candles(symbol, tf_interval, limit=100)
                
                # 2. Check if candles were returned
                if not candles:
                    continue
                
                # 3. Analyze the candles
                signal = analyze_candles(candles, trend_4h=None)
                
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
            # FIXED: Changed "1day" to "1h" to prevent Twelve Data helper crash
            candles = get_twelvedata_candles(forex_test_symbol, "1h", limit=2)
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
