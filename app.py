import os
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
STATS = {"signals_sent": 0, "crypto_signals": 0, "forex_signals": 0}
LAST_SIGNALS = {}

CRYPTO_PAIRS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
FOREX_PAIRS = ["EUR/USD", "GBP/USD", "XAU/USD"]
CRYPTO_TIMEFRAMES = {"15M": "15m", "1H": "1h"}
FOREX_TIMEFRAMES = {"15M": "15min", "1H": "1h"}

# --- INDICATORS (New) ---
def calculate_ema(closes, period=20):
    ema = closes[0]
    multiplier = 2 / (period + 1)
    for price in closes[1:]:
        ema = (price - ema) * multiplier + ema
    return ema

def calculate_macd(closes):
    return calculate_ema(closes, 12) - calculate_ema(closes, 26)

# --- SENDING FUNCTIONS ---
def send_to_channel(text):
    requests.post(TELEGRAM_API_URL, json={"chat_id": PUBLIC_CHAT_ID, "text": text, "parse_mode": "HTML"}, timeout=10)

def send_to_private(text):
    requests.post(TELEGRAM_API_URL, json={"chat_id": PRIVATE_USER_ID, "text": text, "parse_mode": "HTML"}, timeout=10)

# --- ROUTES ---
@app.route("/", methods=["GET"])
def home():
    return "SmartFX Signal Bot is running.", 200

@app.route("/analyze/crypto", methods=["GET"])
def analyze_crypto():
    results = 0
    for symbol in CRYPTO_PAIRS:
        for tf_label, tf_interval in CRYPTO_TIMEFRAMES.items():
            try:
                candles = get_binance_candles(symbol, tf_interval, limit=100)
                closes = [c['close'] for c in candles]
                ema, macd = calculate_ema(closes), calculate_macd(closes)
                
                signal = analyze_candles(candles, trend_4h=get_trend_direction(get_binance_candles(symbol, "4h", limit=200)))
                # The Golden Filter: Trend must align with EMA and MACD
                if signal and closes[-1] > ema and macd > 0 and should_send_signal(symbol, tf_label, signal):
                    msg = f"🚀 <b>Signal: {symbol}</b>\nTrend: Bullish (EMA/MACD)\n{format_signal_message(symbol, tf_label, 'Crypto', signal)}"
                    send_to_channel(msg)
                    STATS["signals_sent"] += 1; STATS["crypto_signals"] += 1; results += 1
            except Exception as e:
                logging.error(f"Error analyzing {symbol}: {e}")
    return jsonify({"status": "ok", "signals_sent": results}), 200

@app.route("/analyze/forex", methods=["GET"])
def analyze_forex():
    results = 0
    for symbol in FOREX_PAIRS:
        try:
            candles = get_twelvedata_candles(symbol, "1h", limit=100)
            if not candles: continue
            closes = [c['close'] for c in candles]
            ema, macd = calculate_ema(closes), calculate_macd(closes)
            
            signal = analyze_candles(candles, trend_4h=get_trend_direction(get_twelvedata_candles(symbol, "4h", limit=200)))
            if signal and closes[-1] > ema and macd > 0 and should_send_signal(symbol, "1H", signal):
                msg = f"🚀 <b>Signal: {symbol}</b>\nTrend: Bullish (EMA/MACD)\n{format_signal_message(symbol, '1H', 'Forex', signal)}"
                send_to_channel(msg)
                STATS["signals_sent"] += 1; STATS["forex_signals"] += 1; results += 1
        except Exception as e:
            logging.error(f"Error analyzing {symbol}: {e}")
    return jsonify({"status": "ok", "signals_sent": results}), 200

@app.route("/daily-summary", methods=["GET"])
def daily_summary():
    msg = f"📊 <b>Daily Report:</b> {STATS['signals_sent']} signals sent."
    send_to_private(msg)
    return jsonify({"status": "ok", "stats": STATS}), 200

# --- HELPER FUNCTIONS ---
def get_binance_candles(symbol, interval, limit=100):
    url = "https://data-api.binance.vision/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    resp = requests.get(url, params=params, timeout=15)
    return [{"close": float(c[4])} for c in resp.json()]

def get_twelvedata_candles(symbol, interval, limit=100):
    if not TWELVE_DATA_API_KEY: return None
    url = f"https://api.twelvedata.com/time_series?symbol={symbol}&interval={interval}&outputsize={limit}&apikey={TWELVE_DATA_API_KEY}"
    resp = requests.get(url, timeout=15)
    data = resp.json()
    return [{"close": float(c["close"])} for c in reversed(data.get("values", []))]

def format_signal_message(symbol, timeframe, market, signal):
    return f"Timeframe: {timeframe}\nMarket: {market}\nDirection: {signal.get('direction')}"

def should_send_signal(symbol, timeframe, signal):
    key = f"{symbol}_{timeframe}"
    if LAST_SIGNALS.get(key) == signal["direction"]: return False
    LAST_SIGNALS[key] = signal["direction"]
    return True

def get_trend_direction(candles):
    return "UP" if candles[-1]['close'] > candles[0]['close'] else "DOWN"

if __name__ == "__main__":
    app.run()
