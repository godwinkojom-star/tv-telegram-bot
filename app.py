import os
import logging
import threading
from flask import Flask, jsonify
import requests
from datetime import datetime
from smc_analysis import analyze_candles

app = Flask(__name__)

# --- CONFIGURATION ---
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
PUBLIC_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "YOUR_PUBLIC_CHANNEL_ID_HERE")
PRIVATE_USER_ID = "8662582348"
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
TWELVE_DATA_API_KEY = os.environ.get("TWELVE_DATA_API_KEY", "")

logging.basicConfig(level=logging.INFO)
STATS = {"signals_sent": 0, "crypto_signals": 0, "forex_signals": 0, "wins": 0, "losses": 0}
LAST_SIGNALS = {}
ACTIVE_TRADES = []

CRYPTO_PAIRS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
FOREX_PAIRS = ["EUR/USD", "GBP/USD", "XAU/USD"]
CRYPTO_TIMEFRAMES = {"15M": "15m", "1H": "1h"}
FOREX_TIMEFRAMES = {"15M": "15min", "1H": "1h"}

# --- HELPERS ---
def is_market_active():
    """Only trade during London (07:00-16:00) or NY (12:00-21:00) UTC."""
    h = datetime.utcnow().hour
    return (7 <= h < 16) or (12 <= h < 21)

def calculate_ema(closes, period=20):
    ema = closes[0]
    multiplier = 2 / (period + 1)
    for price in closes[1:]:
        ema = (price - ema) * multiplier + ema
    return ema

def calculate_macd(closes):
    return calculate_ema(closes, 12) - calculate_ema(closes, 26)

def send_to_channel(text):
    requests.post(TELEGRAM_API_URL, json={"chat_id": PUBLIC_CHAT_ID, "text": text, "parse_mode": "HTML"}, timeout=10)

def send_to_private(text):
    requests.post(TELEGRAM_API_URL, json={"chat_id": PRIVATE_USER_ID, "text": text, "parse_mode": "HTML"}, timeout=10)

def should_send_signal(symbol, timeframe, signal):
    key = f"{symbol}_{timeframe}"
    if LAST_SIGNALS.get(key) == signal["direction"]: return False
    LAST_SIGNALS[key] = signal["direction"]
    return True

# --- TRADE MONITORING ---
@app.route("/monitor-trades", methods=["GET"])
def monitor_trades():
    global ACTIVE_TRADES
    for trade in ACTIVE_TRADES[:]:
        current_price = get_live_price(trade['symbol'])
        if current_price >= trade['tp']:
            send_to_channel(f"✅ <b>TP Hit: {trade['symbol']}</b>. Profit secured!")
            STATS["wins"] += 1; ACTIVE_TRADES.remove(trade)
        elif current_price <= trade['sl']:
            send_to_channel(f"❌ <b>SL Hit: {trade['symbol']}</b>. Loss recorded!")
            STATS["losses"] += 1; ACTIVE_TRADES.remove(trade)
    return jsonify({"status": "monitoring", "active_count": len(ACTIVE_TRADES)})

def get_live_price(symbol):
    try: return float(requests.get(f"https://api.binance.com/api/v3/ticker/price?symbol={symbol.replace('/', '')}", timeout=5).json()['price'])
    except: return 0

def perform_crypto_analysis():
    # Heartbeat
    if datetime.utcnow().minute == 0:
        send_to_private("🤖 Crypto Scanner: Healthy and active...")
        
    if not is_market_active(): return 
    # ... rest of your existing code

# --- ANALYSIS TASKS ---
def perform_crypto_analysis():
    if not is_market_active(): return
    for symbol in CRYPTO_PAIRS:
        for tf_label, tf_interval in CRYPTO_TIMEFRAMES.items():
            try:
                candles = get_binance_candles(symbol, tf_interval, limit=100)
                closes = [c['close'] for c in candles]
                # Pass the 4H trend into your analysis
                trend_4h = get_trend_direction(get_binance_candles(symbol, "4h", limit=200))
                signal = analyze_candles(candles, trend_4h=trend_4h)
                
                # Check if signal is not None and matches trend
                if signal and signal['signal']:
                    if should_send_signal(symbol, tf_label, signal):
                        entry = closes[-1]
                        # Set TP/SL dynamically based on direction
                        if signal['signal'] == "BUY":
                            tp, sl = entry * 1.01, entry * 0.995
                        else:
                            tp, sl = entry * 0.99, entry * 1.005
                            
                        ACTIVE_TRADES.append({'symbol': symbol, 'entry': entry, 'tp': tp, 'sl': sl})
                        send_to_channel(f"🚀 <b>Crypto: {symbol}</b> ({tf_label})\nDirection: {signal['signal']}\nTP: {tp:.2f} | SL: {sl:.2f}")
                        STATS["signals_sent"] += 1; STATS["crypto_signals"] += 1
            except Exception as e: logging.error(e)
                
                
def perform_forex_analysis():
    # Heartbeat
    if datetime.utcnow().minute == 0:
        send_to_private("🤖 Forex Scanner: Healthy and active...")
        
    if not is_market_active(): return
    # ... rest of your existing code

def perform_forex_analysis():
    if not is_market_active(): return
    for symbol in FOREX_PAIRS:
        for tf_label, tf_interval in FOREX_TIMEFRAMES.items():
            try:
                candles = get_twelvedata_candles(symbol, tf_interval, limit=100)
                if not candles: continue
                closes = [c['close'] for c in candles]
                # Pass the 4H trend
                trend_4h = get_trend_direction(get_twelvedata_candles(symbol, "4h", limit=200))
                signal = analyze_candles(candles, trend_4h=trend_4h)
                
                if signal and signal['signal']:
                    if should_send_signal(symbol, tf_label, signal):
                        entry = closes[-1]
                        if signal['signal'] == "BUY":
                            tp, sl = entry * 1.005, entry * 0.995
                        else:
                            tp, sl = entry * 0.995, entry * 1.005
                            
                        ACTIVE_TRADES.append({'symbol': symbol, 'entry': entry, 'tp': tp, 'sl': sl})
                        send_to_channel(f"🚀 <b>Forex: {symbol}</b> ({tf_label})\nDirection: {signal['signal']}\nTP: {tp:.4f} | SL: {sl:.4f}")
                        STATS["signals_sent"] += 1; STATS["forex_signals"] += 1
            except Exception as e: logging.error(e)

# --- ROUTES ---
@app.route("/analyze/crypto", methods=["GET"])
def analyze_crypto():
    threading.Thread(target=perform_crypto_analysis).start()
    return jsonify({"status": "accepted"}), 202

@app.route("/analyze/forex", methods=["GET"])
def analyze_forex():
    threading.Thread(target=perform_forex_analysis).start()
    return jsonify({"status": "accepted"}), 202

@app.route("/daily-summary", methods=["GET"])
def daily_summary():
    msg = f"📊 <b>Performance Report</b>\nSignals: {STATS['signals_sent']}\nWins: {STATS['wins']}\nLosses: {STATS['losses']}"
    send_to_private(msg)
    return jsonify({"status": "ok", "stats": STATS})

# --- DATA HELPERS ---
def get_binance_candles(symbol, interval, limit=100):
    resp = requests.get(f"https://data-api.binance.vision/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}", timeout=15)
    return [{"close": float(c[4])} for c in resp.json()]

def get_twelvedata_candles(symbol, interval, limit=100):
    resp = requests.get(f"https://api.twelvedata.com/time_series?symbol={symbol}&interval={interval}&outputsize={limit}&apikey={TWELVE_DATA_API_KEY}", timeout=15)
    data = resp.json()
    return [{"close": float(c["close"])} for c in reversed(data.get("values", []))]

def get_trend_direction(candles):
    return "UP" if candles[-1]['close'] > candles[0]['close'] else "DOWN"

if __name__ == "__main__":
    app.run()
