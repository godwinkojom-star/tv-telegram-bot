import os
import logging
import threading
from flask import Flask, jsonify
import requests
from datetime import datetime
from smc_analysis import analyze_candles, get_trend_direction

app = Flask(__name__)

# ---------------- CONFIG ----------------
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

# ---------------- HELPERS ----------------
def is_market_active():
    h = datetime.utcnow().hour
    return (7 <= h < 16) or (12 <= h < 21)


def send_to_channel(text):
    requests.post(TELEGRAM_API_URL, json={
        "chat_id": PUBLIC_CHAT_ID,
        "text": text,
        "parse_mode": "HTML"
    }, timeout=10)


def send_to_private(text):
    requests.post(TELEGRAM_API_URL, json={
        "chat_id": PRIVATE_USER_ID,
        "text": text,
        "parse_mode": "HTML"
    }, timeout=10)


def should_send_signal(symbol, timeframe, signal):
    key = f"{symbol}_{timeframe}"
    if LAST_SIGNALS.get(key) == signal["direction"]:
        return False
    LAST_SIGNALS[key] = signal["direction"]
    return True


# ---------------- MARKET DATA ----------------
def get_binance_candles(symbol, interval, limit=100):
    try:
        url = f"https://data-api.binance.vision/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
        resp = requests.get(url, timeout=15).json()

        candles = []
        for c in resp:
            candles.append({
                "open": float(c[1]),
                "high": float(c[2]),
                "low": float(c[3]),
                "close": float(c[4])
            })
        return candles
    except Exception as e:
        logging.error(f"Binance error: {e}")
        return []


def get_twelvedata_candles(symbol, interval, limit=100):
    try:
        url = f"https://api.twelvedata.com/time_series?symbol={symbol}&interval={interval}&outputsize={limit}&apikey={TWELVE_DATA_API_KEY}"
        resp = requests.get(url, timeout=15).json()

        values = resp.get("values", [])
        candles = []

        for c in reversed(values):
            candles.append({
                "open": float(c["open"]),
                "high": float(c["high"]),
                "low": float(c["low"]),
                "close": float(c["close"])
            })

        return candles
    except Exception as e:
        logging.error(f"TwelveData error: {e}")
        return []


# ---------------- TRADE MONITOR ----------------
@app.route("/monitor-trades", methods=["GET"])
def monitor_trades():
    global ACTIVE_TRADES

    for trade in ACTIVE_TRADES[:]:
        symbol = trade["symbol"]
        current_price = get_live_price(symbol)

        if current_price == 0:
            continue

        if current_price >= trade["tp"] and trade["direction"] == "BUY":
            send_to_channel(f"✅ <b>TP Hit: {symbol}</b>")
            STATS["wins"] += 1
            ACTIVE_TRADES.remove(trade)

        elif current_price <= trade["tp"] and trade["direction"] == "SELL":
            send_to_channel(f"✅ <b>TP Hit: {symbol}</b>")
            STATS["wins"] += 1
            ACTIVE_TRADES.remove(trade)

        elif current_price <= trade["sl"] and trade["direction"] == "BUY":
            send_to_channel(f"❌ <b>SL Hit: {symbol}</b>")
            STATS["losses"] += 1
            ACTIVE_TRADES.remove(trade)

        elif current_price >= trade["sl"] and trade["direction"] == "SELL":
            send_to_channel(f"❌ <b>SL Hit: {symbol}</b>")
            STATS["losses"] += 1
            ACTIVE_TRADES.remove(trade)

    return jsonify({"active": len(ACTIVE_TRADES)})


def get_live_price(symbol):
    try:
        url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
        return float(requests.get(url, timeout=5).json()["price"])
    except:
        return 0


# ---------------- ANALYSIS CORE ----------------
def run_analysis(symbols, timeframes, market_type):
    if not is_market_active():
        return

    for symbol in symbols:
        for tf_label, tf in timeframes.items():
            try:
    candles = get_binance_candles(symbol, tf) if market_type == "crypto" else get_twelvedata_candles(symbol, tf)

    if len(candles) < 50:
        continue

    closes = [c["close"] for c in candles]

    trend_4h = get_trend_direction(candles)

    signal = analyze_candles(candles, trend_4h=trend_4h)

    print(symbol, tf_label, signal)  # DEBUG LINE

    if not signal:
        continue

    if should_send_signal(symbol, tf_label, signal):

        direction = signal["direction"]
        entry = signal["entry"]

        trade = {
            "symbol": symbol,
            "direction": "BUY" if "BUY" in direction else "SELL",
            "entry": entry,
            "tp": signal["tp3"],
            "sl": signal["sl"]
        }

        ACTIVE_TRADES.append(trade)

        send_to_channel(
            f"🚀 <b>{market_type.upper()}: {symbol}</b> ({tf_label})\n"
            f"{direction}\n"
            f"Entry: {entry}\n"
            f"TP: {signal['tp1']} | {signal['tp2']} | {signal['tp3']}\n"
            f"SL: {signal['sl']}\n"
            f"Confidence: {signal['confidence']}%\n"
            f"Risk: {signal['risk']}"
        )

        STATS["signals_sent"] += 1

        if market_type == "crypto":
            STATS["crypto_signals"] += 1
        else:
            STATS["forex_signals"] += 1

except Exception as e:
    logging.error(f"{symbol} error: {e}")


def perform_crypto_analysis():
    run_analysis(CRYPTO_PAIRS, CRYPTO_TIMEFRAMES, "crypto")


def perform_forex_analysis():
    run_analysis(FOREX_PAIRS, FOREX_TIMEFRAMES, "forex")


# ---------------- ROUTES ----------------
@app.route("/")
def home():
    return "Bot is active", 200


@app.route("/analyze/crypto")
def analyze_crypto():
    threading.Thread(target=perform_crypto_analysis).start()
    return jsonify({"status": "crypto running"})


@app.route("/analyze/forex")
def analyze_forex():
    threading.Thread(target=perform_forex_analysis).start()
    return jsonify({"status": "forex running"})


@app.route("/daily-summary")
def daily_summary():
    send_to_private(str(STATS))
    return jsonify(STATS)


if __name__ == "__main__":
    app.run(debug=True)
