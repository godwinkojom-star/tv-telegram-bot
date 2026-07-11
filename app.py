"""
==========================================================
 SmartFX Signal Bot
 Main Application (app.py) - Version 2.1.0
==========================================================

Reads live market data (Kraken for crypto, TwelveData for forex),
detects 4H trend, finds 1H/15M SMC entries using smc_analysis.py
(with multi-timeframe confirmation: 15M only fires if 1H agrees),
sends signals to a public Telegram channel, and sends bot health /
statistics / morning & evening / weekly reports to a private
Telegram chat. Tracks trade outcomes and per-pair performance.

Deployment note: run with a SINGLE worker (e.g. `gunicorn -w 1 app:app`)
since background threads and in-memory state are not shared across
multiple worker processes.

All state (active trades, statistics, last signals) is kept in memory.
If the process restarts, that state is lost. Add a database if you need
it to survive restarts/redeploys.
"""

import os
import time
import logging
import threading
from datetime import datetime

from flask import Flask, jsonify
import requests

import smc_analysis


# ==========================================================
# VERSION
# ==========================================================
# Bump this every time a real batch of changes ships - makes it easy
# to know which version is running on Render and match it against
# what's in a given YouTube update.

VERSION = "2.1.0"
BOT_NAME = "SmartFX Signal Bot"
PUBLIC_MODE = True


# ==========================================================
# CONFIG
# ==========================================================

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_PUBLIC_CHANNEL_ID = os.environ.get("TELEGRAM_PUBLIC_CHANNEL_ID")
# Falls back to your personal Telegram user ID if the env var isn't set,
# so bot health / stats / daily summaries always reach you privately
# and never end up in the public channel.
TELEGRAM_PRIVATE_USER_ID = os.environ.get("TELEGRAM_PRIVATE_USER_ID", "8662582348")
TWELVEDATA_API_KEY = os.environ.get("TWELVEDATA_API_KEY")

TELEGRAM_API_URL = "https://api.telegram.org/bot{token}/sendMessage"

REQUEST_TIMEOUT = 10

CRYPTO_PAIRS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
FOREX_PAIRS = ["EUR/USD", "GBP/USD", "XAU/USD", "USD/JPY"]

# Kraken uses different symbols than Binance. We keep the friendly names
# above (for messages/stats/dedupe keys) and map them to Kraken's symbols
# only when calling Kraken's API.
CRYPTO_SYMBOL_MAP = {
    "BTCUSDT": "XBTUSD",
    "ETHUSDT": "ETHUSD",
    "SOLUSDT": "SOLUSD",
}

TREND_TIMEFRAME = "4h"
ENTRY_TIMEFRAMES = ["1h", "15m"]

# Kraken OHLC "interval" is in minutes.
KRAKEN_INTERVAL_MAP = {"4h": 240, "1h": 60, "15m": 15}
TWELVEDATA_INTERVAL_MAP = {"4h": "4h", "1h": "1h", "15m": "15min"}

ANALYSIS_LOOP_SECONDS = 60       # full scan every 1 minute (was 5 minutes)
TRADE_MONITOR_SECONDS = 20       # check active trades every 20 seconds (was 1 minute)
SUMMARY_CHECK_SECONDS = 30       # check clock every 30 seconds
TREND_CACHE_SECONDS = 3600       # reuse the 4H trend for 1 hour instead of refetching every scan
PAIR_COOLDOWN_SECONDS = 1800     # minimum gap between ANY two signals for the same pair (30 minutes)
SIGNAL_EXPIRY_SECONDS = 4 * 3600 # a trade that hits neither TP1 nor SL within 4 hours is auto-expired

# TwelveData's free plan allows only 8 API calls/minute AND 800/day.
# These settings are sized so forex stays comfortably under 800/day
# even with active trades open all day:
#   - Scanning every 20 min, 4 pairs, 2 entry timeframes = ~576 calls/day
#   - Trend refreshed hourly (shared cache above) = ~96 calls/day
#   - Trade monitoring reuses the price seen during scanning (see
#     analyze_pair) instead of making its own separate calls, so it
#     adds close to zero extra usage.
#   Total: ~670-700 calls/day, leaving headroom for retries.
FOREX_SCAN_INTERVAL_SECONDS = 1200  # only actually scan forex every 20 minutes
FOREX_PAIR_DELAY_SECONDS = 3        # small delay between each forex pair within a scan, so calls aren't bursty
FOREX_INNER_CALL_DELAY_SECONDS = 2  # small delay between trend/1H/15M calls within the SAME pair, so a fresh-cache startup burst can't briefly exceed 8/min
FOREX_PRICE_CACHE_SECONDS = 1200    # matches the scan interval, so monitoring piggybacks on scan data

CANDLE_LIMIT = 300

RISK_DISCLAIMER = "⚠️ Risk only 1-2% of your account on any single trade. Trade with discipline."


# ==========================================================
# LOGGING
# ==========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("SmartFX")


# ==========================================================
# IN-MEMORY STATE
# ==========================================================

last_signals = {}      # key: "PAIR_TF" -> "BUY" | "SELL"
last_pair_signal_time = {}   # key: pair -> unix timestamp of last signal sent (any timeframe)
active_trades = {}     # key: trade_id -> trade dict
pair_stats = {}        # key: pair -> {"signals": int, "wins": int, "losses": int}
trend_cache = {}       # key: "PAIR_MARKETTYPE" -> {"trend": str, "fetched_at": float}
forex_price_cache = {} # key: pair -> {"price": float, "fetched_at": float}
last_forex_scan_time = 0.0  # unix timestamp of the last time forex pairs were actually scanned
signal_id_counter = 0  # incrementing unique ID given to every signal sent
loop_heartbeats = {}   # key: loop name -> unix timestamp it last completed an iteration

# Stats that reset every day at 00:00 UTC - used for the morning/evening
# reports so "Signals Today" actually means today, not all-time.
daily_stats = {
    "signals": 0,
    "buy_count": 0,
    "sell_count": 0,
    "wins": 0,
    "losses": 0,
    "crypto_signals": 0,
    "forex_signals": 0,
    "timeframe_counts": {"15m": 0, "1h": 0},
}

global_stats = {
    "signals_sent": 0,
    "crypto_signals": 0,
    "forex_signals": 0,
    "wins": 0,
    "losses": 0,
    "errors": 0,
}

state_lock = threading.Lock()

_threads_started = False


# ==========================================================
# STARTUP CHECKS
# ==========================================================

def check_env():
    missing = []
    for name, value in [
        ("TELEGRAM_BOT_TOKEN", TELEGRAM_BOT_TOKEN),
        ("TELEGRAM_PUBLIC_CHANNEL_ID", TELEGRAM_PUBLIC_CHANNEL_ID),
        ("TELEGRAM_PRIVATE_USER_ID", TELEGRAM_PRIVATE_USER_ID),
        ("TWELVEDATA_API_KEY", TWELVEDATA_API_KEY),
    ]:
        if not value:
            missing.append(name)

    if missing:
        logger.warning(
            "Missing environment variables: %s. Related features will fail until they are set.",
            ", ".join(missing),
        )


# ==========================================================
# TELEGRAM
# ==========================================================

def send_telegram_message(chat_id, text):
    if not TELEGRAM_BOT_TOKEN or not chat_id:
        log_error("Telegram send skipped: missing bot token or chat id.")
        return False

    url = TELEGRAM_API_URL.format(token=TELEGRAM_BOT_TOKEN)
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown",
    }

    last_exception = None

    for attempt in range(2):  # 1 retry
        try:
            resp = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)
            if resp.status_code != 200:
                if attempt == 0:
                    time.sleep(2)
                    continue
                log_error(f"Telegram error {resp.status_code}: {resp.text}")
                return False
            return True

        except Exception as e:
            last_exception = e
            if attempt == 0:
                time.sleep(2)

    log_error(f"Telegram send failed after retry: {last_exception}")
    return False


def send_public_signal(text):
    return send_telegram_message(TELEGRAM_PUBLIC_CHANNEL_ID, text)


def send_private_message(text):
    return send_telegram_message(TELEGRAM_PRIVATE_USER_ID, text)


# ==========================================================
# LOGGING HELPERS
# ==========================================================

def log_error(msg):
    logger.error(msg)
    with state_lock:
        global_stats["errors"] += 1


def log_info(msg):
    logger.info(msg)


def update_heartbeat(name):
    with state_lock:
        loop_heartbeats[name] = time.time()


def request_with_retry(method, url, retries=1, backoff_seconds=2, timeout=REQUEST_TIMEOUT, **kwargs):
    """
    Makes an HTTP request and retries once (by default) before giving up.
    This stops a single slow/blip API response from immediately counting
    as a logged error - only a failure that persists through the retry
    gets logged.
    """
    last_exception = None

    for attempt in range(retries + 1):
        try:
            resp = requests.request(method, url, timeout=timeout, **kwargs)
            resp.raise_for_status()
            return resp
        except Exception as e:
            last_exception = e
            if attempt < retries:
                time.sleep(backoff_seconds)

    raise last_exception


# ==========================================================
# MARKET DATA - KRAKEN (CRYPTO)
# ==========================================================
# Kraken's public endpoints need no API key and aren't blocked on
# cloud hosts the way Binance is.

def fetch_kraken_ohlc(symbol, interval_minutes):
    url = "https://api.kraken.com/0/public/OHLC"
    params = {"pair": symbol, "interval": interval_minutes}

    resp = request_with_retry("GET", url, params=params)
    data = resp.json()

    if data.get("error"):
        raise ValueError(f"Kraken error for {symbol}: {data['error']}")

    result = data["result"]
    pair_key = next(k for k in result.keys() if k != "last")
    rows = result[pair_key]

    candles = []
    for row in rows:
        candles.append({
            "time": row[0],
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
        })

    return candles


def fetch_kraken_price(symbol):
    url = "https://api.kraken.com/0/public/Ticker"
    params = {"pair": symbol}

    resp = request_with_retry("GET", url, params=params)
    data = resp.json()

    if data.get("error"):
        raise ValueError(f"Kraken ticker error for {symbol}: {data['error']}")

    result = data["result"]
    pair_key = next(iter(result.keys()))
    return float(result[pair_key]["c"][0])


# ==========================================================
# MARKET DATA - TWELVEDATA (FOREX)
# ==========================================================

def fetch_twelvedata_candles(symbol, interval, outputsize=CANDLE_LIMIT):
    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": symbol,
        "interval": interval,
        "outputsize": outputsize,
        "apikey": TWELVEDATA_API_KEY,
    }

    resp = request_with_retry("GET", url, params=params, timeout=REQUEST_TIMEOUT + 5)
    data = resp.json()

    if "values" not in data:
        raise ValueError(f"TwelveData error for {symbol}: {data}")

    values = list(reversed(data["values"]))  # oldest -> newest

    candles = []
    for v in values:
        candles.append({
            "time": v["datetime"],
            "open": float(v["open"]),
            "high": float(v["high"]),
            "low": float(v["low"]),
            "close": float(v["close"]),
        })

    return candles


def fetch_twelvedata_price(symbol):
    url = "https://api.twelvedata.com/price"
    params = {"symbol": symbol, "apikey": TWELVEDATA_API_KEY}
    resp = request_with_retry("GET", url, params=params)
    data = resp.json()
    return float(data["price"])


# ==========================================================
# MARKET DATA DISPATCH
# ==========================================================

def get_candles(pair, timeframe, market_type):
    if market_type == "crypto":
        kraken_symbol = CRYPTO_SYMBOL_MAP[pair]
        interval = KRAKEN_INTERVAL_MAP[timeframe]
        return fetch_kraken_ohlc(kraken_symbol, interval)
    else:
        interval = TWELVEDATA_INTERVAL_MAP[timeframe]
        return fetch_twelvedata_candles(pair, interval, CANDLE_LIMIT)


def get_current_price(pair, market_type):
    if market_type == "crypto":
        return fetch_kraken_price(CRYPTO_SYMBOL_MAP[pair])

    now = time.time()
    with state_lock:
        cached = forex_price_cache.get(pair)

    if cached and (now - cached["fetched_at"] < FOREX_PRICE_CACHE_SECONDS):
        return cached["price"]

    price = fetch_twelvedata_price(pair)

    with state_lock:
        forex_price_cache[pair] = {"price": price, "fetched_at": now}

    return price


def is_forex_open():
    now = datetime.utcnow()
    weekday = now.weekday()  # Monday=0 ... Sunday=6

    if weekday == 5:
        return False  # Saturday: closed all day

    if weekday == 6:
        return now.hour >= 22  # Sunday: opens ~22:00 UTC

    if weekday == 4 and now.hour >= 22:
        return False  # Friday: closes ~22:00 UTC

    return True


# ==========================================================
# SIGNAL ID
# ==========================================================

def get_next_signal_id():
    global signal_id_counter
    with state_lock:
        signal_id_counter += 1
        return f"{signal_id_counter:04d}"


# ==========================================================
# SIGNAL MESSAGE FORMATTING
# ==========================================================

def format_signal_message(pair, timeframe, result, trend, signal_id, market_type):
    direction_word = "BUY" if "BUY" in result["direction"] else "SELL"
    direction_emoji = "🟢" if direction_word == "BUY" else "🔴"
    trend_text = "Bullish" if trend == "BUY" else "Bearish"
    market_hashtag = "Crypto" if market_type == "crypto" else "Forex"
    pair_hashtag = pair.replace("/", "")

    return (
        f"🚀 *{BOT_NAME}*\n\n"
        f"{direction_emoji} {direction_word}\n\n"
        f"💹 {pair}\n"
        f"⏰ {timeframe.upper()}\n\n"
        f"💵 Entry: `{result['entry']}`\n\n"
        f"🎯 TP1: `{result['tp1']}`\n"
        f"🎯 TP2: `{result['tp2']}`\n"
        f"🎯 TP3: `{result['tp3']}`\n\n"
        f"🛑 Stop Loss: `{result['sl']}`\n\n"
        f"📉 Support: `{result['support']}`\n"
        f"📈 Resistance: `{result['resistance']}`\n\n"
        f"📊 Signal Strength: {result['confidence']}%\n"
        f"⚠️ Risk: {result['risk']}\n"
        f"📈 Trend: 4H {trend_text}\n\n"
        f"🆔 Signal #{signal_id}\n\n"
        f"{RISK_DISCLAIMER}\n\n"
        f"#{pair_hashtag} #{market_hashtag}"
    )


# ==========================================================
# DUPLICATE PROTECTION
# ==========================================================

def is_duplicate_signal(pair, timeframe, direction):
    key = f"{pair}_{timeframe}"
    with state_lock:
        return last_signals.get(key) == direction


def store_last_signal(pair, timeframe, direction):
    key = f"{pair}_{timeframe}"
    with state_lock:
        last_signals[key] = direction


def is_pair_in_cooldown(pair):
    now = time.time()
    with state_lock:
        last_time = last_pair_signal_time.get(pair)

    if last_time is None:
        return False

    return (now - last_time) < PAIR_COOLDOWN_SECONDS


def mark_pair_signal_time(pair):
    with state_lock:
        last_pair_signal_time[pair] = time.time()


# ==========================================================
# TRADE TRACKING
# ==========================================================

def has_active_trade_for_pair(pair):
    with state_lock:
        return any(t["pair"] == pair for t in active_trades.values())


def open_trade(pair, timeframe, market_type, direction, result, signal_id):
    trade_id = f"{pair}_{timeframe}_{int(time.time() * 1000)}"

    trade = {
        "pair": pair,
        "timeframe": timeframe,
        "market_type": market_type,
        "direction": direction,
        "entry": result["entry"],
        "sl": result["sl"],
        "tp1": result["tp1"],
        "tp2": result["tp2"],
        "tp3": result["tp3"],
        "opened_at": datetime.utcnow().isoformat(),
        "signal_id": signal_id,
    }

    with state_lock:
        active_trades[trade_id] = trade

    return trade_id


def close_trade(trade_id, trade, outcome):
    pair = trade["pair"]

    with state_lock:
        active_trades.pop(trade_id, None)

        stats = pair_stats.setdefault(pair, {"signals": 0, "wins": 0, "losses": 0})

        if outcome == "WIN":
            stats["wins"] += 1
            global_stats["wins"] += 1
            daily_stats["wins"] += 1
        else:
            stats["losses"] += 1
            global_stats["losses"] += 1
            daily_stats["losses"] += 1

    log_info(f"Trade closed: {pair} {trade['direction']} ({trade['timeframe']}) -> {outcome}")

    signal_id = trade.get("signal_id", "N/A")
    result_emoji = "🏆" if outcome == "WIN" else "❌"
    hit_text = "Hit TP1" if outcome == "WIN" else "Hit Stop Loss"

    send_private_message(
        f"{result_emoji} *Signal #{signal_id} Result: {outcome}*\n\n"
        f"{pair} | {trade['timeframe']} | {trade['direction']}\n"
        f"Entry: `{trade['entry']}`\n"
        f"{hit_text}"
    )


def expire_trade(trade_id, trade):
    with state_lock:
        active_trades.pop(trade_id, None)

    log_info(
        f"Trade expired (no TP1/SL hit within "
        f"{SIGNAL_EXPIRY_SECONDS / 3600:.0f}h): "
        f"{trade['pair']} {trade['direction']} ({trade['timeframe']})"
    )

    signal_id = trade.get("signal_id", "N/A")
    hours = SIGNAL_EXPIRY_SECONDS / 3600

    send_private_message(
        f"⏳ *Signal #{signal_id} Expired*\n\n"
        f"{trade['pair']} | {trade['timeframe']} | {trade['direction']}\n"
        f"No TP1 or Stop Loss hit within {hours:.0f}h - closed without a result."
    )


def monitor_trades():
    with state_lock:
        trades_snapshot = list(active_trades.items())

    for trade_id, trade in trades_snapshot:
        try:
            price = get_current_price(trade["pair"], trade["market_type"])
        except Exception as e:
            log_error(f"Price fetch failed while monitoring {trade['pair']}: {e}")
            continue

        outcome = None

        # TP1 hit counts as a WIN. SL hit counts as a LOSS.
        if trade["direction"] == "BUY":
            if price >= trade["tp1"]:
                outcome = "WIN"
            elif price <= trade["sl"]:
                outcome = "LOSS"
        else:
            if price <= trade["tp1"]:
                outcome = "WIN"
            elif price >= trade["sl"]:
                outcome = "LOSS"

        if outcome:
            close_trade(trade_id, trade, outcome)
            continue

        opened_at = datetime.fromisoformat(trade["opened_at"])
        age_seconds = (datetime.utcnow() - opened_at).total_seconds()

        if age_seconds > SIGNAL_EXPIRY_SECONDS:
            expire_trade(trade_id, trade)


# ==========================================================
# ANALYSIS PIPELINE
# ==========================================================

def get_cached_trend(pair, market_type):
    cache_key = f"{pair}_{market_type}"
    now = time.time()

    with state_lock:
        cached = trend_cache.get(cache_key)

    if cached and (now - cached["fetched_at"] < TREND_CACHE_SECONDS):
        return cached["trend"]

    try:
        trend_candles = get_candles(pair, TREND_TIMEFRAME, market_type)
    except Exception as e:
        log_error(f"Failed to fetch {TREND_TIMEFRAME} candles for {pair}: {e}")
        # If we have a stale cached trend, better to reuse it than to
        # skip the pair entirely because of one failed request.
        return cached["trend"] if cached else None

    trend = smc_analysis.get_trend_direction(trend_candles)

    with state_lock:
        trend_cache[cache_key] = {"trend": trend, "fetched_at": now}

    if market_type == "forex":
        time.sleep(FOREX_INNER_CALL_DELAY_SECONDS)

    return trend


def analyze_pair(pair, market_type):
    trend = get_cached_trend(pair, market_type)
    if trend is None:
        return

    timeframe_results = {}

    for timeframe in ENTRY_TIMEFRAMES:
        if market_type == "forex" and timeframe_results:
            time.sleep(FOREX_INNER_CALL_DELAY_SECONDS)

        try:
            entry_candles = get_candles(pair, timeframe, market_type)
        except Exception as e:
            log_error(f"Failed to fetch {timeframe} candles for {pair}: {e}")
            continue

        # For forex, reuse this candle's latest close as the "current
        # price" for trade monitoring, instead of making a separate
        # dedicated price call - this is what keeps forex well under
        # TwelveData's 800 calls/day free-plan limit.
        if market_type == "forex" and entry_candles:
            with state_lock:
                forex_price_cache[pair] = {
                    "price": entry_candles[-1]["close"],
                    "fetched_at": time.time(),
                }

        try:
            result = smc_analysis.analyze_candles(entry_candles, trend_4h=trend)
        except Exception as e:
            log_error(f"Analysis error for {pair} ({timeframe}): {e}")
            continue

        timeframe_results[timeframe] = result

        if result is None:
            continue

        direction = "BUY" if "BUY" in result["direction"] else "SELL"

        # Multi-timeframe confirmation: the 15M entry must agree with
        # the 1H entry from this same scan. If 1H found no valid setup,
        # or disagrees on direction, skip the 15M signal - this stops
        # 15M from firing against the higher timeframe.
        if timeframe == "15m" and "1h" in ENTRY_TIMEFRAMES:
            higher_tf_result = timeframe_results.get("1h")

            if higher_tf_result is None:
                log_info(f"Skipping 15M signal for {pair}: no confirming 1H setup this scan.")
                continue

            higher_tf_direction = "BUY" if "BUY" in higher_tf_result["direction"] else "SELL"

            if higher_tf_direction != direction:
                log_info(
                    f"Skipping 15M signal for {pair}: 1H direction disagrees "
                    f"({higher_tf_direction} vs {direction})."
                )
                continue

        if is_duplicate_signal(pair, timeframe, direction):
            continue

        if is_pair_in_cooldown(pair):
            log_info(f"Skipping signal for {pair} ({timeframe}): pair cooldown active.")
            continue

        if has_active_trade_for_pair(pair):
            log_info(f"Skipping signal for {pair} ({timeframe}): pair already has an active trade.")
            continue

        store_last_signal(pair, timeframe, direction)

        signal_id = get_next_signal_id()
        message = format_signal_message(pair, timeframe, result, trend, signal_id, market_type)
        sent = send_public_signal(message)

        if sent:
            with state_lock:
                global_stats["signals_sent"] += 1
                daily_stats["signals"] += 1
                daily_stats["timeframe_counts"][timeframe] = daily_stats["timeframe_counts"].get(timeframe, 0) + 1

                if direction == "BUY":
                    daily_stats["buy_count"] += 1
                else:
                    daily_stats["sell_count"] += 1

                if market_type == "crypto":
                    global_stats["crypto_signals"] += 1
                    daily_stats["crypto_signals"] += 1
                else:
                    global_stats["forex_signals"] += 1
                    daily_stats["forex_signals"] += 1

                pair_stat = pair_stats.setdefault(pair, {"signals": 0, "wins": 0, "losses": 0})
                pair_stat["signals"] += 1

            log_info(f"Signal #{signal_id} sent: {pair} {direction} ({timeframe}) confidence={result['confidence']}")
            mark_pair_signal_time(pair)
            open_trade(pair, timeframe, market_type, direction, result, signal_id)


def safe_analyze(pair, market_type):
    try:
        analyze_pair(pair, market_type)
    except Exception as e:
        log_error(f"Unhandled error analyzing {pair}: {e}")


def run_crypto_analysis():
    threads = []
    for pair in CRYPTO_PAIRS:
        t = threading.Thread(target=safe_analyze, args=(pair, "crypto"))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()


def run_forex_analysis():
    global last_forex_scan_time

    if not is_forex_open():
        log_info("Forex market closed, skipping forex scan.")
        return

    now = time.time()
    with state_lock:
        elapsed = now - last_forex_scan_time

    if elapsed < FOREX_SCAN_INTERVAL_SECONDS:
        return  # not time yet - keeps us under TwelveData's 8 requests/minute free-tier limit

    with state_lock:
        last_forex_scan_time = now

    # Sequential with a small delay between pairs, instead of parallel
    # threads, so all 4 pairs don't hit TwelveData at the exact same
    # instant.
    for pair in FOREX_PAIRS:
        safe_analyze(pair, "forex")
        time.sleep(FOREX_PAIR_DELAY_SECONDS)


# ==========================================================
# PAIR PERFORMANCE / STATISTICS
# ==========================================================

def get_best_worst_pairs():
    best_pair = "N/A"
    worst_pair = "N/A"
    best_rate = -1
    worst_rate = 101

    with state_lock:
        snapshot = dict(pair_stats)

    for pair, stats in snapshot.items():
        total = stats["wins"] + stats["losses"]
        if total == 0:
            continue

        rate = (stats["wins"] / total) * 100

        if rate > best_rate:
            best_rate = rate
            best_pair = f"{pair} ({rate:.1f}%)"

        if rate < worst_rate:
            worst_rate = rate
            worst_pair = f"{pair} ({rate:.1f}%)"

    return best_pair, worst_pair


def get_most_active_timeframe():
    with state_lock:
        counts = dict(daily_stats["timeframe_counts"])

    if not counts or max(counts.values()) == 0:
        return "N/A"

    return max(counts, key=counts.get).upper()


def reset_daily_stats():
    with state_lock:
        daily_stats["signals"] = 0
        daily_stats["buy_count"] = 0
        daily_stats["sell_count"] = 0
        daily_stats["wins"] = 0
        daily_stats["losses"] = 0
        daily_stats["crypto_signals"] = 0
        daily_stats["forex_signals"] = 0
        daily_stats["timeframe_counts"] = {"15m": 0, "1h": 0}

    log_info("Daily stats reset for the new day.")


def build_morning_report():
    with state_lock:
        stats = dict(daily_stats)
        active_count = len(active_trades)

    return (
        f"🤖 *{BOT_NAME} Morning Report*\n\n"
        "✅ Bot Status: ONLINE\n\n"
        f"📅 Date: {datetime.utcnow().strftime('%d %B %Y')}\n\n"
        f"📈 Crypto Pairs: {len(CRYPTO_PAIRS)}\n"
        f"🌱 Forex Pairs: {len(FOREX_PAIRS)}\n\n"
        f"📊 Signals Today: {stats['signals']}\n"
        f"🟢 BUY: {stats['buy_count']}\n"
        f"🔴 SELL: {stats['sell_count']}\n\n"
        f"🏆 Wins: {stats['wins']}\n"
        f"❌ Losses: {stats['losses']}\n\n"
        f"🔄 Active Trades: {active_count}\n\n"
        "Bot is healthy and scanning the markets..."
    )


def build_evening_report():
    with state_lock:
        stats = dict(daily_stats)
        active_count = len(active_trades)

    total_trades = stats["wins"] + stats["losses"]
    win_rate = round((stats["wins"] / total_trades) * 100, 1) if total_trades else 0

    best_pair, _ = get_best_worst_pairs()
    most_active_tf = get_most_active_timeframe()

    return (
        f"🌙 *{BOT_NAME} Evening Report*\n\n"
        f"📊 Signals Generated: {stats['signals']}\n\n"
        f"Crypto Signals: {stats['crypto_signals']}\n"
        f"Forex Signals: {stats['forex_signals']}\n\n"
        f"Wins: {stats['wins']}\n"
        f"Losses: {stats['losses']}\n"
        f"🔄 Still Running (Active): {active_count}\n"
        f"Win Rate: {win_rate}%\n\n"
        f"Best Pair: {best_pair}\n"
        f"Most Active Timeframe: {most_active_tf}\n\n"
        "✅ Bot Status: Running Normally\n\n"
        "See you tomorrow."
    )


def send_morning_report():
    sent = send_private_message(build_morning_report())
    if sent:
        log_info("Morning report sent to private chat.")
    return sent


def send_evening_report():
    sent = send_private_message(build_evening_report())
    if sent:
        log_info("Evening report sent to private chat.")
    return sent


def build_weekly_pair_breakdown():
    with state_lock:
        snapshot = dict(pair_stats)

    all_pairs = CRYPTO_PAIRS + FOREX_PAIRS
    lines = [f"📊 *{BOT_NAME} - Weekly Pair Performance*\n"]

    for pair in all_pairs:
        stats = snapshot.get(pair, {"signals": 0, "wins": 0, "losses": 0})
        total = stats["wins"] + stats["losses"]
        win_rate = round((stats["wins"] / total) * 100, 1) if total else 0

        lines.append(
            f"\n*{pair}*\n"
            f"Signals: {stats.get('signals', 0)}\n"
            f"Wins: {stats['wins']}\n"
            f"Losses: {stats['losses']}\n"
            f"Win Rate: {win_rate}%"
        )

    return "\n".join(lines)


def send_weekly_summary():
    sent = send_private_message(build_weekly_pair_breakdown())
    if sent:
        log_info("Weekly summary sent to private chat.")
    return sent


# ==========================================================
# BACKGROUND LOOPS
# ==========================================================

def analysis_loop():
    while True:
        try:
            run_crypto_analysis()
            run_forex_analysis()
        except Exception as e:
            log_error(f"Analysis loop error: {e}")

        update_heartbeat("analysis")
        time.sleep(ANALYSIS_LOOP_SECONDS)


def trade_monitor_loop():
    while True:
        try:
            monitor_trades()
        except Exception as e:
            log_error(f"Trade monitor loop error: {e}")

        update_heartbeat("trade_monitor")
        time.sleep(TRADE_MONITOR_SECONDS)


def daily_summary_loop():
    sent_morning_on = None
    sent_evening_on = None
    sent_weekly_on = None
    reset_on = None

    while True:
        try:
            now = datetime.utcnow()
            today = now.date()

            # Reset daily stats once per day at midnight UTC, so the
            # morning/evening reports reflect today only.
            if now.hour == 0 and now.minute < 2 and reset_on != today:
                reset_daily_stats()
                reset_on = today

            # Times are in UTC. Adjust the hour checks below if you want
            # 8:00 / 20:00 in a different timezone. The "minute < 2" window
            # (instead of an exact minute == 0) gives a small buffer in case
            # the loop check lands a little late (e.g. after a cold start).
            if now.hour == 8 and now.minute < 2 and sent_morning_on != today:
                send_morning_report()
                sent_morning_on = today

            if now.hour == 20 and now.minute < 2 and sent_evening_on != today:
                send_evening_report()
                sent_evening_on = today

            # Weekly summary: every Sunday at 21:00 UTC (once per week).
            if now.weekday() == 6 and now.hour == 21 and now.minute < 2 and sent_weekly_on != today:
                send_weekly_summary()
                sent_weekly_on = today

        except Exception as e:
            log_error(f"Daily summary loop error: {e}")

        update_heartbeat("daily_summary")
        time.sleep(SUMMARY_CHECK_SECONDS)


def watchdog_loop():
    """
    Checks that all three background loops are still alive and ticking.
    If one goes quiet for far longer than its normal interval, it means
    that loop has stalled - this sends a private alert so it doesn't
    fail silently for hours/days without you knowing.
    """
    expected_intervals = {
        "analysis": ANALYSIS_LOOP_SECONDS,
        "trade_monitor": TRADE_MONITOR_SECONDS,
        "daily_summary": SUMMARY_CHECK_SECONDS,
    }

    already_alerted = set()

    while True:
        time.sleep(300)  # check every 5 minutes

        now = time.time()
        with state_lock:
            snapshot = dict(loop_heartbeats)

        for name, interval in expected_intervals.items():
            last = snapshot.get(name)

            if last is None:
                continue

            stalled = (now - last) > interval * 5

            if stalled and name not in already_alerted:
                minutes = int((now - last) / 60)
                log_error(f"Watchdog: {name} loop appears stuck (no heartbeat in {minutes}m).")
                send_private_message(
                    f"⚠️ Warning: the {name} loop hasn't updated in {minutes} minutes. "
                    "The bot may need a manual restart."
                )
                already_alerted.add(name)

            elif not stalled and name in already_alerted:
                already_alerted.discard(name)


def start_background_threads():
    global _threads_started

    if _threads_started:
        return

    _threads_started = True

    check_env()

    threading.Thread(target=analysis_loop, daemon=True).start()
    threading.Thread(target=trade_monitor_loop, daemon=True).start()
    threading.Thread(target=daily_summary_loop, daemon=True).start()
    threading.Thread(target=watchdog_loop, daemon=True).start()

    log_info(f"Background threads started: analysis, trade monitor, daily summary, watchdog. ({BOT_NAME} v{VERSION})")

    send_private_message(f"🤖 {BOT_NAME} v{VERSION} started and running.")


# ==========================================================
# FLASK APP
# ==========================================================

app = Flask(__name__)


@app.route("/")
def index():
    return f"{BOT_NAME} v{VERSION} is running."


@app.route("/analyze/crypto")
def analyze_crypto_route():
    threading.Thread(target=run_crypto_analysis, daemon=True).start()
    return jsonify({"status": "crypto analysis triggered"})


@app.route("/analyze/forex")
def analyze_forex_route():
    threading.Thread(target=run_forex_analysis, daemon=True).start()
    return jsonify({"status": "forex analysis triggered"})


@app.route("/morning-report")
def morning_report_route():
    sent = send_morning_report()
    return jsonify({"status": "sent" if sent else "failed"})


@app.route("/evening-report")
def evening_report_route():
    sent = send_evening_report()
    return jsonify({"status": "sent" if sent else "failed"})


@app.route("/weekly-summary")
def weekly_summary_route():
    sent = send_weekly_summary()
    return jsonify({"status": "sent" if sent else "failed"})


@app.route("/health")
def health_route():
    with state_lock:
        stats = dict(global_stats)
        active_count = len(active_trades)

    return jsonify({
        "status": "running",
        "version": VERSION,
        "bot_name": BOT_NAME,
        "active_trades": active_count,
        "signals_sent": stats["signals_sent"],
        "crypto_signals": stats["crypto_signals"],
        "forex_signals": stats["forex_signals"],
        "wins": stats["wins"],
        "losses": stats["losses"],
        "errors": stats["errors"],
        "checked_at": datetime.utcnow().isoformat(),
    })


# ==========================================================
# ENTRY POINT
# ==========================================================

start_background_threads()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
