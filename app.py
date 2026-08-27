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
import io
import json
import time
import logging
import threading
from datetime import datetime, timedelta

from flask import Flask, jsonify
import requests
from PIL import Image, ImageDraw, ImageFont
import psycopg
from dotenv import load_dotenv

load_dotenv()

try:
    from pywebpush import webpush, WebPushException
    PUSH_AVAILABLE = True
except ImportError:
    PUSH_AVAILABLE = False

import smc_analysis
import news_engine


# ==========================================================
# VERSION
# ==========================================================
# Bump this every time a real batch of changes ships - makes it easy
# to know which version is running on Render and match it against
# what's in a given YouTube update.

# 2.10.0 - Same-candle ambiguity resolution: when a forex 1H/15M candle
#          shows both SL and TP1 in range (GBP/USD #0043 case), the bot
#          now fetches 1-minute candles for that exact hour and checks
#          them in order to determine which was actually hit first,
#          instead of always defaulting to LOSS. Paired with
#          smc_analysis 1.1.0's entry-freshness gate (blocks chasing
#          entries that fire after the confirming move already ran).
#
# 2.11.0 - Forex-scan-stall detection: Signal #0058 (GBP/USD) revealed
#          that forex scanning could silently stop advancing for over
#          an hour with nothing logged at all, because crypto scanning
#          kept succeeding and masked it from the existing combined
#          "analysis" heartbeat. Added a forex-only heartbeat
#          (forex_scan_heartbeat) that only updates when a full pass
#          over FOREX_PAIRS actually completes, a watchdog check that
#          alerts your private Telegram if it goes stale for 50+
#          minutes while the forex market is open, and a
#          [FOREX-SCAN-GATE] diagnostic log line on every
#          run_forex_analysis call so a repeat is immediately
#          diagnosable instead of guessed at from an absence of logs.
# 2.12.0 - Fixed the delayed public-group broadcast: the public
#          channel's branded result card now goes out immediately when
#          TP1 hits, same as the private alerts message, instead of
#          waiting until the trade fully closes (TP2/TP3/breakeven/
#          expiry) which could be hours later (Signal #0060 case).
#          Updated cards are sent the same way when TP2 or TP3 hit
#          too, so the public card always reflects the best level
#          reached so far. The breakeven and expiry paths no longer
#          send their own card, since by the time either fires the
#          card for the actual highest level reached has already gone
#          out - sending another there would just be a duplicate.
# 2.13.0 - Push notifications: New Signal, Signal Result (WIN/LOSS/
#          EXPIRED), and Bot Status (general loop stall + forex-scan
#          stall, both directions) now send real browser push
#          notifications via VAPID/webpush, on top of the existing
#          Telegram alerts. A separate Supabase Edge Function +
#          pg_cron job (bot-watchdog, runs every 5 min) handles true
#          "Render itself is down" detection independently of this
#          process, since an in-process watchdog can't alert about its
#          own process being dead.
# 2.14.0 - Auto-trading: users can opt in (Settings → Auto-Trading,
#          off by default) with a chosen risk % of their real Portfolio
#          balance. Position size = (balance x risk%) / |entry - SL|,
#          computed per user from their own real balance at signal
#          time - always uses SmartFX's own Entry/SL/TP, never a
#          separate sizing strategy. On the same real closure points
#          the bot already uses for cards/DB status (LOSS, TP1-final,
#          TP2, TP3, breakeven-reversal, expiry), the real $ P/L is
#          applied straight to that user's portfolio.balance and
#          logged to portfolio_balance_log, so the dashboard's
#          Today/Week/Month P/L picks it up automatically with no
#          separate dashboard logic needed. One auto-trade per
#          user+signal enforced by a DB unique constraint.
# 2.14.1 - Bugfix: auto-trades were never actually closing. Postgres
#          returns the `numeric` position_size column as a Python
#          Decimal, while entry/sl/tp1/tp2/tp3 (double precision) come
#          back as float - multiplying Decimal x float raised a
#          TypeError that was silently swallowed by a broad except,
#          so every close attempt failed quietly and auto_trades rows
#          stayed stuck OPEN forever even after their signal resolved.
#          Fixed by casting position_size to float right after fetch.
# 2.15.0 - Forex auto-trading now sizes in lots (MT5-style) instead of
#          a raw balance*risk%/SL-distance quantity: lot size starts
#          from balance (round(balance/10000, 2), floored at 0.01),
#          then shrinks in 0.01 steps - or skips the trade entirely if
#          even 0.01 is too much - to keep the real dollar risk under
#          the existing Risk % setting for that signal's actual SL
#          distance. Risk % keeps its job as the safety ceiling, not
#          the primary sizing calculation. Crypto auto-trading is
#          unchanged (still balance*risk%/SL-distance quantity sizing -
#          "lots" aren't a crypto concept).
# 2.16.0 - News Engine V1 scaffolding: added news_engine.py, a fully
#          independent module (own DB connection, own logging, no
#          imports from app.py or smc_analysis.py) that runs as its
#          own background thread and writes a real heartbeat every 5
#          minutes to news_engine_state. No news provider is
#          configured yet - fetch_news_events() is a clear placeholder
#          returning nothing, by design, until a provider is chosen
#          and connected after the current 30-day strategy trial ends
#          (Sept 19). app.py's existing watchdog gained one small
#          read-only health check on that heartbeat, reusing the same
#          alert/recovery pattern as everything else - this and
#          starting the thread are the ONLY two places app.py touches
#          this module. Nothing here can affect a signal's confidence,
#          block a signal, or touch the strategy - signals.news_tag /
#          news_event_id / news_context columns exist but nothing
#          writes to them yet.
# 2.17.0 - Fixed trades getting permanently stuck OPEN forever after a
#          restart: active_trades (the in-memory dict trade_monitor_loop
#          uses to keep watching a trade for TP2/TP3/breakeven/expiry)
#          starts empty every process restart - a deploy, a crash, a
#          Wispbyte hiccup - with nothing to reload it. Any trade mid-
#          tracking at that exact moment (TP1 already hit, still
#          watching for more) was silently wiped from memory and never
#          checked again - it just sat "OPEN" forever in both the
#          dashboard and (for auto-trading users) the real Portfolio,
#          since close_auto_trades_for_signal never got called for it
#          either. Added resume_open_trades_from_db(), called once at
#          startup before the trade-monitor loop begins: rebuilds
#          active_trades from every signals row where closed_at IS
#          NULL, so trade_monitor_loop picks up exactly where the
#          previous process left off instead of losing anything.
VERSION = "2.17.0"
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
DATABASE_URL = os.environ.get("DATABASE_URL")

# Web push (browser notifications) - VAPID identifies this app to push
# services (Chrome/Firefox/etc). The private key must stay a secret env
# var; the public key is safe to also live in dashboard.html since
# browsers are meant to see it.
VAPID_PRIVATE_KEY = os.environ.get("VAPID_PRIVATE_KEY")
VAPID_PUBLIC_KEY = "BMJesWLDIi3yXX-s0cj5nM7VIlwBZvpyHwaJuxNTLfBrbQCAGiiw-krYs5Z4Vykov_a-p8-Hg3W4tA_Ao7vpjIo"
VAPID_CLAIMS_EMAIL = os.environ.get("VAPID_CLAIMS_EMAIL", "mailto:admin@smartfx.app")

TELEGRAM_API_URL = "https://api.telegram.org/bot{token}/sendMessage"
TELEGRAM_API_PHOTO_URL = "https://api.telegram.org/bot{token}/sendPhoto"

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

# Duration (in minutes) each entry-timeframe candle spans - used when
# resolving same-candle SL/TP1 ambiguity, to know how wide a window of
# 1-minute candles to check.
TIMEFRAME_MINUTES = {"1h": 60, "15m": 15}

ANALYSIS_LOOP_SECONDS = 60       # full scan every 1 minute (was 5 minutes)
TRADE_MONITOR_SECONDS = 20       # check active trades every 20 seconds (was 1 minute)
SUMMARY_CHECK_SECONDS = 30       # check clock every 30 seconds
TREND_CACHE_SECONDS = 3600       # reuse the 4H trend for 1 hour instead of refetching every scan
PAIR_COOLDOWN_SECONDS = 1800     # minimum gap between ANY two signals for the same pair (30 minutes)
SIGNAL_EXPIRY_SECONDS = {
    # Scaled up 50% alongside the SL/TP redesign (TP1 moved from 1.0x to
    # 1.5x ATR - a 50% farther target needs proportionally more time to
    # have a fair chance of being reached before we give up on it).
    "15m": 3 * 3600,   # was 2h
    "1h": 12 * 3600,   # was 8h
}

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

# How long forex_scan_heartbeat can go without updating (while the
# forex market is open) before the watchdog alerts - well above the
# normal ~20-minute cadence so a couple of slow/delayed cycles don't
# false-alarm, but well below "found out hours later by accident".
FOREX_SCAN_STALL_SECONDS = 50 * 60

# News Engine writes its own heartbeat every 5 minutes (see
# news_engine.py's NEWS_ENGINE_LOOP_SECONDS) - allow a few missed
# cycles before alerting, same margin-of-safety reasoning as the
# other stall thresholds above.
NEWS_ENGINE_STALL_SECONDS = 20 * 60
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
forex_candle_cache = {} # key: "PAIR_TIMEFRAME" -> list of recent candles (for high/low-based monitoring)
last_forex_scan_time = 0.0  # unix timestamp of the last time forex pairs were actually scanned
signal_id_counter = 0  # incrementing unique ID given to every signal sent
loop_heartbeats = {}   # key: loop name -> unix timestamp it last completed an iteration

# Separate from loop_heartbeats["analysis"]: that heartbeat updates once
# per analysis_loop iteration regardless of whether forex actually did
# anything that cycle, so a forex-only stall (run_forex_analysis
# silently doing nothing every cycle while crypto keeps succeeding)
# never shows up there - crypto's success masks it completely. This
# heartbeat only updates when run_forex_analysis actually finishes
# scanning all FOREX_PAIRS, so a real forex stall can be caught even
# while the general analysis loop looks perfectly healthy.
forex_scan_heartbeat = 0.0

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


def send_telegram_photo(chat_id, image_bytes, caption=None):
    if not TELEGRAM_BOT_TOKEN or not chat_id:
        log_error("Telegram photo send skipped: missing bot token or chat id.")
        return False

    url = TELEGRAM_API_PHOTO_URL.format(token=TELEGRAM_BOT_TOKEN)
    files = {"photo": ("result.png", image_bytes, "image/png")}
    data = {"chat_id": chat_id}

    if caption:
        data["caption"] = caption
        data["parse_mode"] = "Markdown"

    last_exception = None

    for attempt in range(2):  # 1 retry
        try:
            resp = requests.post(url, data=data, files=files, timeout=REQUEST_TIMEOUT)
            if resp.status_code != 200:
                if attempt == 0:
                    time.sleep(2)
                    continue
                log_error(f"Telegram photo error {resp.status_code}: {resp.text}")
                return False
            return True

        except Exception as e:
            last_exception = e
            if attempt == 0:
                time.sleep(2)

    log_error(f"Telegram photo send failed after retry: {last_exception}")
    return False


def generate_result_card(trade, outcome, final_tp_label=None):
    """
    Generates a simple branded image card for a finished trade - only
    called for genuinely final outcomes (a LOSS, or a full TP3 win).
    Uses PIL directly (no external image APIs, no extra network calls).
    """

    width, height = 800, 450
    bg_color = (10, 22, 16) if outcome == "WIN" else (26, 10, 10)
    accent_color = (0, 200, 120) if outcome == "WIN" else (220, 60, 60)

    img = Image.new("RGB", (width, height), bg_color)
    draw = ImageDraw.Draw(img)

    try:
        font_large = ImageFont.truetype(
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 46
        )
        font_medium = ImageFont.truetype(
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 32
        )
        font_small = ImageFont.truetype(
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 22
        )
    except Exception:
        # Falls back gracefully if that specific font isn't present on
        # the host - the card still renders, just with a default font.
        font_large = ImageFont.load_default()
        font_medium = ImageFont.load_default()
        font_small = ImageFont.load_default()

    draw.text((40, 30), BOT_NAME, font=font_small, fill=(170, 170, 170))

    title = "TRADE WON 🏆" if outcome == "WIN" else "STOP LOSS ❌"
    draw.text((40, 80), title, font=font_large, fill=accent_color)

    draw.text((40, 165), trade["pair"], font=font_medium, fill=(255, 255, 255))
    draw.text(
        (40, 215),
        f"{trade['direction']} | {trade['timeframe']}",
        font=font_small,
        fill=(200, 200, 200),
    )

    draw.text((40, 275), f"Entry: {trade['entry']}", font=font_small, fill=(200, 200, 200))

    if outcome == "WIN" and final_tp_label:
        draw.text(
            (40, 315),
            f"Final Target Reached: {final_tp_label}",
            font=font_small,
            fill=accent_color,
        )
    elif outcome == "LOSS":
        draw.text(
            (40, 315),
            f"Stop Loss: {trade['sl']}",
            font=font_small,
            fill=accent_color,
        )

    signal_id = trade.get("signal_id", "N/A")
    draw.text((40, height - 55), f"Signal #{signal_id}", font=font_small, fill=(140, 140, 140))

    buffer = io.BytesIO()
    img.save(buffer, format="PNG")
    buffer.seek(0)
    return buffer


def send_result_card(trade, outcome, final_tp_label=None):
    try:
        image_bytes = generate_result_card(trade, outcome, final_tp_label=final_tp_label)
    except Exception as e:
        log_error(f"Failed to generate result card for Signal #{trade.get('signal_id')}: {e}")
        return False

    caption = f"Signal #{trade.get('signal_id', 'N/A')} - {trade['pair']} {trade['direction']}"

    return send_telegram_photo(TELEGRAM_PUBLIC_CHANNEL_ID, image_bytes, caption=caption)


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


# ==========================================================
# DATABASE (permanent signal history for the dashboard app)
# ==========================================================
# This is a persistent, permanent log kept ALONGSIDE the existing
# in-memory state above - it doesn't replace any of it. The bot's
# real-time decisions (cooldowns, active-trade checks, etc.) still
# run entirely off the fast in-memory data, exactly as before. Every
# time something meaningful happens to a signal, we ALSO write a
# permanent record here, purely for the future dashboard app to read.
# If the database is ever unreachable, the bot itself must keep
# running normally - these calls are wrapped so a DB hiccup never
# breaks a signal from sending or a trade from being tracked.

def get_db_connection():
    if not DATABASE_URL:
        return None
    return psycopg.connect(DATABASE_URL, connect_timeout=10)


def init_db():
    if not DATABASE_URL:
        log_info("DATABASE_URL not set - skipping database setup (signal history won't be persisted).")
        return

    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS signals (
                        signal_id TEXT PRIMARY KEY,
                        pair TEXT NOT NULL,
                        market_type TEXT NOT NULL,
                        timeframe TEXT NOT NULL,
                        direction TEXT NOT NULL,
                        entry DOUBLE PRECISION NOT NULL,
                        sl DOUBLE PRECISION NOT NULL,
                        tp1 DOUBLE PRECISION NOT NULL,
                        tp2 DOUBLE PRECISION NOT NULL,
                        tp3 DOUBLE PRECISION NOT NULL,
                        confidence INTEGER,
                        risk TEXT,
                        status TEXT NOT NULL DEFAULT 'OPEN',
                        final_level TEXT,
                        sent_at TIMESTAMP NOT NULL,
                        closed_at TIMESTAMP
                    )
                """)
                # Safe to run every startup - only adds the column if it's
                # not already there, so this works whether the table is
                # brand new or already exists from an earlier deploy.
                cur.execute("""
                    ALTER TABLE signals
                    ADD COLUMN IF NOT EXISTS trigger_info TEXT
                """)
                # Stores the actual ATR value used to calculate this
                # signal's SL/TP distances - lets us verify directly from
                # the database whether a pair's ATR looks abnormal (like
                # the XAU/USD case) instead of guessing from screenshots.
                cur.execute("""
                    ALTER TABLE signals
                    ADD COLUMN IF NOT EXISTS atr_at_signal DOUBLE PRECISION
                """)
                # Stores the pass/fail breakdown of every confirmation
                # factor (EMA, RSI, BOS, CHOCH, liquidity sweep, order
                # block, etc.) for the dashboard's strategy confirmation
                # checklist - shows WHY a signal was accepted, not just
                # its final confidence number.
                cur.execute("""
                    ALTER TABLE signals
                    ADD COLUMN IF NOT EXISTS confirmation_factors JSONB
                """)
                # Market scanner table: one row per watched pair,
                # overwritten every scan cycle regardless of whether a
                # signal actually fires - lets the dashboard show what
                # the bot is currently seeing on every pair (trend,
                # confidence, status), not just pairs that produced a
                # signal.
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS pair_scan_status (
                        pair TEXT PRIMARY KEY,
                        market_type TEXT NOT NULL,
                        trend TEXT,
                        status TEXT NOT NULL,
                        confidence INTEGER,
                        direction TEXT,
                        updated_at TIMESTAMP NOT NULL
                    )
                """)
                # Single-row table the dashboard reads to show a live
                # "bot online / last scan Xs ago" indicator. Always the
                # same row (id=1), overwritten every scan cycle rather
                # than growing - this is current status, not history.
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS bot_status (
                        id INTEGER PRIMARY KEY DEFAULT 1,
                        last_scan_at TIMESTAMP,
                        bot_version TEXT,
                        CONSTRAINT single_row CHECK (id = 1)
                    )
                """)
                cur.execute("""
                    ALTER TABLE bot_status
                    ADD COLUMN IF NOT EXISTS smc_version TEXT
                """)
                # Live activity feed - only meaningful events get a row
                # here (a pair's scan status actually changing, a signal
                # firing, a trade closing, a bot restart), not every scan
                # cycle for every pair - keeps this cheap even running
                # 24/7. Old rows get pruned on insert so this never grows
                # unbounded.
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS activity_log (
                        id SERIAL PRIMARY KEY,
                        event_type TEXT NOT NULL,
                        message TEXT NOT NULL,
                        created_at TIMESTAMP NOT NULL
                    )
                """)
        conn.close()
        log_info("Database ready: 'signals', 'bot_status', 'pair_scan_status', and 'activity_log' tables confirmed/created.")
    except Exception as e:
        log_error(f"Database setup failed (bot will keep running without persistence): {e}")


def db_insert_signal(pair, market_type, timeframe, direction, result, signal_id):
    if not DATABASE_URL:
        return

    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO signals
                        (signal_id, pair, market_type, timeframe, direction,
                         entry, sl, tp1, tp2, tp3, confidence, risk,
                         atr_at_signal, confirmation_factors, status, sent_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'OPEN', %s)
                    ON CONFLICT (signal_id) DO NOTHING
                """, (
                    signal_id, pair, market_type, timeframe, direction,
                    result["entry"], result["sl"], result["tp1"], result["tp2"], result["tp3"],
                    result.get("confidence"), result.get("risk"),
                    result.get("atr"),
                    json.dumps(result["factors"]) if result.get("factors") is not None else None,
                    datetime.utcnow(),
                ))
        conn.close()
    except Exception as e:
        log_error(f"Database insert failed for Signal #{signal_id}: {e}")


def db_update_signal_status(signal_id, status, final_level=None, closed=False, trigger_info=None):
    if not DATABASE_URL:
        return

    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE signals
                    SET status = %s,
                        final_level = COALESCE(%s, final_level),
                        closed_at = CASE WHEN %s THEN %s ELSE closed_at END,
                        trigger_info = COALESCE(%s, trigger_info)
                    WHERE signal_id = %s
                """, (status, final_level, closed, datetime.utcnow(), trigger_info, signal_id))

                level_text = f" ({final_level})" if final_level else ""
                cur.execute("""
                    INSERT INTO activity_log (event_type, message, created_at)
                    VALUES (%s, %s, %s)
                """, (
                    "trade_outcome",
                    f"Signal #{signal_id} → {status}{level_text}",
                    datetime.utcnow(),
                ))
        conn.close()
    except Exception as e:
        log_error(f"Database update failed for Signal #{signal_id}: {e}")


def db_update_bot_status():
    """
    Overwrites the single bot_status row with the current time and
    version. Called at the end of every analysis scan cycle so the
    dashboard can show a real "online / last scan Xs ago" indicator -
    if this stops updating, the dashboard can tell the bot actually
    stopped rather than just showing a static "online" label that
    would lie if the bot crashed or Render spun it down.
    """
    if not DATABASE_URL:
        return

    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO bot_status (id, last_scan_at, bot_version, smc_version)
                    VALUES (1, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE
                    SET last_scan_at = EXCLUDED.last_scan_at,
                        bot_version = EXCLUDED.bot_version,
                        smc_version = EXCLUDED.smc_version
                """, (datetime.utcnow(), VERSION, smc_analysis.SMC_VERSION))
        conn.close()
    except Exception as e:
        log_error(f"Failed to update bot_status: {e}")


def db_update_pair_status(pair, market_type, trend, status, confidence=None, direction=None):
    """
    Overwrites this pair's single row in pair_scan_status - called at
    the end of every analyze_pair() run, regardless of whether a signal
    fired. This is what powers the dashboard's market scanner (showing
    every watched pair's current trend/confidence/status, not just the
    ones that produced a signal).

    Also logs to the activity feed, but only when the status actually
    CHANGES (e.g. WATCHING -> SETUP_READY) - not every scan cycle,
    which would be way too noisy and expensive to log continuously.
    """
    if not DATABASE_URL:
        return

    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT status FROM pair_scan_status WHERE pair = %s", (pair,))
                row = cur.fetchone()
                previous_status = row[0] if row else None

                cur.execute("""
                    INSERT INTO pair_scan_status
                        (pair, market_type, trend, status, confidence, direction, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (pair) DO UPDATE
                    SET market_type = EXCLUDED.market_type,
                        trend = EXCLUDED.trend,
                        status = EXCLUDED.status,
                        confidence = EXCLUDED.confidence,
                        direction = EXCLUDED.direction,
                        updated_at = EXCLUDED.updated_at
                """, (pair, market_type, trend, status, confidence, direction, datetime.utcnow()))

                if previous_status is not None and previous_status != status:
                    conf_text = f" ({confidence}%)" if confidence is not None else ""
                    cur.execute("""
                        INSERT INTO activity_log (event_type, message, created_at)
                        VALUES (%s, %s, %s)
                    """, (
                        "scan_status_change",
                        f"{pair} moved from {previous_status} → {status}{conf_text}",
                        datetime.utcnow(),
                    ))
        conn.close()
    except Exception as e:
        log_error(f"Failed to update pair_scan_status for {pair}: {e}")


def db_log_activity(event_type, message):
    """
    Writes one row to the live activity feed the dashboard shows. Only
    called at genuinely meaningful moments (a pair's status changing,
    a signal firing, a trade closing, a bot restart) - not on every
    scan cycle - so this stays cheap even running 24/7.

    Also prunes anything older than 7 days on every insert, so the
    table never grows unbounded over months of runtime.
    """
    if not DATABASE_URL:
        return

    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO activity_log (event_type, message, created_at)
                    VALUES (%s, %s, %s)
                """, (event_type, message, datetime.utcnow()))
                cur.execute("""
                    DELETE FROM activity_log
                    WHERE created_at < NOW() - INTERVAL '7 days'
                """)
        conn.close()
    except Exception as e:
        log_error(f"Failed to write activity_log entry: {e}")


def get_push_targets(alert_type):
    """
    Returns [(subscription_id, endpoint, p256dh, auth), ...] for every
    device that should receive this category of alert - i.e. the
    person has the master Push Notifications switch on AND the specific
    alert_type column (new_signal_alerts / signal_result_alerts /
    bot_status_alerts) on, joined against their registered device(s).
    A person with no row in user_settings yet (never opened Settings)
    gets nothing, rather than assuming they want alerts they never
    actually turned on.
    """
    if not DATABASE_URL or not PUSH_AVAILABLE:
        return []

    valid_columns = {"new_signal_alerts", "signal_result_alerts", "bot_status_alerts"}
    if alert_type not in valid_columns:
        log_error(f"get_push_targets called with unknown alert_type: {alert_type}")
        return []

    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT ps.id, ps.endpoint, ps.p256dh, ps.auth
                    FROM push_subscriptions ps
                    JOIN user_settings us ON us.user_id = ps.user_id
                    WHERE us.push_notifications = true AND us.{alert_type} = true
                """)
                rows = cur.fetchall()
        conn.close()
        return rows
    except Exception as e:
        log_error(f"Failed to fetch push targets for {alert_type}: {e}")
        return []


def delete_push_subscription(subscription_id):
    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM push_subscriptions WHERE id = %s", (subscription_id,))
        conn.close()
    except Exception as e:
        log_error(f"Failed to delete stale push_subscription {subscription_id}: {e}")


def get_auto_trading_users():
    """
    Returns [(user_id, risk_pct, balance), ...] for every user who has
    auto-trading turned on. Balance comes from a LEFT JOIN against
    portfolio, so a user who enabled auto-trading but never set an
    initial balance still shows up here with balance=0 - the caller
    is responsible for skipping them rather than sizing a trade off a
    zero balance.
    """
    if not DATABASE_URL:
        return []
    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT us.user_id, us.auto_trading_risk_pct, COALESCE(p.balance, 0)
                    FROM user_settings us
                    LEFT JOIN portfolio p ON p.user_id = us.user_id
                    WHERE us.auto_trading_enabled = true
                """)
                rows = cur.fetchall()
        conn.close()
        return rows
    except Exception as e:
        log_error(f"Failed to fetch auto-trading users: {e}")
        return []


# ==========================================================
# FOREX LOT SIZING (MT5-style)
# ==========================================================
# Reference data needed to turn a lot size into a real dollar figure.
# pip_size: the price movement that counts as "1 pip" for this pair.
# contract_size: units per 1.00 (standard) lot.
# quote_is_usd: True when the quote currency IS USD (EUR/USD, XAU/USD -
# gold priced in USD) so price differences are already in USD terms.
# False means the quote currency isn't USD (USD/JPY is quoted in JPY),
# so a price difference has to be converted to USD using the trade's
# own entry price as the conversion rate - a standard simplification
# (locks in the rate at trade open rather than re-converting live at
# close, which is how most retail platforms present it too).
FOREX_CONTRACT_INFO = {
    "EUR/USD": {"pip_size": 0.0001, "contract_size": 100000, "quote_is_usd": True},
    "GBP/USD": {"pip_size": 0.0001, "contract_size": 100000, "quote_is_usd": True},
    "USD/JPY": {"pip_size": 0.01, "contract_size": 100000, "quote_is_usd": False},
    "XAU/USD": {"pip_size": 0.01, "contract_size": 100, "quote_is_usd": True},
}


def compute_lot_sizing(pair, entry, sl, balance, risk_pct):
    """
    Returns {"lot_size", "position_size", "risk_amount"} for a forex
    pair, or None if this pair isn't forex (caller falls back to the
    existing risk%-only sizing for crypto) or the trade can't be sized
    safely at all.

    Flow, matching the "think in lots, bot keeps a safety net
    underneath" structure: start from a lot size scaled off balance
    (round(balance / 10000, 2), floored at the 0.01 minimum - tracks
    the reference table's $100->0.01 through $10,000->1.00 range
    smoothly instead of a rigid lookup that breaks between rows), then
    shrink it in 0.01 steps for real SL distance until the dollar risk
    fits under risk_pct of balance. If even 0.01 lot is still too much
    risk for this balance and this SL distance, the trade is skipped
    entirely rather than forcing an oversized position.
    """
    info = FOREX_CONTRACT_INFO.get(pair)
    if not info:
        return None

    sl_distance = abs(entry - sl)
    if sl_distance <= 0 or not balance or balance <= 0:
        return None

    pip_size = info["pip_size"]
    contract_size = info["contract_size"]
    quote_is_usd = info["quote_is_usd"]

    def dollar_risk_for_lot(lot):
        units = lot * contract_size
        risk_in_quote_ccy = units * sl_distance
        return risk_in_quote_ccy if quote_is_usd else risk_in_quote_ccy / entry

    lot = round(max(balance / 10000, 0.01), 2)
    risk_ceiling = balance * (risk_pct / 100.0)

    while lot > 0 and dollar_risk_for_lot(lot) > risk_ceiling:
        lot = round(lot - 0.01, 2)

    if lot < 0.01:
        return None

    units = lot * contract_size
    # Folding the USD-conversion into position_size here means the
    # existing close-price P/L formula (position_size * price_diff)
    # keeps working completely unchanged for forex too - no separate
    # P/L math needed per pair at close time.
    position_size = units if quote_is_usd else units / entry

    return {
        "lot_size": lot,
        "position_size": position_size,
        "risk_amount": dollar_risk_for_lot(lot),
    }


def open_auto_trades_for_signal(pair, market_type, direction, entry, sl, tp1, tp2, tp3, signal_id):
    """
    Opens one sized position per auto-trading user for a freshly-fired
    signal, using SmartFX's own Entry/SL/TP levels - never a separate
    SL/TP calculation of its own.

    Forex pairs (see FOREX_CONTRACT_INFO) are sized in lots, MT5-style:
    a lot size derived from the user's balance, capped down by their
    risk% if the signal's real SL distance would otherwise risk too
    much. Crypto pairs keep the original balance*risk% / SL-distance
    sizing, since "lots" aren't a crypto concept.
    """
    if not DATABASE_URL:
        return

    sl_distance = abs(entry - sl)
    if sl_distance <= 0:
        log_error(f"Auto-trade skipped for Signal #{signal_id}: SL distance is zero.")
        return

    users = get_auto_trading_users()
    if not users:
        return

    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                for user_id, risk_pct, balance in users:
                    if not balance or balance <= 0:
                        # Risk is a % of balance - a $0 or unset
                        # balance has nothing meaningful to risk, so
                        # skip rather than open a zero-size position.
                        continue

                    balance = float(balance)
                    risk_pct = float(risk_pct)
                    lot_sizing = compute_lot_sizing(pair, entry, sl, balance, risk_pct)

                    if lot_sizing:
                        lot_size = lot_sizing["lot_size"]
                        position_size = lot_sizing["position_size"]
                        risk_amount = lot_sizing["risk_amount"]
                    elif pair in FOREX_CONTRACT_INFO:
                        # Forex pair, but even the 0.01 lot minimum was
                        # too risky for this balance/SL combination -
                        # skip this user's trade entirely rather than
                        # force an oversized position.
                        continue
                    else:
                        # Crypto - unchanged balance*risk%/SL-distance sizing.
                        lot_size = None
                        risk_amount = balance * (risk_pct / 100.0)
                        position_size = risk_amount / sl_distance

                    cur.execute("""
                        INSERT INTO auto_trades
                            (user_id, signal_id, pair, market_type, direction,
                             entry, sl, tp1, tp2, tp3, risk_pct, risk_amount,
                             position_size, lot_size)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (user_id, signal_id) DO NOTHING
                    """, (
                        user_id, signal_id, pair, market_type, direction,
                        entry, sl, tp1, tp2, tp3, risk_pct, risk_amount,
                        position_size, lot_size,
                    ))
        conn.close()
    except Exception as e:
        log_error(f"Failed to open auto-trades for Signal #{signal_id}: {e}")


def close_auto_trades_for_signal(signal_id, outcome, final_level=None):
    """
    Closes every open auto-trade for this signal (one per user who had
    it open) and applies the real dollar P/L straight to that user's
    portfolio balance, logging a balance snapshot so the dashboard's
    Today/Week/Month P/L stays accurate automatically. outcome is
    WIN/LOSS/EXPIRED - EXPIRED closes at $0 P/L since nothing actually
    resolved. WIN's exit level is whichever of TP1/TP2/TP3 final_level
    points to; the same +/- distance-from-entry formula as a LOSS
    means the position_size already computed from risk_amount/sl_
    distance produces exactly -risk_amount at the real SL, and the
    correct proportional profit at any TP - no separate P/L formula
    needed per market type.
    """
    if not DATABASE_URL:
        return

    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, user_id, direction, entry, sl, tp1, tp2, tp3, position_size
                    FROM auto_trades
                    WHERE signal_id = %s AND status = 'OPEN'
                """, (signal_id,))
                open_trades = cur.fetchall()

                for trade_id, user_id, direction, entry, sl, tp1, tp2, tp3, position_size in open_trades:
                    # position_size is a Postgres numeric -> comes back
                    # as a Decimal, while entry/sl/tp1/tp2/tp3 are
                    # double precision -> come back as float. Mixing
                    # Decimal and float directly raises a TypeError,
                    # which was silently caught by the except below on
                    # every single close attempt - this cast is the
                    # actual fix for auto-trades never closing.
                    position_size = float(position_size)

                    if outcome == "EXPIRED":
                        pl_amount = 0.0
                    elif outcome == "LOSS":
                        exit_price = sl
                        pl_amount = position_size * (exit_price - entry) if direction == "BUY" \
                            else position_size * (entry - exit_price)
                    else:  # WIN
                        exit_price = {"TP1": tp1, "TP2": tp2, "TP3": tp3}.get(final_level, tp1)
                        pl_amount = position_size * (exit_price - entry) if direction == "BUY" \
                            else position_size * (entry - exit_price)

                    cur.execute("""
                        UPDATE auto_trades
                        SET status = %s, final_level = %s, pl_amount = %s, closed_at = NOW()
                        WHERE id = %s
                    """, (outcome, final_level, pl_amount, trade_id))

                    if pl_amount != 0:
                        cur.execute("""
                            INSERT INTO portfolio (user_id, balance, updated_at)
                            VALUES (%s, %s, NOW())
                            ON CONFLICT (user_id) DO UPDATE
                            SET balance = portfolio.balance + EXCLUDED.balance, updated_at = NOW()
                        """, (user_id, pl_amount))
                        # The ON CONFLICT branch above adds pl_amount to the
                        # existing balance; the INSERT branch (first-ever
                        # row) would incorrectly set balance = pl_amount
                        # instead of 0 + pl_amount, but since a user only
                        # gets here after having a real balance already
                        # counted in get_auto_trading_users(), a fresh
                        # INSERT should never actually happen in practice.
                        cur.execute("SELECT balance FROM portfolio WHERE user_id = %s", (user_id,))
                        new_balance = cur.fetchone()[0]
                        cur.execute("""
                            INSERT INTO portfolio_balance_log (user_id, balance)
                            VALUES (%s, %s)
                        """, (user_id, new_balance))
        conn.close()
    except Exception as e:
        log_error(f"Failed to close auto-trades for Signal #{signal_id}: {e}")


def send_push_to_all(alert_type, title, body, url="dashboard.html"):
    """
    Sends one push notification to every device subscribed to this
    alert_type. A subscription that comes back expired/invalid (the
    browser unsubscribed, or the person cleared site data) is deleted
    right away instead of being retried forever.
    """
    if not PUSH_AVAILABLE:
        log_info("Push notification skipped: pywebpush not installed.")
        return
    if not VAPID_PRIVATE_KEY:
        log_info("Push notification skipped: VAPID_PRIVATE_KEY not set.")
        return

    targets = get_push_targets(alert_type)
    if not targets:
        return

    payload = json.dumps({"title": title, "body": body, "url": url})

    for sub_id, endpoint, p256dh, auth in targets:
        try:
            webpush(
                subscription_info={
                    "endpoint": endpoint,
                    "keys": {"p256dh": p256dh, "auth": auth},
                },
                data=payload,
                vapid_private_key=VAPID_PRIVATE_KEY,
                vapid_claims={"sub": VAPID_CLAIMS_EMAIL},
            )
        except WebPushException as e:
            status = e.response.status_code if e.response is not None else None
            if status in (404, 410):
                # Subscription no longer valid on the browser's end -
                # clean it up so we stop trying it every time.
                delete_push_subscription(sub_id)
            else:
                log_error(f"Push send failed ({alert_type}) to subscription {sub_id}: {e}")
        except Exception as e:
            log_error(f"Unexpected push error ({alert_type}) to subscription {sub_id}: {e}")


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
            "volume": float(row[6]),
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

TWELVEDATA_MAX_CALLS_PER_MINUTE = 8

_twelvedata_call_times = []
_twelvedata_rate_lock = threading.Lock()


def twelvedata_rate_limit():
    """
    Guarantees no more than TWELVEDATA_MAX_CALLS_PER_MINUTE calls go out
    to TwelveData in any rolling 60-second window, blocking (sleeping)
    if needed before letting a call through.

    The fixed delays elsewhere (FOREX_INNER_CALL_DELAY_SECONDS,
    FOREX_PAIR_DELAY_SECONDS) space calls out during a normal scan, but
    they don't actually enforce the limit - right after a restart the
    in-memory trend cache is empty, so all 4 forex pairs need a fresh
    trend + 1H + 15M fetch (12 calls) fired in quick succession, which
    was enough to exceed 8 calls/minute and trigger 429 "Too Many
    Requests" errors. This limiter is a hard backstop that holds true
    regardless of restarts, cache state, or timing changes elsewhere.
    """
    global _twelvedata_call_times

    while True:
        now = time.time()
        with _twelvedata_rate_lock:
            _twelvedata_call_times = [t for t in _twelvedata_call_times if now - t < 60]
            if len(_twelvedata_call_times) < TWELVEDATA_MAX_CALLS_PER_MINUTE:
                _twelvedata_call_times.append(now)
                return
            oldest = _twelvedata_call_times[0]

        wait = 60 - (now - oldest) + 0.5
        time.sleep(max(wait, 0.5))


def fetch_twelvedata_candles(symbol, interval, outputsize=CANDLE_LIMIT):
    twelvedata_rate_limit()
    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": symbol,
        "interval": interval,
        "outputsize": outputsize,
        "apikey": TWELVEDATA_API_KEY,
        # Without this, TwelveData defaults to the exchange's local
        # timezone, not UTC - but parse_candle_time() and the rest of
        # the bot assume every timestamp it receives is already UTC.
        # That mismatch is what caused the "future-timestamped candle"
        # errors (an entire batch of real candles all shifted by a
        # fixed offset looks like it's ahead of "now"), and very likely
        # also fed subtly shifted candle data into ATR calculations -
        # a plausible root cause of the abnormal XAU/USD ATR readings.
        "timezone": "UTC",
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
            "volume": float(v.get("volume") or 0),
        })

    return candles


def fetch_twelvedata_price(symbol):
    twelvedata_rate_limit()
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


def seed_signal_id_counter():
    """
    On every startup, signal_id_counter used to reset to 0 in memory,
    so IDs always restarted at #0001 after a redeploy - colliding with
    IDs already saved in the database from before that redeploy. Since
    signal_id is the database's PRIMARY KEY and inserts use
    ON CONFLICT (signal_id) DO NOTHING, every one of those colliding
    signals was silently skipped - no error, no log line, it just
    never got saved. This is why the signals table appeared "frozen"
    after a redeploy even though new signals kept posting to Telegram
    fine.

    This reads the highest signal_id already in the database and
    continues counting from there, so IDs stay unique across any
    number of redeploys. Safe to call even if the table is empty or
    the database is temporarily unreachable - falls back to starting
    at 0 (so the first signal sent is #0001), same as before.
    """
    global signal_id_counter

    if not DATABASE_URL:
        return

    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT signal_id FROM signals
                    ORDER BY (signal_id::INTEGER) DESC
                    LIMIT 1
                """)
                row = cur.fetchone()
        conn.close()

        if row:
            with state_lock:
                signal_id_counter = int(row[0])
            log_info(f"Signal ID counter resumed from database at #{row[0]} (next signal will be #{int(row[0]) + 1:04d}).")
        else:
            log_info("No existing signals found in database - signal ID counter starting fresh at #0001.")

    except Exception as e:
        log_error(f"Could not resume signal ID counter from database, starting from 0: {e}")


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

def parse_candle_time(time_str):
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(time_str, fmt)
        except (ValueError, TypeError):
            continue
    return None


def check_forex_candles_for_hit(trade):
    """
    Checks the most recently cached candles (from the last scan) for
    this trade's pair/timeframe to see if TP1 or the Stop Loss was
    touched by a candle's high/low range since the trade opened - this
    catches real moves that happened and reversed between the 20-minute
    forex price-cache refreshes, which a single cached closing price
    would otherwise miss entirely.

    NOTE: if a single candle's range touches both TP1 and the Stop
    Loss, we can't know which happened first within that candle - we
    conservatively assume the Stop Loss was hit first, so we never
    overstate a win.

    Returns a tuple: (outcome, trigger_info) where outcome is "WIN",
    "LOSS", or None, and trigger_info is a diagnostic string showing
    the matching candle PLUS a few candles immediately around it, plus
    the overall min/max range of the full cached batch - this makes it
    possible to see whether one candle looks like a bad/glitched data
    point sitting among otherwise-normal neighbors, or whether the
    entire cached batch for that pair is shifted/wrong. All of this
    lands in Supabase, not just server logs, so it's easy to check.
    """

    key = f"{trade['pair']}_{trade['timeframe']}"

    with state_lock:
        candles = list(forex_candle_cache.get(key, []))

    if not candles:
        return None, None

    # FRESHNESS CHECK: if the most recent candle in the cache is
    # noticeably older than expected, the whole cached batch may be
    # stale - internally consistent with itself, but reflecting an
    # earlier point in time than "now" (e.g. the market moved a lot
    # since the last scan, but the cache hasn't caught up yet). A
    # stale-but-self-consistent batch is exactly what caused a real
    # incident: an internally coherent set of candles that simply
    # didn't reflect where the market actually was. Rather than trust
    # it, skip the candle check entirely and let monitor_trades fall
    # back to a fresh live price fetch instead.
    last_candle_time = parse_candle_time(candles[-1].get("time"))
    now = datetime.utcnow()

    if last_candle_time is None or (now - last_candle_time).total_seconds() > FOREX_SCAN_INTERVAL_SECONDS * 2:
        log_error(
            f"[DATA-ANOMALY] Forex candle cache for {trade['pair']} "
            f"{trade['timeframe']} looks stale (last candle="
            f"{last_candle_time.isoformat() if last_candle_time else 'unknown'}, "
            f"now={now.isoformat()}) - skipping candle check, falling back "
            f"to a fresh live price fetch instead."
        )
        return None, None

    opened_at = datetime.fromisoformat(trade["opened_at"])
    direction = trade["direction"]

    for i, c in enumerate(candles):
        candle_time = parse_candle_time(c.get("time"))

        if candle_time is None or candle_time <= opened_at:
            continue

        # SANITY CHECK: a "completed" candle can never represent a time
        # period that hasn't happened yet. If one shows up with a
        # future timestamp, it's bad/mismatched data (whatever the
        # underlying cause) and must never be trusted to decide a
        # trade's outcome - skip it rather than risk a false result.
        if candle_time > now:
            log_error(
                f"[DATA-ANOMALY] Skipped a future-timestamped candle for "
                f"{trade['pair']} {trade['timeframe']}: candle_time="
                f"{candle_time.isoformat()} is after current time={now.isoformat()}"
            )
            continue

        hit_sl = (
            (direction == "BUY" and c["low"] <= trade["sl"])
            or (direction == "SELL" and c["high"] >= trade["sl"])
        )
        hit_tp1 = (
            (direction == "BUY" and c["high"] >= trade["tp1"])
            or (direction == "SELL" and c["low"] <= trade["tp1"])
        )

        if hit_sl or hit_tp1:
            outcome = "LOSS" if hit_sl else "WIN"

            # A single candle only gives us O/H/L/C, not the order price
            # actually moved within that candle - so if BOTH the SL and
            # TP1 levels fall inside the same candle's range, we genuinely
            # can't tell which was touched first. Defaulting to LOSS is
            # the safe assumption (never overstate a win), but this makes
            # that ambiguous case visible in trigger_info instead of
            # silently treating it the same as a clean, unambiguous SL hit.
            both_hit_same_candle = hit_sl and hit_tp1
            ambiguity_tag = ""

            if both_hit_same_candle:
                log_error(
                    f"[AMBIGUOUS-CANDLE] {trade['pair']} {trade['timeframe']} "
                    f"signal touched both SL ({trade['sl']}) and TP1 ({trade['tp1']}) "
                    f"within the same candle (O={c['open']} H={c['high']} L={c['low']} "
                    f"C={c['close']}) - attempting to resolve with 1-minute candles."
                )

                resolved_outcome, resolved_note = resolve_ambiguous_candle_with_finer_data(
                    trade, candle_time
                )

                if resolved_outcome is not None:
                    log_error(
                        f"[AMBIGUOUS-CANDLE] {trade['pair']} {trade['timeframe']} "
                        f"resolved: {resolved_note}"
                    )
                    outcome = resolved_outcome
                    ambiguity_tag = f"AMBIGUOUS_SAME_CANDLE ({resolved_note}) | "
                else:
                    log_error(
                        f"[AMBIGUOUS-CANDLE] {trade['pair']} {trade['timeframe']} "
                        f"couldn't be resolved with 1-minute data - defaulted to LOSS "
                        f"since intrabar order is unknown."
                    )
                    ambiguity_tag = (
                        "AMBIGUOUS_SAME_CANDLE (both SL and TP1 in range - "
                        "1min resolution unavailable, defaulted to LOSS) | "
                    )

            context_start = max(0, i - 3)
            context_end = min(len(candles), i + 4)
            context_candles = candles[context_start:context_end]

            context_str = " || ".join(
                f"[{parse_candle_time(cc.get('time'))}] O={cc['open']} H={cc['high']} "
                f"L={cc['low']} C={cc['close']}"
                + (" <== MATCH" if cc is c else "")
                for cc in context_candles
            )

            all_lows = [cc["low"] for cc in candles]
            all_highs = [cc["high"] for cc in candles]

            trigger = (
                f"{ambiguity_tag}"
                f"forex_candle open={c['open']} high={c['high']} low={c['low']} "
                f"close={c['close']} candle_time={candle_time.isoformat()} "
                f"opened_at={opened_at.isoformat()} | "
                f"CONTEXT: {context_str} | "
                f"FULL_CACHE_RANGE: low={min(all_lows)} high={max(all_highs)} count={len(candles)}"
            )

            return outcome, trigger

    return None, None


def resolve_ambiguous_candle_with_finer_data(trade, candle_time):
    """
    When a single cached 1H/15M candle's range shows both the Stop
    Loss and TP1 as touched, we can't tell which happened first from
    that candle's O/H/L/C alone - this is what caused the GBP/USD
    #0043 case to be logged as a LOSS even though a 1-minute chart
    showed TP1 was actually hit first.

    This fetches 1-minute candles covering that exact candle's time
    window and checks them in order, so the TRUE sequence of events
    can be determined instead of guessing.

    Returns (outcome, note):
      - ("WIN"/"LOSS", note) if resolved (or still ambiguous even at
        1-minute resolution, in which case it stays LOSS by the same
        safe default as before).
      - (None, None) if 1-minute data couldn't be fetched or didn't
        cover the window - caller should fall back to the original
        conservative default.
    """
    window_minutes = TIMEFRAME_MINUTES.get(trade["timeframe"], 60)

    try:
        one_min_candles = fetch_twelvedata_candles(
            trade["pair"], "1min", outputsize=window_minutes + 30
        )
    except Exception as e:
        log_error(
            f"[AMBIGUOUS-CANDLE] Failed to fetch 1-minute candles for "
            f"{trade['pair']} to resolve ambiguity: {e}"
        )
        return None, None

    window_start = candle_time
    window_end = candle_time + timedelta(minutes=window_minutes)
    direction = trade["direction"]

    for c in one_min_candles:
        c_time = parse_candle_time(c.get("time"))

        if c_time is None or c_time < window_start or c_time >= window_end:
            continue

        hit_sl = (
            (direction == "BUY" and c["low"] <= trade["sl"])
            or (direction == "SELL" and c["high"] >= trade["sl"])
        )
        hit_tp1 = (
            (direction == "BUY" and c["high"] >= trade["tp1"])
            or (direction == "SELL" and c["low"] <= trade["tp1"])
        )

        if hit_sl and hit_tp1:
            # Still ambiguous even at 1-minute resolution (rare) - keep
            # the same safe default, but note it was checked further.
            return "LOSS", f"still ambiguous at 1min resolution ({c_time.isoformat()})"

        if hit_sl:
            return "LOSS", f"RESOLVED_VIA_1MIN: SL hit first at {c_time.isoformat()}"

        if hit_tp1:
            return "WIN", f"RESOLVED_VIA_1MIN: TP1 hit first at {c_time.isoformat()}"

    return None, None


def has_active_trade_for_pair(pair):
    with state_lock:
        # Only trades that haven't hit TP1 yet count as real open risk.
        # Once TP1 is secured, the trade is a guaranteed win regardless
        # of what happens next - so it shouldn't keep blocking new
        # signals on that pair while we passively watch for TP2/TP3.
        return any(
            t["pair"] == pair and not t.get("tp1_hit", False)
            for t in active_trades.values()
        )


def count_active_risk_trades():
    with state_lock:
        return sum(1 for t in active_trades.values() if not t.get("tp1_hit", False))


def resume_open_trades_from_db():
    """
    THE FIX for trades that get stuck OPEN forever after a restart.

    active_trades is a plain in-memory dict - it starts empty every
    time the process restarts (a deploy, a crash, one of Wispbyte's
    free-tier hiccups). Before this fix, any trade that was mid-
    tracking at that moment (TP1 already hit, still watching for
    TP2/TP3/breakeven/expiry) was silently wiped from memory and never
    checked again - it just sat there "OPEN" forever, and its
    auto-trade P/L never applied to the real Portfolio balance either,
    since nothing ever called close_auto_trades_for_signal for it.

    The signals table already has everything needed to rebuild that
    state: `closed_at IS NULL` identifies exactly the set of trades
    that should still be under active tracking, right now, at this
    moment - covering both "hasn't hit TP1 or SL yet" (status=OPEN)
    and "TP1 hit, still watching for TP2/TP3/breakeven/expiry"
    (status=WIN, closed_at still null). tp1_hit/tp2_hit/tp3_hit aren't
    stored as their own columns, but they're fully recoverable from
    status + final_level - a WIN row with closed_at null can only be
    sitting at TP1 or TP2, since a TP3 hit always closes the row
    immediately in the existing close logic.

    Called once, at startup, before the analysis/trade-monitor loops
    start - so trade_monitor_loop picks up exactly where the last
    process left off instead of losing anything.
    """
    if not DATABASE_URL:
        return

    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT signal_id, pair, market_type, timeframe, direction,
                           entry, sl, tp1, tp2, tp3, status, final_level, sent_at
                    FROM signals
                    WHERE closed_at IS NULL
                """)
                rows = cur.fetchall()
        conn.close()
    except Exception as e:
        log_error(f"Failed to resume open trades from database: {e}")
        return

    resumed_count = 0
    for (signal_id, pair, market_type, timeframe, direction,
         entry, sl, tp1, tp2, tp3, status, final_level, sent_at) in rows:

        tp1_hit = status == "WIN"
        tp2_hit = tp1_hit and final_level == "TP2"

        trade = {
            "pair": pair,
            "timeframe": timeframe,
            "market_type": market_type,
            "direction": direction,
            "entry": entry,
            "sl": sl,
            "tp1": tp1,
            "tp2": tp2,
            "tp3": tp3,
            "opened_at": sent_at.isoformat(),
            "signal_id": signal_id,
            "tp1_hit": tp1_hit,
            "tp2_hit": tp2_hit,
            "tp3_hit": False,  # a real TP3 hit always closes the row immediately - can't coexist with closed_at IS NULL
        }

        trade_id = f"{pair}_{timeframe}_{signal_id}"
        with state_lock:
            active_trades[trade_id] = trade
        resumed_count += 1

    if resumed_count:
        log_info(f"Resumed {resumed_count} still-open trade(s) from the database after restart.")
    else:
        log_info("No open trades to resume from the database.")


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
        "tp1_hit": False,
        "tp2_hit": False,
        "tp3_hit": False,
    }

    with state_lock:
        active_trades[trade_id] = trade

    return trade_id


def close_trade(trade_id, trade, outcome, keep_tracking=False, trigger_info=None):
    pair = trade["pair"]

    with state_lock:
        if not keep_tracking:
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

    # DIAGNOSTIC: exact reason this trade closed, so a repeat of the
    # "instant loss" pattern can be proven from logs instead of
    # reconstructed by hand from screenshots and timestamps.
    try:
        opened_at = datetime.fromisoformat(trade["opened_at"])
        seconds_since_open = (datetime.utcnow() - opened_at).total_seconds()
    except Exception:
        seconds_since_open = None

    log_info(
        f"[DIAGNOSTIC] Signal #{trade.get('signal_id', 'N/A')} closed as {outcome} | "
        f"entry={trade['entry']} sl={trade['sl']} tp1={trade['tp1']} | "
        f"trigger={trigger_info or 'N/A'} | "
        f"closed {seconds_since_open:.0f}s after opening"
        if seconds_since_open is not None else
        f"[DIAGNOSTIC] Signal #{trade.get('signal_id', 'N/A')} closed as {outcome} | "
        f"entry={trade['entry']} sl={trade['sl']} tp1={trade['tp1']} | "
        f"trigger={trigger_info or 'N/A'} | opened_at unavailable"
    )

    signal_id = trade.get("signal_id", "N/A")
    result_emoji = "🏆" if outcome == "WIN" else "❌"
    hit_text = "Hit TP1" if outcome == "WIN" else "Hit Stop Loss"
    extra_note = "\n\n(Still tracking to see if TP2/TP3 are also reached...)" if keep_tracking else ""

    send_private_message(
        f"{result_emoji} *Signal #{signal_id} Result: {outcome}*\n\n"
        f"{pair} | {trade['timeframe']} | {trade['direction']}\n"
        f"Entry: `{trade['entry']}`\n"
        f"{hit_text}{extra_note}"
    )

    push_emoji = "✅" if outcome == "WIN" else ""
    send_push_to_all(
        "signal_result_alerts",
        f"SmartFX Result {push_emoji}".strip(),
        f"{pair} {trade['direction']} — {outcome}",
        url=f"dashboard.html?signal={signal_id}",
    )

    # A LOSS is always a final outcome (no further tracking happens
    # after a stop loss) - send the branded result card here.
    if outcome == "LOSS":
        send_result_card(trade, "LOSS")
        db_update_signal_status(signal_id, "LOSS", closed=True, trigger_info=trigger_info)
        close_auto_trades_for_signal(signal_id, "LOSS")
    else:
        # TP1 secured - send the public result card right away instead
        # of waiting for the trade to fully close (TP2/TP3/breakeven/
        # expiry), which could be hours later. This is what caused the
        # public group to go quiet on a win for hours while the
        # private alerts chat already knew (Signal #0060 case) - the
        # private message above and the public card now go out
        # together. If TP2/TP3 hit later, an updated card is sent then
        # too (see monitor_trades), so the public card always reflects
        # the best level reached so far.
        send_result_card(trade, "WIN", final_tp_label="TP1")
        db_update_signal_status(signal_id, "WIN", final_level="TP1", closed=not keep_tracking, trigger_info=trigger_info)
        if not keep_tracking:
            # Only truly final here if TP2/TP3 tracking isn't
            # continuing - otherwise the real close happens later at
            # whichever point (TP2, TP3, breakeven reversal, or
            # expiry) actually stops tracking this signal.
            close_auto_trades_for_signal(signal_id, "WIN", final_level="TP1")


def expire_trade(trade_id, trade):
    with state_lock:
        active_trades.pop(trade_id, None)

    expiry_seconds = SIGNAL_EXPIRY_SECONDS.get(trade["timeframe"], 4 * 3600)
    hours = expiry_seconds / 3600

    log_info(
        f"Trade expired (no TP1/SL hit within "
        f"{hours:.0f}h): "
        f"{trade['pair']} {trade['direction']} ({trade['timeframe']})"
    )

    signal_id = trade.get("signal_id", "N/A")

    send_private_message(
        f"⏳ *Signal #{signal_id} Expired*\n\n"
        f"{trade['pair']} | {trade['timeframe']} | {trade['direction']}\n"
        f"No TP1 or Stop Loss hit within {hours:.0f}h - closed without a result."
    )

    send_push_to_all(
        "signal_result_alerts",
        "SmartFX Result ⏱️",
        f"{trade['pair']} {trade['direction']} — EXPIRED",
        url=f"dashboard.html?signal={signal_id}",
    )

    db_update_signal_status(signal_id, "EXPIRED", closed=True)
    close_auto_trades_for_signal(signal_id, "EXPIRED")


def monitor_trades():
    with state_lock:
        trades_snapshot = list(active_trades.items())

    for trade_id, trade in trades_snapshot:
        direction = trade["direction"]
        tp1_hit = trade.get("tp1_hit", False)

        if not tp1_hit:
            outcome = None
            trigger_info = None

            # For forex, first check the cached candle high/low range -
            # this catches a real TP1/SL touch that happened and
            # reversed between the 20-minute price-cache refreshes,
            # which a single point-in-time price would miss.
            if trade["market_type"] == "forex":
                outcome, trigger_info = check_forex_candles_for_hit(trade)

            if outcome is None:
                try:
                    price = get_current_price(trade["pair"], trade["market_type"])
                except Exception as e:
                    log_error(f"Price fetch failed while monitoring {trade['pair']}: {e}")
                    continue

                # SANITY CHECK: same guard as the forex candle path -
                # reject an implausible price that's wildly farther from
                # entry than any real move should be, rather than trust
                # it to decide a trade's outcome.
                risk = abs(trade["entry"] - trade["sl"])
                price_is_sane = True
                if risk > 0 and abs(price - trade["entry"]) > risk * 5:
                    price_is_sane = False
                    log_error(
                        f"[DATA-ANOMALY] Ignored an implausible live price for "
                        f"{trade['pair']} {trade['timeframe']}: price={price} is "
                        f"more than 5x the risk distance ({risk:.5f}) away from "
                        f"entry={trade['entry']} - treating as bad data."
                    )

                if price_is_sane:
                    hit_sl = (
                        (direction == "BUY" and price <= trade["sl"])
                        or (direction == "SELL" and price >= trade["sl"])
                    )
                    hit_tp1 = (
                        (direction == "BUY" and price >= trade["tp1"])
                        or (direction == "SELL" and price <= trade["tp1"])
                    )

                    if hit_sl:
                        outcome = "LOSS"
                        trigger_info = f"live_price={price} source={trade['market_type']}"
                    elif hit_tp1:
                        outcome = "WIN"
                        trigger_info = f"live_price={price} source={trade['market_type']}"

            # Stop loss only counts as a real LOSS if TP1 hasn't already
            # been secured - once TP1 hits, the trade is a guaranteed
            # win no matter what happens afterward.
            if outcome == "LOSS":
                close_trade(trade_id, trade, "LOSS", trigger_info=trigger_info)
                continue

            if outcome == "WIN":
                with state_lock:
                    if trade_id in active_trades:
                        active_trades[trade_id]["tp1_hit"] = True
                close_trade(trade_id, trade, "WIN", keep_tracking=True, trigger_info=trigger_info)
                continue

        else:
            # TP1 already secured - now just watching to see how far
            # the move keeps running. This never affects win/loss
            # stats either way, it's purely informational.
            try:
                price = get_current_price(trade["pair"], trade["market_type"])
            except Exception as e:
                log_error(f"Price fetch failed while monitoring {trade['pair']}: {e}")
                continue

            # Breakeven: if price falls all the way back to entry after
            # TP1 was already secured, stop tracking for TP2/TP3 here -
            # a full reversal back to entry is a sign the strong
            # continuation isn't happening. The WIN result was already
            # recorded and never changes; this only affects when we
            # stop watching for further milestones.
            breakeven_hit = (
                (direction == "BUY" and price <= trade["entry"])
                or (direction == "SELL" and price >= trade["entry"])
            )

            if breakeven_hit:
                final_label = "TP2" if trade.get("tp2_hit") else "TP1"
                # No card sent here - the public card for whichever
                # level was actually reached (TP1 or TP2) already went
                # out immediately when that level was hit, so sending
                # another one here would just be a duplicate. This just
                # closes out the DB row and stops watching.
                db_update_signal_status(trade.get("signal_id"), "WIN", final_level=final_label, closed=True)
                close_auto_trades_for_signal(trade.get("signal_id"), "WIN", final_level=final_label)
                with state_lock:
                    active_trades.pop(trade_id, None)
                log_info(
                    f"Stopped tracking Signal #{trade.get('signal_id')} for "
                    "TP2/TP3 - price returned to breakeven (entry)."
                )
                continue

            if not trade.get("tp2_hit"):
                hit_tp2 = (
                    (direction == "BUY" and price >= trade["tp2"])
                    or (direction == "SELL" and price <= trade["tp2"])
                )
                if hit_tp2:
                    with state_lock:
                        if trade_id in active_trades:
                            active_trades[trade_id]["tp2_hit"] = True
                    signal_id = trade.get("signal_id", "N/A")
                    db_update_signal_status(signal_id, "WIN", final_level="TP2", closed=False)
                    send_private_message(
                        f"🎯 *Signal #{signal_id} also hit TP2!*\n\n"
                        f"{trade['pair']} | {trade['timeframe']} | {trade['direction']}\n"
                        "Strong move - still running."
                    )
                    # Updated public card so the group sees TP2 too,
                    # not just the earlier TP1 card - same reasoning as
                    # the TP1 fix above. Auto-trades stay OPEN here too -
                    # TP2 isn't final, tracking continues for TP3.
                    send_result_card(trade, "WIN", final_tp_label="TP2")
                    continue

            if not trade.get("tp3_hit"):
                hit_tp3 = (
                    (direction == "BUY" and price >= trade["tp3"])
                    or (direction == "SELL" and price <= trade["tp3"])
                )
                if hit_tp3:
                    signal_id = trade.get("signal_id", "N/A")
                    send_private_message(
                        f"🚀 *Signal #{signal_id} ran all the way to TP3!*\n\n"
                        f"{trade['pair']} | {trade['timeframe']} | {trade['direction']}\n"
                        "Very strong move - final target reached."
                    )
                    # TP3 is the biggest win milestone - a genuinely
                    # final outcome, so this is where the branded
                    # result card is sent.
                    send_result_card(trade, "WIN", final_tp_label="TP3")
                    db_update_signal_status(signal_id, "WIN", final_level="TP3", closed=True)
                    close_auto_trades_for_signal(signal_id, "WIN", final_level="TP3")
                    with state_lock:
                        active_trades.pop(trade_id, None)
                    continue

        opened_at = datetime.fromisoformat(trade["opened_at"])
        age_seconds = (datetime.utcnow() - opened_at).total_seconds()
        expiry_seconds = SIGNAL_EXPIRY_SECONDS.get(trade["timeframe"], 4 * 3600)

        if age_seconds > expiry_seconds:
            if tp1_hit:
                # Already a confirmed WIN - stop watching for further
                # TP2/TP3. No card sent here either, same reasoning as
                # the breakeven path above: the card for whichever
                # level was actually reached already went out
                # immediately when that level was hit.
                final_label = "TP2" if trade.get("tp2_hit") else "TP1"
                db_update_signal_status(trade.get("signal_id"), "WIN", final_level=final_label, closed=True)
                close_auto_trades_for_signal(trade.get("signal_id"), "WIN", final_level=final_label)
                with state_lock:
                    active_trades.pop(trade_id, None)
                log_info(
                    f"Stopped tracking Signal #{trade.get('signal_id')} for "
                    "TP2/TP3 (already a WIN, tracking window closed)."
                )
            else:
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
    entry_candles_by_tf = {}
    signal_sent_this_scan = False

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
        #
        # Also cache the full candle set (not just the latest close) so
        # trade monitoring can check each candle's high/low range for a
        # TP/SL touch that happened and reversed between scans - a single
        # cached closing price would miss that kind of move entirely.
        if market_type == "forex" and entry_candles:
            with state_lock:
                forex_price_cache[pair] = {
                    "price": entry_candles[-1]["close"],
                    "fetched_at": time.time(),
                }
                forex_candle_cache[f"{pair}_{timeframe}"] = entry_candles

        try:
            result = smc_analysis.analyze_candles(entry_candles, trend_4h=trend)
        except Exception as e:
            log_error(f"Analysis error for {pair} ({timeframe}): {e}")
            continue

        timeframe_results[timeframe] = result
        entry_candles_by_tf[timeframe] = entry_candles

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

            log_info(
                f"Signal #{signal_id} sent: {pair} {direction} ({timeframe}) "
                f"confidence={result['confidence']} atr={result.get('atr')} "
                f"entry={result['entry']} sl={result['sl']} tp1={result['tp1']} "
                f"support={result.get('support')} resistance={result.get('resistance')}"
            )
            mark_pair_signal_time(pair)
            open_trade(pair, timeframe, market_type, direction, result, signal_id)
            db_insert_signal(pair, market_type, timeframe, direction, result, signal_id)
            open_auto_trades_for_signal(
                pair, market_type, direction,
                result["entry"], result["sl"], result["tp1"], result["tp2"], result["tp3"],
                signal_id,
            )
            db_log_activity(
                "signal_sent",
                f"Signal #{signal_id} sent: {pair} {direction} ({timeframe}) - confidence {result['confidence']}%",
            )
            send_push_to_all(
                "new_signal_alerts",
                "SmartFX Signal 🚨",
                f"{pair} — {direction}\nConfidence: {result['confidence']}%",
                url=f"dashboard.html?signal={signal_id}",
            )
            signal_sent_this_scan = True

    # Market scanner status: one row per pair, overwritten every scan
    # regardless of whether a signal actually fired, so the dashboard
    # can show what the bot is currently seeing on every watched pair -
    # including a real confidence % for pairs that are being watched
    # but haven't cleared the stricter bar for a full signal yet.
    scan_status = "SIGNAL" if signal_sent_this_scan else "WATCHING"
    scan_confidence = None
    scan_direction = None

    candles_1h = entry_candles_by_tf.get("1h")
    if candles_1h is not None:
        try:
            snapshot = smc_analysis.get_scan_snapshot(candles_1h, trend)
        except Exception as e:
            log_error(f"Scan snapshot error for {pair}: {e}")
            snapshot = None

        if snapshot is not None:
            scan_confidence = snapshot["confidence"]
            scan_direction = snapshot["direction"]
            if not signal_sent_this_scan:
                # snapshot["status"] is one of "signal"/"watching"/"no_setup" -
                # "signal" here means the snapshot qualified but this exact
                # signal didn't actually go out (cooldown, duplicate, or an
                # already-open trade on this pair suppressed it).
                if snapshot["status"] == "signal":
                    scan_status = "SETUP_READY"
                elif snapshot["status"] == "watching":
                    scan_status = "WATCHING"
                else:
                    scan_status = "NO_SETUP"

    db_update_pair_status(pair, market_type, trend, scan_status, scan_confidence, scan_direction)


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
    global last_forex_scan_time, forex_scan_heartbeat

    if not is_forex_open():
        log_info("Forex market closed, skipping forex scan.")
        return

    now = time.time()
    with state_lock:
        elapsed = now - last_forex_scan_time

    # DIAGNOSTIC: logs the gate state on every single call, not just
    # when a scan actually proceeds. Signal #0058 revealed a real
    # incident where forex scanning silently stopped advancing for
    # over an hour with nothing at all logged - no error, no "market
    # closed" message, nothing - which made the cause impossible to
    # pin down after the fact. If elapsed stops growing the way it
    # should here, this line makes that visible immediately instead of
    # having to guess from an absence of logs.
    log_info(
        f"[FOREX-SCAN-GATE] elapsed={elapsed:.0f}s "
        f"threshold={FOREX_SCAN_INTERVAL_SECONDS}s "
        f"last_scan={datetime.utcfromtimestamp(last_forex_scan_time).isoformat() if last_forex_scan_time else 'never'}"
    )

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

    # Only updated here, once a full pass over every FOREX_PAIRS entry
    # actually completes - deliberately separate from
    # loop_heartbeats["analysis"], which updates every cycle regardless
    # of whether forex did anything, so it can never catch a forex-only
    # stall on its own (crypto succeeding every cycle keeps that
    # heartbeat looking healthy no matter what forex is doing).
    with state_lock:
        forex_scan_heartbeat = time.time()


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

    active_count = count_active_risk_trades()

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

    active_count = count_active_risk_trades()

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


def build_weekly_public_update():
    with state_lock:
        stats = dict(global_stats)

    total_trades = stats["wins"] + stats["losses"]
    win_rate = round((stats["wins"] / total_trades) * 100, 1) if total_trades else 0
    best_pair, _ = get_best_worst_pairs()
    total_pairs = len(CRYPTO_PAIRS) + len(FOREX_PAIRS)

    return (
        f"📢 *{BOT_NAME} - Weekly Update*\n\n"
        f"This week the bot scanned {total_pairs} pairs across crypto and forex.\n\n"
        f"📊 Total Signals: {stats['signals_sent']}\n"
        f"🏆 Win Rate: {win_rate}%\n"
        f"⭐ Best Performing Pair: {best_pair}\n\n"
        "Thanks for following along - see you next week! 🚀"
    )


def send_weekly_public_update():
    sent = send_public_signal(build_weekly_public_update())
    if sent:
        log_info("Weekly public update posted to channel.")
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
        db_update_bot_status()
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
    sent_weekly_public_on = None
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

            # Weekly PUBLIC update: same day, staggered 10 minutes later
            # so it doesn't compete with the private weekly summary send.
            if (
                now.weekday() == 6
                and now.hour == 21
                and 10 <= now.minute < 12
                and sent_weekly_public_on != today
            ):
                send_weekly_public_update()
                sent_weekly_public_on = today

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
                send_push_to_all(
                    "bot_status_alerts",
                    "SmartFX Bot ⚠️",
                    f"The {name} loop hasn't updated in {minutes} minutes.",
                    url="dashboard.html",
                )
                already_alerted.add(name)

            elif not stalled and name in already_alerted:
                send_push_to_all(
                    "bot_status_alerts",
                    "SmartFX Bot 🟢",
                    f"The {name} loop is back to normal.",
                    url="dashboard.html",
                )
                already_alerted.discard(name)

        # Forex-specific stall check, separate from the loop above.
        # loop_heartbeats["analysis"] updates every cycle regardless of
        # whether forex actually did anything that cycle, so it can
        # never catch a forex-only stall on its own - crypto succeeding
        # every cycle keeps that heartbeat looking perfectly healthy no
        # matter what forex is doing (this is exactly what happened
        # with Signal #0058: forex went quiet for over an hour with the
        # general analysis loop never once flagging it). Only checked
        # while the forex market is actually open, since it's expected
        # to go quiet on its own otherwise.
        with state_lock:
            forex_last = forex_scan_heartbeat

        if is_forex_open() and forex_last:
            forex_stalled = (now - forex_last) > FOREX_SCAN_STALL_SECONDS

            if forex_stalled and "forex_scan" not in already_alerted:
                minutes = int((now - forex_last) / 60)
                log_error(f"[FOREX-SCAN-STALLED] No completed forex scan in {minutes}m.")
                send_private_message(
                    f"⚠️ Warning: forex scanning hasn't completed a cycle in {minutes} "
                    "minutes even though the forex market is open. New forex signals "
                    "and same-candle trade monitoring may be degraded - worth checking "
                    "Render logs or restarting the bot."
                )
                already_alerted.add("forex_scan")

            elif not forex_stalled and "forex_scan" in already_alerted:
                already_alerted.discard("forex_scan")

        # News Engine health check - purely observational. This reads
        # a heartbeat written independently by news_engine.py (a
        # completely separate module with its own DB connection) and
        # reuses the same alert/recovery pattern as everything else
        # here. It has no other connection to news_engine.py at all -
        # it cannot affect it, and news_engine.py cannot affect the
        # signal pipeline either way.
        if DATABASE_URL:
            try:
                conn = get_db_connection()
                with conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT last_heartbeat_at, status_message FROM news_engine_state WHERE id = 1")
                        row = cur.fetchone()
                conn.close()

                if row and row[0]:
                    news_last, news_status = row
                    news_stalled = (datetime.utcnow() - news_last.replace(tzinfo=None)).total_seconds() > NEWS_ENGINE_STALL_SECONDS

                    if news_stalled and "news_engine" not in already_alerted:
                        log_error(f"[NEWS-ENGINE-STALLED] No heartbeat since {news_last} (last status: {news_status})")
                        send_private_message(
                            "⚠️ NEWS SYSTEM WARNING\n"
                            "News Opportunity Detection is currently unavailable.\n"
                            "The main trading strategy is still running normally.\n"
                            "Please check the news service/API."
                        )
                        already_alerted.add("news_engine")

                    elif not news_stalled and "news_engine" in already_alerted:
                        send_private_message(
                            "✅ NEWS SYSTEM RESTORED\n"
                            "News Opportunity Detection is working normally again."
                        )
                        already_alerted.discard("news_engine")
            except Exception as e:
                log_error(f"News Engine health check failed: {e}")


def start_background_threads():
    global _threads_started

    if _threads_started:
        return

    _threads_started = True

    check_env()
    init_db()
    seed_signal_id_counter()
    resume_open_trades_from_db()

    threading.Thread(target=analysis_loop, daemon=True).start()
    threading.Thread(target=trade_monitor_loop, daemon=True).start()
    threading.Thread(target=daily_summary_loop, daemon=True).start()
    threading.Thread(target=watchdog_loop, daemon=True).start()
    news_engine.start_news_engine_thread()

    log_info(f"Background threads started: analysis, trade monitor, daily summary, watchdog, news engine. ({BOT_NAME} v{VERSION} / smc_analysis v{smc_analysis.SMC_VERSION})")
    db_log_activity("bot_restart", f"Bot started ({BOT_NAME} v{VERSION} / smc_analysis v{smc_analysis.SMC_VERSION})")

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
