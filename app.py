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
import math
import random
from datetime import datetime, timedelta

from flask import Flask, jsonify
import requests
from PIL import Image, ImageDraw, ImageFont, ImageFilter
import psycopg
from dotenv import load_dotenv

load_dotenv()

try:
    from pywebpush import webpush, WebPushException
    PUSH_AVAILABLE = True
except ImportError:
    PUSH_AVAILABLE = False

import smc_analysis_v2
import smc_analysis_v3
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
# 2.19.0 - Forex Auto Bot lot sizing fix: replaced the balance/10000
#          starting-lot + shrink-to-fit approach (which could skip valid
#          trades or behave unlike true risk% sizing) with pure
#          risk-based lot calculation: lot = floor(
#          (balance * risk%) / dollar_risk_per_1_lot ) to the symbol's
#          volume_step, using SmartFX's existing Entry/SL only. If the
#          safe lot is below the symbol min_volume, the trade is skipped
#          for that user rather than forcing the minimum (which would
#          exceed the configured risk). Crypto sizing unchanged.
# 2.30.0 - Strategy-specific cooldown/active-trade; dual portfolio balances.
# 2.29.1 - Strategy switch control: refresh flags before each analysis cycle;
#          dashboard verifies bot_status write persistence.
# 2.29.0 - Stage F10: strategy_version on auto_trades + resume recovery.
# 2.28.0 - Stage F9: strategy-aware Telegram (signals/captions/reports).
# 2.27.0 - Stage F8: normalized signal contract; mandatory strategy_version;
#          core level validation before shared emit.
# 2.26.0 - Stage F7: shared scanner executes V2 then V3 independently
#          in one loop; isolated try/except; V3 still OFF by default.
# 2.25.0 - Stage F6: shared OHLC cache for V3 4h/1h/15m/5m pipeline;
#          get_candles reuses cache; V3 bundle failure isolated from V2.
#          V3 remains OFF via is_strategy_enabled. No strategy logic change.
# 2.24.0 - Stage F5: persistent strategy control on bot_status
#          (strategy_v2_enabled / strategy_v3_enabled). Defaults
#          V2=ON V3=OFF. is_strategy_enabled(); fail-safe never
#          enables V3 on read error. ENABLE_V3 constant removed.
# 2.23.0 - Stage F4: strategy-aware last_signals (pair_TF_strategy);
#          is_duplicate/store_last_signal take strategy_version.
#          Pair cooldown + active-trade remain pair-wide (V2 safety).
#          ENABLE_V3 remains False. No dual open risk auto-allow.
# 2.22.0 - Stage F3: connect smc_analysis_v3 as second engine behind
#          ENABLE_V3=False (default OFF). Thin adapter fetches 4h/1h/15m/5m
#          and calls analyze_v3; V2 path unchanged. V3 signal timeframe="5m".
# 2.21.0 - Stage F2: shared 5m candle support (Kraken + TwelveData maps)
#          for future V3 use only — V3 not connected. db_insert_signal now
#          writes strategy_version (V2 path passes "V2") so new signals are
#          never NULL. V2 analyze_candles path and Auto Bot unchanged.
# 2.20.0 - Forex Auto Bot: when the risk-based lot floors below the
#          broker min_volume (0.01), use 0.01 instead of skipping — as
#          long as the account balance can cover the dollar risk of
#          that minimum lot. Still respects volume_step and max_volume.
#          Crypto sizing unchanged; SmartFX Entry/SL/TP still pass-through.
VERSION = "2.36.0"
BOT_NAME = "SmartFX Signal Bot"
PUBLIC_MODE = True

# ==========================================================
# PERSISTENT STRATEGY CONTROL (Stage F5)
# ==========================================================
# Source of truth: bot_status.strategy_v2_enabled / strategy_v3_enabled
# Defaults (and fail-safe if DB unread): V2=ON, V3=OFF.
# Disabling a strategy only blocks NEW signals — never closes/deletes
# existing trades or stops outcome monitoring.
# Dashboard switches are a later Stage F step.

_STRATEGY_DEFAULTS = {"V2": True, "V3": False}
_strategy_enabled_cache = {"V2": True, "V3": False}
_strategy_cache_loaded = False


def _normalize_strategy_version(strategy_version):
    s = str(strategy_version or "").strip().upper()
    if s in ("V2", "2"):
        return "V2"
    if s in ("V3", "3"):
        return "V3"
    return s


def refresh_strategy_flags_from_db():
    """Load strategy ON/OFF from bot_status. Fail-safe never enables V3 on error."""
    global _strategy_enabled_cache, _strategy_cache_loaded
    flags = dict(_STRATEGY_DEFAULTS)
    if not DATABASE_URL:
        _strategy_enabled_cache = flags
        _strategy_cache_loaded = True
        return flags
    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT strategy_v2_enabled, strategy_v3_enabled FROM bot_status WHERE id = 1"
                )
                row = cur.fetchone()
        conn.close()
        if row is not None:
            v2, v3 = row[0], row[1]
            if v2 is not None:
                flags["V2"] = bool(v2)
            if v3 is not None:
                flags["V3"] = bool(v3)
    except Exception as e:
        log_error(f"Strategy flags read failed (fail-safe V2=ON V3=OFF): {e}")
        flags = dict(_STRATEGY_DEFAULTS)
    _strategy_enabled_cache = flags
    _strategy_cache_loaded = True
    log_info(
        "Strategy flags loaded: V2=%s V3=%s"
        % ("ON" if flags.get("V2") else "OFF", "ON" if flags.get("V3") else "OFF")
    )
    return flags



def _pair_stat_key(pair, strategy_version="V2"):
    sv = _normalize_strategy_version(strategy_version) or "V2"
    return "%s_%s" % (pair, sv)


def _daily_bucket(strategy_version="V2"):
    sv = _normalize_strategy_version(strategy_version) or "V2"
    if sv not in daily_stats:
        daily_stats[sv] = _empty_daily_bucket()
    return daily_stats[sv]


def _global_bucket(strategy_version="V2"):
    sv = _normalize_strategy_version(strategy_version) or "V2"
    if sv not in global_stats:
        global_stats[sv] = _empty_global_bucket()
    return global_stats[sv]


def _record_signal_stats(pair, direction, market_type, timeframe, strategy_version="V2"):
    sv = _normalize_strategy_version(strategy_version) or "V2"
    with state_lock:
        d = _daily_bucket(sv)
        g = _global_bucket(sv)
        g["signals_sent"] += 1
        d["signals"] += 1
        d["timeframe_counts"][timeframe] = d["timeframe_counts"].get(timeframe, 0) + 1
        if direction and "BUY" in str(direction).upper():
            d["buy_count"] += 1
        else:
            d["sell_count"] += 1
        if market_type == "crypto":
            g["crypto_signals"] += 1
            d["crypto_signals"] += 1
        else:
            g["forex_signals"] += 1
            d["forex_signals"] += 1
        key = _pair_stat_key(pair, sv)
        st = pair_stats.setdefault(key, {"signals": 0, "wins": 0, "losses": 0, "expired": 0})
        st["signals"] = st.get("signals", 0) + 1


def _record_outcome_stats(pair, outcome, strategy_version="V2"):
    sv = _normalize_strategy_version(strategy_version) or "V2"
    with state_lock:
        d = _daily_bucket(sv)
        g = _global_bucket(sv)
        if outcome == "WIN":
            d["wins"] += 1
            g["wins"] += 1
        elif outcome == "LOSS":
            d["losses"] += 1
            g["losses"] += 1
        elif outcome == "EXPIRED":
            d["expired"] = d.get("expired", 0) + 1
            g["expired"] = g.get("expired", 0) + 1
        key = _pair_stat_key(pair, sv)
        st = pair_stats.setdefault(key, {"signals": 0, "wins": 0, "losses": 0, "expired": 0})
        if outcome == "WIN":
            st["wins"] = st.get("wins", 0) + 1
        elif outcome == "LOSS":
            st["losses"] = st.get("losses", 0) + 1
        elif outcome == "EXPIRED":
            st["expired"] = st.get("expired", 0) + 1


def is_strategy_enabled(strategy_version):
    """Should this strategy generate NEW signals? Default V2 True, V3 False."""
    key = _normalize_strategy_version(strategy_version)
    if key not in ("V2", "V3"):
        return False
    if not _strategy_cache_loaded:
        refresh_strategy_flags_from_db()
    return bool(_strategy_enabled_cache.get(key, _STRATEGY_DEFAULTS.get(key, False)))


# Stage F3: V3 second strategy engine — OFF by default.
# When False, analyze_v3 is never called and no V3 signals are sent/stored.
# Persistent dashboard switches come in a later Stage F step.


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
V3_FOREX_PAIRS = ["EUR/USD", "GBP/USD", "USD/JPY"]  # Gold excluded from V3
V3_CRYPTO_PAIRS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

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
KRAKEN_INTERVAL_MAP = {"4h": 240, "1h": 60, "15m": 15, "5m": 5, "1m": 1}
TWELVEDATA_INTERVAL_MAP = {"4h": "4h", "1h": "1h", "15m": "15min", "5m": "5min", "1m": "1min"}

# Duration (in minutes) each entry-timeframe candle spans - used when
# resolving same-candle SL/TP1 ambiguity, to know how wide a window of
# 1-minute candles to check.
TIMEFRAME_MINUTES = {"1h": 60, "15m": 15, "5m": 5, "1m": 1}

ANALYSIS_LOOP_SECONDS = 60       # full scan every 1 minute (was 5 minutes)
TRADE_MONITOR_SECONDS = 20       # check active trades every 20 seconds (was 1 minute)
SUMMARY_CHECK_SECONDS = 30       # check clock every 30 seconds
TREND_CACHE_SECONDS = 3600       # reuse the 4H trend for 1 hour instead of refetching every scan
# Stage F6: OHLC series cache TTLs (aligned to bar size / existing architecture).
# Used by get_candles for both V2 reuse and V3 multi-TF bundles.
OHLC_CACHE_SECONDS = {
    "4h": 3600,   # same spirit as TREND_CACHE_SECONDS
    "1h": 900,    # 15 minutes — within a 1H bar
    "15m": 300,   # 5 minutes — within a 15M bar
    "5m": 60,     # 1 minute — fresher for V3 trigger TF
    "1m": 30,     # future new V2 Gold (15M→5M→1M); not wired yet
}

PAIR_COOLDOWN_SECONDS = 1800     # minimum gap between ANY two signals for the same pair (30 minutes)
SIGNAL_EXPIRY_SECONDS = {
    "1m": 2 * 3600,  # Gold V2 entry TF
    "5m": 2 * 3600,  # V3 trigger TF (only used if strategy V3 is enabled)
    # Scaled up 50% alongside the SL/TP redesign (TP1 moved from 1.0x to
    # 1.5x ATR - a 50% farther target needs proportionally more time to
    # have a fair chance of being reached before we give up on it).
    "15m": 3 * 3600,   # was 2h
    "1h": 12 * 3600,   # was 8h
}

# How much extra (read-only, non-authoritative) time to keep watching
# an already-expired signal, purely to log whether it would have hit
# TP1/SL if we'd kept waiting. Does not affect the real WIN/LOSS/
# EXPIRED result, which is already final by the time this runs.
EXPIRY_FOLLOWUP_WINDOW_HOURS = 48

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

last_signals = {}      # key: "PAIR_TF_STRATEGY" -> "BUY" | "SELL" (F4 strategy-aware)
last_pair_signal_time = {}   # key: pair -> unix timestamp of last signal sent (any timeframe)
active_trades = {}     # key: trade_id -> trade dict
pair_stats = {}        # key: "PAIR_STRATEGY" -> counters
trend_cache = {}       # key: "PAIR_MARKETTYPE" -> {"trend": str, "fetched_at": float}
forex_price_cache = {} # key: pair -> {"price": float, "fetched_at": float}
forex_candle_cache = {} # key: "PAIR_TIMEFRAME" -> list of recent candles (for high/low-based monitoring)
ohlc_cache = {}  # Stage F6: key "PAIR_TF_MARKET" -> {"candles": list, "fetched_at": float}

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

def _empty_daily_bucket():
    return {
        "signals": 0, "buy_count": 0, "sell_count": 0,
        "wins": 0, "losses": 0, "expired": 0,
        "crypto_signals": 0, "forex_signals": 0,
        "timeframe_counts": {"15m": 0, "1h": 0, "5m": 0},
    }

def _empty_global_bucket():
    return {
        "signals_sent": 0, "crypto_signals": 0, "forex_signals": 0,
        "wins": 0, "losses": 0, "expired": 0, "errors": 0,
    }

# Strategy-specific stats — V2 and V3 never share counters.
daily_stats = {"V2": _empty_daily_bucket(), "V3": _empty_daily_bucket()}
global_stats = {"V2": _empty_global_bucket(), "V3": _empty_global_bucket()}
_ops_errors = 0

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


# Robot mascot assets live in a dedicated folder, not embedded in
# source - this is deliberate: the artwork can be swapped out later
# (a redesign, a different pose, etc.) without touching any code here.
ASSETS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets")

_mascot_cache = {}


def load_mascot(outcome):
    """
    Loads the WIN or LOSS mascot image from the assets folder, based
    on the exact same `outcome` value already used everywhere else in
    generate_result_card() - this is not a second win/loss system,
    just a filename lookup keyed off the existing result.

    Checks assets/<file> first (the intended location), then falls
    back to the repo root (where these two files actually ended up
    on the first deploy) - so it works with what's already live
    without needing another git push just to move two files.

    Returns an RGBA PIL Image, or None if neither location has the
    file or it's unreadable - callers must treat None as "skip the
    mascot, render the card exactly as before", never as an error to
    surface to the user. Results are cached in memory since the same
    two files are reused for every card - avoids re-reading disk on
    every trade.
    """
    filename = "smartfx_win.png" if outcome == "WIN" else "smartfx_loss.png"

    if filename in _mascot_cache:
        return _mascot_cache[filename]

    repo_root = os.path.dirname(os.path.abspath(__file__))
    candidate_paths = [
        os.path.join(ASSETS_DIR, filename),   # intended: assets/<file>
        os.path.join(repo_root, filename),     # fallback: repo root
    ]

    for path in candidate_paths:
        try:
            mascot = Image.open(path).convert("RGBA")
            _mascot_cache[filename] = mascot
            return mascot
        except Exception:
            continue

    log_error(f"Could not load mascot asset '{filename}' from assets/ or repo root")
    _mascot_cache[filename] = None
    return None


def _card_font(size, bold=False):
    path = (
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold
        else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
    )
    try:
        return ImageFont.truetype(path, size)
    except Exception:
        return ImageFont.load_default()


def _card_glow_border(img, color, radius=36, thickness=3, glow_blur=18, glow_alpha=140):
    """Soft glow behind a crisp border, simulating a neon outline
    without needing a real glow-capable renderer."""
    glow_layer = Image.new("RGBA", img.size, (0, 0, 0, 0))
    gd = ImageDraw.Draw(glow_layer)
    gd.rounded_rectangle([10, 10, img.size[0] - 11, img.size[1] - 11],
                          radius=radius, outline=color + (glow_alpha,), width=10)
    glow_layer = glow_layer.filter(ImageFilter.GaussianBlur(glow_blur))
    img.alpha_composite(glow_layer)

    crisp = ImageDraw.Draw(img)
    crisp.rounded_rectangle([10, 10, img.size[0] - 11, img.size[1] - 11],
                             radius=radius, outline=color + (255,), width=thickness)


def _card_glow_text(img, draw, pos, text, font, fill, glow_color, blur=10, glow_alpha=180):
    """Text with a soft colored glow behind it - fakes the neon-heading
    look from the reference design."""
    glow_layer = Image.new("RGBA", img.size, (0, 0, 0, 0))
    gd = ImageDraw.Draw(glow_layer)
    gd.text(pos, text, font=font, fill=glow_color + (glow_alpha,))
    glow_layer = glow_layer.filter(ImageFilter.GaussianBlur(blur))
    img.alpha_composite(glow_layer)
    draw.text(pos, text, font=font, fill=fill)


# ---- Custom vector icons, deliberately NOT emoji - the server's font
# has no emoji glyphs (confirmed separately: they render as empty
# boxes), so every icon here is drawn from lines/circles/polygons,
# which renders identically regardless of what fonts exist on the
# host. ----

def _icon_badge(draw, cx, cy, r, color):
    draw.ellipse([cx - r, cy - r, cx + r, cy + r], outline=color + (255,), width=3)


def _icon_checkmark(draw, cx, cy, r, color):
    _icon_badge(draw, cx, cy, r, color)
    draw.line([cx - r * 0.45, cy, cx - r * 0.1, cy + r * 0.4], fill=color + (255,), width=5)
    draw.line([cx - r * 0.1, cy + r * 0.4, cx + r * 0.5, cy - r * 0.35], fill=color + (255,), width=5)


def _icon_warning(draw, cx, cy, r, color):
    pts = [(cx, cy - r), (cx - r * 0.95, cy + r * 0.8), (cx + r * 0.95, cy + r * 0.8)]
    draw.polygon(pts, outline=color + (255,), width=3)
    draw.line([cx, cy - r * 0.3, cx, cy + r * 0.15], fill=color + (255,), width=4)
    draw.ellipse([cx - 2.5, cy + r * 0.4, cx + 2.5, cy + r * 0.45 + 5], fill=color + (255,))


def _icon_coin(draw, cx, cy, r, color, font_small):
    _icon_badge(draw, cx, cy, r, color)
    draw.text((cx, cy), "$", font=font_small, fill=color + (255,), anchor="mm")


def _icon_target(draw, cx, cy, r, color):
    _icon_badge(draw, cx, cy, r, color)
    draw.ellipse([cx - r * 0.55, cy - r * 0.55, cx + r * 0.55, cy + r * 0.55], outline=color + (255,), width=2)
    draw.ellipse([cx - r * 0.18, cy - r * 0.18, cx + r * 0.18, cy + r * 0.18], fill=color + (255,))


def _icon_clock(draw, cx, cy, r, color):
    _icon_badge(draw, cx, cy, r, color)
    draw.line([cx, cy, cx, cy - r * 0.5], fill=color + (255,), width=3)
    draw.line([cx, cy, cx + r * 0.35, cy + r * 0.15], fill=color + (255,), width=3)


def _icon_link(draw, cx, cy, r, color):
    draw.rounded_rectangle([cx - r * 0.9, cy - r * 0.35, cx + r * 0.1, cy + r * 0.35],
                            radius=int(r * 0.35), outline=color + (255,), width=3)
    draw.rounded_rectangle([cx - r * 0.1, cy - r * 0.35, cx + r * 0.9, cy + r * 0.35],
                            radius=int(r * 0.35), outline=color + (255,), width=3)


def _icon_arrow(draw, cx, cy, r, color, up=True):
    if up:
        pts = [(cx, cy - r), (cx - r * 0.6, cy + r * 0.4), (cx + r * 0.6, cy + r * 0.4)]
    else:
        pts = [(cx, cy + r), (cx - r * 0.6, cy - r * 0.4), (cx + r * 0.6, cy - r * 0.4)]
    draw.polygon(pts, fill=color + (255,))


def _icon_trophy(draw, cx, cy, r, color):
    bowl = [
        (cx - r * 0.55, cy - r * 0.6), (cx + r * 0.55, cy - r * 0.6),
        (cx + r * 0.25, cy + r * 0.15), (cx - r * 0.25, cy + r * 0.15),
    ]
    draw.polygon(bowl, outline=color + (255,), width=3)
    draw.line([cx, cy + r * 0.15, cx, cy + r * 0.45], fill=color + (255,), width=3)
    draw.line([cx - r * 0.35, cy + r * 0.5, cx + r * 0.35, cy + r * 0.5], fill=color + (255,), width=3)


def _icon_shield(draw, cx, cy, r, color):
    pts = [(cx, cy - r), (cx + r * 0.8, cy - r * 0.5), (cx + r * 0.8, cy + r * 0.2),
           (cx, cy + r), (cx - r * 0.8, cy + r * 0.2), (cx - r * 0.8, cy - r * 0.5)]
    draw.polygon(pts, outline=color + (255,), width=3)


def _draw_candlestick_bg(img, x0, y0, x1, y1, color, rng, count=9):
    layer = Image.new("RGBA", img.size, (0, 0, 0, 0))
    d = ImageDraw.Draw(layer)
    step = (x1 - x0) / count
    for i in range(count):
        cx = x0 + step * i + step / 2
        h = rng.uniform(0.25, 1.0) * (y1 - y0)
        top = rng.uniform(y0, y1 - h)
        w = step * 0.4
        alpha = rng.randint(30, 60)
        d.rectangle([cx - w / 2, top, cx + w / 2, top + h], fill=color + (alpha,))
        d.line([cx, top - 12, cx, top + h + 12], fill=color + (alpha,), width=2)
    img.alpha_composite(layer)


def _draw_confetti(img, x0, y0, x1, y1, colors, rng, density):
    layer = Image.new("RGBA", img.size, (0, 0, 0, 0))
    d = ImageDraw.Draw(layer)
    for _ in range(density):
        x, y = rng.uniform(x0, x1), rng.uniform(y0, y1)
        c = rng.choice(colors)
        size = rng.uniform(4, 10)
        if rng.random() < 0.5:
            d.rectangle([x, y, x + size, y + size * 0.4], fill=c + (rng.randint(160, 230),))
        else:
            d.ellipse([x, y, x + size * 0.6, y + size * 0.6], fill=c + (rng.randint(160, 230),))
    img.alpha_composite(layer)


def _draw_falling_shards(img, x0, y0, x1, y1, color, rng, density):
    layer = Image.new("RGBA", img.size, (0, 0, 0, 0))
    d = ImageDraw.Draw(layer)
    for _ in range(density):
        x, y = rng.uniform(x0, x1), rng.uniform(y0, y1)
        length = rng.uniform(14, 30)
        angle = rng.uniform(60, 80)
        dx = length * math.cos(math.radians(angle))
        dy = length * math.sin(math.radians(angle))
        d.line([x, y, x + dx, y + dy], fill=color + (rng.randint(90, 160),), width=2)
    img.alpha_composite(layer)


def generate_result_card(trade, outcome, final_tp_label=None):
    """
    Generates the branded result card for a finished trade - only
    called for genuinely final outcomes (a LOSS, or a full TP3 win).
    Uses PIL directly (no external image APIs, no extra network calls).

    Redesigned layout (v2) - same function signature and same return
    type (a PNG bytes buffer) as before, so send_result_card() and
    every other caller needs zero changes. WIN/LOSS determination
    itself is untouched: this function only ever reads the `outcome`
    and `final_tp_label` it's given, never decides them.
    """
    rng = random.Random()
    is_win = outcome == "WIN"

    W, H = 1000, 900
    accent = (60, 230, 130) if is_win else (235, 70, 80)
    bg_top = (8, 16, 12) if is_win else (18, 8, 9)
    bg_bottom = (4, 8, 6) if is_win else (10, 4, 5)

    img = Image.new("RGBA", (W, H), (0, 0, 0, 255))
    for y in range(H):
        t = y / H
        r = int(bg_top[0] + (bg_bottom[0] - bg_top[0]) * t)
        g = int(bg_top[1] + (bg_bottom[1] - bg_top[1]) * t)
        b = int(bg_top[2] + (bg_bottom[2] - bg_top[2]) * t)
        ImageDraw.Draw(img).line([(0, y), (W, y)], fill=(r, g, b, 255))

    _draw_candlestick_bg(img, 640, 160, 960, 520, accent, rng, count=9)

    draw = ImageDraw.Draw(img)
    signal_id = trade.get("signal_id", "N/A")

    # ---- Header ----
    _icon_badge(draw, 90, 95, 40, accent)
    draw.text((90, 95), "SFX", font=_card_font(22, bold=True), fill=accent + (255,), anchor="mm")
    draw.text((150, 75), "SmartFX", font=_card_font(34, bold=True), fill=(240, 240, 240, 255))
    _sv = trade.get("strategy_version")
    _sv_n = _normalize_strategy_version(_sv) if _sv else None
    if _sv_n in ("V2", "V3"):
        draw.text((150, 118), f"Signal #{signal_id}  ·  {_sv_n}", font=_card_font(20), fill=(150, 150, 150, 255))
    else:
        draw.text((150, 118), f"Signal #{signal_id}", font=_card_font(20), fill=(150, 150, 150, 255))

    # ---- Big heading with glow ----
    heading = "TRADE WON" if is_win else "STOP LOSS"
    heading_font = _card_font(64, bold=True)
    _card_glow_text(img, draw, (60, 190), heading, heading_font, accent + (255,), accent, blur=14, glow_alpha=160)

    icon_x = 60 + draw.textlength(heading, font=heading_font) + 45
    if is_win:
        _icon_checkmark(draw, icon_x, 222, 28, accent)
    else:
        _icon_warning(draw, icon_x, 222, 28, accent)

    sub = "TARGET REACHED" if is_win else "TRADE CLOSED"
    sub_font = _card_font(20, bold=True)
    draw.line([60, 280, 130, 280], fill=accent + (150,), width=2)
    draw.text((145, 268), sub, font=sub_font, fill=accent + (200,))
    sub_w = draw.textlength(sub, font=sub_font)
    draw.line([155 + sub_w, 280, 225 + sub_w, 280], fill=accent + (150,), width=2)

    # ---- Pair / direction / entry / target rows - all real trade
    # data, nothing hard-coded. ----
    row_y = 350
    _icon_badge(draw, 90, row_y, 26, accent)
    draw.text((90, row_y), trade["pair"][:1], font=_card_font(20, bold=True), fill=accent + (255,), anchor="mm")
    draw.text((135, row_y - 22), trade["pair"], font=_card_font(38, bold=True), fill=(245, 245, 245, 255))

    row_y = 415
    _icon_arrow(draw, 90, row_y, 14, accent, up=(trade["direction"] == "BUY"))
    dir_font = _card_font(24, bold=True)
    draw.text((120, row_y - 15), trade["direction"], font=dir_font, fill=accent + (255,))
    dir_w = draw.textlength(trade["direction"], font=dir_font)
    draw.text((130 + dir_w, row_y - 15), f"| {trade['timeframe']}", font=_card_font(24), fill=(190, 190, 190, 255))

    row_y = 480
    _icon_coin(draw, 90, row_y, 22, accent, _card_font(16, bold=True))
    draw.text((130, row_y - 16), f"Entry: {trade['entry']}", font=_card_font(26), fill=(220, 220, 220, 255))

    row_y = 535
    _icon_target(draw, 90, row_y, 22, accent)
    if is_win:
        label_text = "Target Reached: "
        value_text = final_tp_label or "TP"
    else:
        label_text = "Stop Loss: "
        value_text = str(trade.get("sl", ""))
    label_font = _card_font(26)
    draw.text((130, row_y - 16), label_text, font=label_font, fill=(220, 220, 220, 255))
    label_w = draw.textlength(label_text, font=label_font)
    draw.text((130 + label_w, row_y - 16), value_text, font=_card_font(26, bold=True), fill=accent + (255,))

    # ---- Pill-shaped info bar ----
    pill_y0, pill_y1 = 610, 700
    draw.rounded_rectangle([60, pill_y0, W - 60, pill_y1], radius=24, outline=accent + (180,), width=2)

    col_w = (W - 120) / 3
    result_time = datetime.now().strftime("%-I:%M %p")
    cells = [
        ("target", f"Signal #{signal_id}"),
        ("clock", result_time),
        ("link", trade["pair"]),
    ]
    for i, (icon_name, text) in enumerate(cells):
        cx = 60 + col_w * i + 55
        cy = (pill_y0 + pill_y1) // 2
        if icon_name == "target":
            _icon_target(draw, cx, cy, 16, accent)
        elif icon_name == "clock":
            _icon_clock(draw, cx, cy, 16, accent)
        else:
            _icon_link(draw, cx, cy, 16, accent)
        draw.text((cx + 30, cy - 12), text, font=_card_font(20, bold=True), fill=(230, 230, 230, 255))
        if i < 2:
            x_div = 60 + col_w * (i + 1)
            draw.line([x_div, pill_y0 + 15, x_div, pill_y1 - 15], fill=(255, 255, 255, 40), width=1)

    # ---- Footer motivational bar ----
    foot_y0, foot_y1 = 730, 810
    draw.rounded_rectangle([60, foot_y0, W - 60, foot_y1], radius=20, outline=accent + (120,), width=2)

    if is_win:
        messages = {
            "TP1": "First target secured.\nKeep focused.",
            "TP2": "Momentum confirmed.\nStay disciplined.",
            "TP3": "Full target achieved.\nExcellent execution!",
        }
        msg = messages.get(final_tp_label, "Target reached.\nWell played.")
        _icon_trophy(draw, 100, (foot_y0 + foot_y1) // 2, 22, accent)
    else:
        # Fixed message, per spec - not randomized per loss.
        msg = "Loss accepted.\nDiscipline comes first."
        _icon_shield(draw, 100, (foot_y0 + foot_y1) // 2, 22, accent)

    lines = msg.split("\n")
    line_h = 26
    start_y = (foot_y0 + foot_y1) // 2 - (len(lines) * line_h) // 2
    for i, line in enumerate(lines):
        draw.text((135, start_y + i * line_h), line, font=_card_font(22, bold=True), fill=accent + (255,))

    # ---- Mascot ----
    # Reuses the exact same load_mascot(outcome) already in place -
    # same assets/-then-root-fallback lookup, same fail-safe (a
    # missing/broken file just means no mascot, never a crash).
    # Sized and positioned so it CANNOT reach the text column on the
    # left or down into the pill bar - verified against real text
    # widths during testing, not just eyeballed.
    try:
        mascot = load_mascot(outcome)
        if mascot is not None:
            target_h = 380
            ratio = target_h / mascot.height
            mascot = mascot.resize((int(mascot.width * ratio), target_h), Image.LANCZOS)

            alpha = mascot.getchannel("A").point(lambda a: int(a * 0.92))
            mascot.putalpha(alpha)

            mx = W - mascot.width - 30
            my = 175
            img.alpha_composite(mascot, (mx, my))

            if is_win:
                density = {"TP1": 16, "TP2": 28, "TP3": 44}.get(final_tp_label, 22)
                _draw_confetti(img, mx - 50, my - 15, mx + mascot.width + 35, my + mascot.height + 30,
                                [(60, 230, 130), (255, 210, 60), (255, 255, 255)], rng, density)
            else:
                _draw_falling_shards(img, mx - 35, my - 10, mx + mascot.width + 25, my + mascot.height + 15,
                                      accent, rng, 20)
    except Exception as e:
        log_error(f"Mascot rendering skipped for Signal #{signal_id}: {e}")

    # ---- Closing tagline ----
    tagline = "SmartFX  •  Risk · Plan · Grow"
    tagline_font = _card_font(16)
    tw = draw.textlength(tagline, font=tagline_font)
    draw.text((W - 60 - tw, H - 45), tagline, font=tagline_font, fill=(140, 140, 140, 200))

    _card_glow_border(img, accent)

    buffer = io.BytesIO()
    img.convert("RGB").save(buffer, format="PNG")
    buffer.seek(0)
    return buffer


def send_result_card(trade, outcome, final_tp_label=None):
    try:
        image_bytes = generate_result_card(trade, outcome, final_tp_label=final_tp_label)
    except Exception as e:
        log_error(f"Failed to generate result card for Signal #{trade.get('signal_id')}: {e}")
        return False

    sv = trade.get("strategy_version")
    sv_n = _normalize_strategy_version(sv) if sv else None
    if sv_n in ("V2", "V3"):
        caption = (
            f"Signal #{trade.get('signal_id', 'N/A')} [{sv_n}] - "
            f"{trade['pair']} {trade['direction']} — {outcome}"
        )
    else:
        caption = f"Signal #{trade.get('signal_id', 'N/A')} - {trade['pair']} {trade['direction']} — {outcome}"

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
        _ops_errors += 1


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
                        pair TEXT NOT NULL,
                        strategy_version TEXT NOT NULL DEFAULT 'V2',
                        market_type TEXT NOT NULL,
                        trend TEXT,
                        status TEXT NOT NULL,
                        confidence INTEGER,
                        direction TEXT,
                        updated_at TIMESTAMP NOT NULL,
                        PRIMARY KEY (pair, strategy_version)
                    )
                """)
                try:
                    cur.execute("ALTER TABLE pair_scan_status ADD COLUMN IF NOT EXISTS strategy_version TEXT DEFAULT 'V2'")
                    cur.execute("UPDATE pair_scan_status SET strategy_version = 'V2' WHERE strategy_version IS NULL")
                    # Drop legacy single-column PK so (pair, strategy_version) can coexist
                    cur.execute("""
                        DO $migrate$
                        BEGIN
                          IF EXISTS (
                            SELECT 1 FROM information_schema.table_constraints
                            WHERE table_name = 'pair_scan_status'
                              AND constraint_type = 'PRIMARY KEY'
                              AND constraint_name = 'pair_scan_status_pkey'
                          ) THEN
                            ALTER TABLE pair_scan_status DROP CONSTRAINT pair_scan_status_pkey;
                          END IF;
                          BEGIN
                            ALTER TABLE pair_scan_status
                              ADD CONSTRAINT pair_scan_status_pkey PRIMARY KEY (pair, strategy_version);
                          EXCEPTION WHEN OTHERS THEN
                            NULL;
                          END;
                          BEGIN
                            CREATE UNIQUE INDEX IF NOT EXISTS pair_scan_status_pair_strategy_uidx
                              ON pair_scan_status (pair, strategy_version);
                          EXCEPTION WHEN OTHERS THEN
                            NULL;
                          END;
                        END
                        $migrate$;
                    """)
                except Exception as e:
                    log_error(f"pair_scan_status strategy_version migrate: {e}")
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
                cur.execute("""
                    ALTER TABLE bot_status
                    ADD COLUMN IF NOT EXISTS bot_started_at TIMESTAMPTZ
                """)
                cur.execute("""
                    ALTER TABLE bot_status
                    ADD COLUMN IF NOT EXISTS strategy_v2_enabled BOOLEAN DEFAULT TRUE
                """)
                cur.execute("""
                    ALTER TABLE bot_status
                    ADD COLUMN IF NOT EXISTS strategy_v3_enabled BOOLEAN DEFAULT FALSE
                """)
                cur.execute("""
                    UPDATE bot_status
                    SET strategy_v2_enabled = COALESCE(strategy_v2_enabled, TRUE),
                        strategy_v3_enabled = COALESCE(strategy_v3_enabled, FALSE)
                    WHERE id = 1
                """)
                try:
                    cur.execute("""
                        ALTER TABLE auto_trades
                        ADD COLUMN IF NOT EXISTS strategy_version TEXT
                    """)
                except Exception:
                    pass

                try:
                    cur.execute("""
                        ALTER TABLE portfolio
                        ADD COLUMN IF NOT EXISTS balance_v2 NUMERIC
                    """)
                    cur.execute("""
                        ALTER TABLE portfolio
                        ADD COLUMN IF NOT EXISTS balance_v3 NUMERIC
                    """)
                    # Seed strategy balances from legacy single balance once
                    cur.execute("""
                        UPDATE portfolio
                        SET balance_v2 = COALESCE(balance_v2, balance),
                            balance_v3 = COALESCE(balance_v3, balance)
                        WHERE balance IS NOT NULL
                    """)
                    cur.execute("""
                        ALTER TABLE portfolio_balance_log
                        ADD COLUMN IF NOT EXISTS strategy_version TEXT
                    """)
                except Exception as e:
                    log_error(f"portfolio strategy balance columns: {e}")
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
                        strategy_version TEXT,
                        created_at TIMESTAMP NOT NULL
                    )
                """)
                try:
                    cur.execute("ALTER TABLE activity_log ADD COLUMN IF NOT EXISTS strategy_version TEXT")
                except Exception as e:
                    log_error(f"activity_log strategy_version: {e}")
                try:
                    # Additive AutoBot V2/V3 settings only — do not overwrite live values
                    cur.execute("ALTER TABLE user_settings ADD COLUMN IF NOT EXISTS auto_trading_v2_enabled BOOLEAN DEFAULT FALSE")
                    cur.execute("ALTER TABLE user_settings ADD COLUMN IF NOT EXISTS auto_trading_v3_enabled BOOLEAN DEFAULT FALSE")
                    cur.execute("ALTER TABLE user_settings ADD COLUMN IF NOT EXISTS auto_trading_v2_risk_pct NUMERIC DEFAULT 1.0")
                    cur.execute("ALTER TABLE user_settings ADD COLUMN IF NOT EXISTS auto_trading_v3_risk_pct NUMERIC DEFAULT 1.0")
                except Exception as e:
                    log_error(f"user_settings auto_trading_v2/v3 columns: {e}")
        conn.close()
        log_info("Database ready: 'signals', 'bot_status', 'pair_scan_status', and 'activity_log' tables confirmed/created.")
    except Exception as e:
        log_error(f"Database setup failed (bot will keep running without persistence): {e}")


def ensure_bot_started_at():
    """
    Sets bot_status.bot_started_at to NOW() if and only if it's
    currently NULL. On every later call (i.e. every future restart),
    it's already set, so this does nothing - the real original start
    date is never overwritten by a later redeploy or restart.
    Purely additive: doesn't touch any other column or table.
    """
    if not DATABASE_URL:
        return
    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE bot_status
                    SET bot_started_at = NOW()
                    WHERE id = 1 AND bot_started_at IS NULL
                """)
        conn.close()
    except Exception as e:
        log_error(f"Failed to set bot_started_at: {e}")


def db_insert_signal(pair, market_type, timeframe, direction, result, signal_id, strategy_version="V2", strategy_variant=None):
    """
    Insert a signal row. strategy_version mandatory: "V2" or "V3".
    New Gold V2 rows must pass strategy_variant="GOLD_SCALPING".
    Historical old V2 rows remain strategy_version=V2 with strategy_variant NULL.
    """
    sv = _normalize_strategy_version(strategy_version)
    if sv not in ("V2", "V3"):
        log_error("db_insert_signal refused Signal #%s: invalid strategy_version=%r" % (signal_id, strategy_version))
        return
    strategy_version = sv
    # Prefer explicit arg; else from result mapping
    variant = strategy_variant
    if variant is None and isinstance(result, dict):
        variant = result.get("strategy_variant")
    if variant is not None:
        variant = str(variant).strip() or None
    if not DATABASE_URL:
        return

    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                factors = result.get("factors") or result.get("confirmation_factors")
                factors_json = None
                if factors is not None:
                    try:
                        factors_json = json.dumps(factors)
                    except Exception:
                        factors_json = None
                cur.execute("""
                    INSERT INTO signals
                        (signal_id, pair, market_type, timeframe, direction,
                         entry, sl, tp1, tp2, tp3, confidence, risk,
                         atr_at_signal, confirmation_factors, status, sent_at,
                         strategy_version, strategy_variant)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, 'OPEN', %s,
                            %s, %s)
                    ON CONFLICT (signal_id) DO NOTHING
                """, (
                    signal_id, pair, market_type, timeframe, direction,
                    result["entry"], result["sl"], result["tp1"], result["tp2"], result["tp3"],
                    result.get("confidence"), result.get("risk"),
                    result.get("atr"), factors_json, datetime.utcnow(),
                    strategy_version, variant,
                ))
        conn.close()
    except Exception as e:
        log_error(f"Failed to insert signal #{signal_id} into database: {e}")



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
                # Attribute outcome activity to the signal's real strategy
                cur.execute(
                    "SELECT strategy_version FROM signals WHERE signal_id = %s",
                    (signal_id,),
                )
                sv_row = cur.fetchone()
                outcome_sv = None
                if sv_row and sv_row[0]:
                    outcome_sv = _normalize_strategy_version(sv_row[0]) or None
                cur.execute("""
                    INSERT INTO activity_log (event_type, message, strategy_version, created_at)
                    VALUES (%s, %s, %s, %s)
                """, (
                    "trade_outcome",
                    f"Signal #{signal_id} → {status}{level_text}",
                    outcome_sv,
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
                """, (datetime.utcnow(), VERSION, "gold-v2"))
        conn.close()
    except Exception as e:
        log_error(f"Failed to update bot_status: {e}")


def db_update_pair_status(pair, market_type, trend, status, confidence=None, direction=None, strategy_version="V2"):
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
                cur.execute("SELECT status FROM pair_scan_status WHERE pair = %s AND COALESCE(strategy_version,'V2') = %s", (pair, _normalize_strategy_version(strategy_version) or "V2"))
                row = cur.fetchone()
                previous_status = row[0] if row else None

                sv = _normalize_strategy_version(strategy_version) or "V2"
                cur.execute("""
                    INSERT INTO pair_scan_status
                        (pair, strategy_version, market_type, trend, status, confidence, direction, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (pair, strategy_version) DO UPDATE
                    SET market_type = EXCLUDED.market_type,
                        trend = EXCLUDED.trend,
                        status = EXCLUDED.status,
                        confidence = EXCLUDED.confidence,
                        direction = EXCLUDED.direction,
                        updated_at = EXCLUDED.updated_at
                """, (pair, sv, market_type, trend, status, confidence, direction, datetime.utcnow()))

                if previous_status is not None and previous_status != status:
                    conf_text = f" ({confidence}%)" if confidence is not None else ""
                    cur.execute("""
                        INSERT INTO activity_log (event_type, message, strategy_version, created_at)
                        VALUES (%s, %s, %s, %s)
                    """, (
                        "scan_status_change",
                        f"{pair} moved from {previous_status} → {status}{conf_text}",
                        sv,
                        datetime.utcnow(),
                    ))
        conn.close()
    except Exception as e:
        log_error(f"Failed to update pair_scan_status for {pair}: {e}")


def db_log_activity(event_type, message, strategy_version=None):
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
                    INSERT INTO activity_log (event_type, message, strategy_version, created_at)
                    VALUES (%s, %s, %s, %s)
                """, (event_type, message,
                      (_normalize_strategy_version(strategy_version) if strategy_version else None),
                      datetime.utcnow()))
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



def get_portfolio_balance_for_strategy(user_id, strategy_version="V2"):
    """Return balance for strategy; falls back to legacy portfolio.balance."""
    sv = _normalize_strategy_version(strategy_version) or "V2"
    col = "balance_v2" if sv == "V2" else "balance_v3"
    if not DATABASE_URL:
        return 0.0
    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT COALESCE({col}, balance, 0) FROM portfolio WHERE user_id = %s",
                    (user_id,),
                )
                row = cur.fetchone()
        conn.close()
        return float(row[0]) if row else 0.0
    except Exception as e:
        log_error(f"get_portfolio_balance_for_strategy: {e}")
        return 0.0


def apply_portfolio_pl(user_id, pl_amount, strategy_version="V2"):
    """Apply P/L to strategy-specific balance; keep legacy balance in sync for V2."""
    sv = _normalize_strategy_version(strategy_version) or "V2"
    col = "balance_v2" if sv == "V2" else "balance_v3"
    if not DATABASE_URL:
        return
    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO portfolio (user_id, balance, balance_v2, balance_v3, updated_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    ON CONFLICT (user_id) DO UPDATE SET updated_at = NOW()
                """, (
                    user_id,
                    pl_amount if sv == "V2" else 0,
                    pl_amount if sv == "V2" else 0,
                    pl_amount if sv == "V3" else 0,
                ))
                # Add P/L to strategy column (and legacy balance for V2)
                if sv == "V2":
                    cur.execute("""
                        UPDATE portfolio
                        SET balance_v2 = COALESCE(balance_v2, balance, 0) + %s,
                            balance = COALESCE(balance, 0) + %s,
                            updated_at = NOW()
                        WHERE user_id = %s
                    """, (pl_amount, pl_amount, user_id))
                else:
                    cur.execute("""
                        UPDATE portfolio
                        SET balance_v3 = COALESCE(balance_v3, 0) + %s,
                            updated_at = NOW()
                        WHERE user_id = %s
                    """, (pl_amount, user_id))
                cur.execute(f"SELECT COALESCE({col}, 0) FROM portfolio WHERE user_id = %s", (user_id,))
                new_balance = cur.fetchone()[0]
                cur.execute("""
                    INSERT INTO portfolio_balance_log (user_id, balance, strategy_version)
                    VALUES (%s, %s, %s)
                """, (user_id, new_balance, sv))
        conn.close()
    except Exception as e:
        log_error(f"apply_portfolio_pl failed: {e}")


def get_auto_trading_users(strategy_version="V2"):
    """
    Returns [(user_id, risk_pct, 0), ...] for users with AutoBot enabled
    for the requested strategy_version only.

    V2 → user_settings.auto_trading_v2_enabled + auto_trading_v2_risk_pct
    V3 → user_settings.auto_trading_v3_enabled + auto_trading_v3_risk_pct

    Does NOT use legacy auto_trading_enabled / auto_trading_risk_pct for
    V2/V3 execution. Strategy-specific balance is loaded later via
    get_portfolio_balance_for_strategy(balance_v2/v3).
    """
    sv = _normalize_strategy_version(strategy_version) or "V2"
    if sv not in ("V2", "V3"):
        return []
    if not DATABASE_URL:
        return []
    enabled_col = "auto_trading_v2_enabled" if sv == "V2" else "auto_trading_v3_enabled"
    risk_col = "auto_trading_v2_risk_pct" if sv == "V2" else "auto_trading_v3_risk_pct"
    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT us.user_id, COALESCE(us.{risk_col}, 1.0), 0  -- NULL column only (unset), not a forced live 1%
                    FROM user_settings us
                    WHERE COALESCE(us.{enabled_col}, false) = true
                """)
                rows = cur.fetchall()
        conn.close()
        return rows
    except Exception as e:
        log_error(f"Failed to fetch auto-trading users for {sv}: {e}")
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
# min_volume / volume_step / max_volume: MT5-style volume constraints.
# Do not force a fixed micro-lot (e.g. 0.001); size from risk%, then
# validate against these rules.
FOREX_CONTRACT_INFO = {
    "EUR/USD": {
        "pip_size": 0.0001, "contract_size": 100000, "quote_is_usd": True,
        "min_volume": 0.01, "volume_step": 0.01, "max_volume": 100.0,
    },
    "GBP/USD": {
        "pip_size": 0.0001, "contract_size": 100000, "quote_is_usd": True,
        "min_volume": 0.01, "volume_step": 0.01, "max_volume": 100.0,
    },
    "USD/JPY": {
        "pip_size": 0.01, "contract_size": 100000, "quote_is_usd": False,
        "min_volume": 0.01, "volume_step": 0.01, "max_volume": 100.0,
    },
    "XAU/USD": {
        "pip_size": 0.01, "contract_size": 100, "quote_is_usd": True,
        "min_volume": 0.01, "volume_step": 0.01, "max_volume": 100.0,
    },
}


def compute_lot_sizing(pair, entry, sl, balance, risk_pct):
    """
    Returns {"lot_size", "position_size", "risk_amount"} for a forex
    pair, or None if this pair isn't forex (caller falls back to the
    existing risk%-only sizing for crypto) or the trade can't be sized
    safely at all.

    Pure risk-based lot sizing using SmartFX's existing Entry and SL
    only — never invents levels:

      risk_amount = balance * (risk_pct / 100)
      dollar_risk_per_1_lot = f(contract_size, |entry - SL|, quote currency)
      raw_lot = risk_amount / dollar_risk_per_1_lot
      lot = floor(raw_lot to volume_step)

    If the risk-safe lot is below broker min_volume, use min_volume only
    when that minimum lot's dollar risk is still within the user's
    risk budget (balance * risk_pct/100). Otherwise skip the trade —
    never increase risk just to meet the broker minimum.
    Still respects volume_step and max_volume.
    """
    info = FOREX_CONTRACT_INFO.get(pair)
    if not info:
        return None

    try:
        entry = float(entry)
        sl = float(sl)
        balance = float(balance)
        risk_pct = float(risk_pct)
    except (TypeError, ValueError):
        return None

    sl_distance = abs(entry - sl)
    if sl_distance <= 0 or balance <= 0 or risk_pct <= 0:
        return None

    contract_size = float(info["contract_size"])
    quote_is_usd = bool(info["quote_is_usd"])
    min_volume = float(info.get("min_volume", 0.01))
    volume_step = float(info.get("volume_step", 0.01))
    max_volume = float(info.get("max_volume", 100.0))

    if volume_step <= 0 or min_volume <= 0:
        return None

    def dollar_risk_for_lot(lot):
        units = lot * contract_size
        risk_in_quote_ccy = units * sl_distance
        return risk_in_quote_ccy if quote_is_usd else risk_in_quote_ccy / entry

    risk_ceiling = balance * (risk_pct / 100.0)
    risk_per_full_lot = dollar_risk_for_lot(1.0)
    if risk_per_full_lot <= 0:
        return None

    # Ideal lot from risk budget, then floor to a valid volume step.
    raw_lot = risk_ceiling / risk_per_full_lot
    steps = math.floor(raw_lot / volume_step + 1e-12)
    lot = round(steps * volume_step, 8)

    # Below broker minimum: use min_volume if the account can cover
    # the dollar risk of that minimum lot; otherwise skip.
    if lot < min_volume:
        min_lot_risk = dollar_risk_for_lot(min_volume)
        # Never force min lot if it would exceed the user's risk budget
        if min_lot_risk > risk_ceiling + 1e-9:
            return None
        # Also require balance can cover that risk
        if min_lot_risk > balance + 1e-9:
            return None
        lot = min_volume

    if lot > max_volume:
        lot = max_volume

    # Final guard: floor/step rounding must not exceed risk budget
    final_risk = dollar_risk_for_lot(lot)
    if final_risk > risk_ceiling + 1e-6:
        # Step down one volume_step if possible
        stepped = round(max(lot - volume_step, 0.0), 8)
        if stepped >= min_volume and dollar_risk_for_lot(stepped) <= risk_ceiling + 1e-6:
            lot = stepped
            final_risk = dollar_risk_for_lot(lot)
        else:
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



# ==========================================================
# GOLD V2 MANAGEMENT HELPERS (lifecycle + DB sync + idempotent P/L)
# ==========================================================

def _is_gold_v2_trade(trade_or_pair, strategy_version=None, strategy_variant=None):
    if isinstance(trade_or_pair, dict):
        t = trade_or_pair
        return (
            t.get("strategy_variant") == "GOLD_SCALPING"
            or (
                _normalize_strategy_version(t.get("strategy_version")) == "V2"
                and t.get("pair") == GOLD_V2_PAIR
            )
        )
    return (
        strategy_variant == "GOLD_SCALPING"
        or (
            _normalize_strategy_version(strategy_version) == "V2"
            and trade_or_pair == GOLD_V2_PAIR
        )
    )


def db_update_auto_trade_management(signal_id, **fields):
    if not DATABASE_URL or not signal_id:
        return
    allowed = {
        "management_stage", "outcome_type", "tp1_hit_at", "protected_stop",
        "tp1_realized_pl", "tp1_pl_applied", "final_pl_applied", "partial_pct",
        "strategy_variant", "status", "final_level", "pl_amount",
    }
    sets, vals = [], []
    for k, v in fields.items():
        if k not in allowed:
            continue
        sets.append("%s = %%s" % k)
        vals.append(v)
    if not sets:
        return
    vals.append(signal_id)
    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE auto_trades SET %s WHERE signal_id = %%s AND status = 'OPEN'"
                    % (", ".join(sets),),
                    tuple(vals),
                )
        conn.close()
    except Exception as e:
        log_error("db_update_auto_trade_management failed for %s: %s" % (signal_id, e))


def compute_gold_tp1_partial_pl(direction, entry, tp1, position_size, partial_pct=None):
    pct = float(partial_pct if partial_pct is not None else GOLD_V2_PARTIAL_PCT)
    pct = max(0.0, min(100.0, pct)) / 100.0
    size = float(position_size) * pct
    entry, tp1 = float(entry), float(tp1)
    if "BUY" in str(direction).upper():
        return size * (tp1 - entry)
    return size * (entry - tp1)


def apply_gold_tp1_partial_once(signal_id, trade, user_rows=None):
    """Idempotent TP1 partial P/L via tp1_pl_applied flag."""
    if not DATABASE_URL or not signal_id:
        return 0.0
    partial_pct = float(trade.get("partial_pct") or GOLD_V2_PARTIAL_PCT)
    total_applied = 0.0
    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, user_id, direction, entry, tp1, position_size,
                           COALESCE(tp1_pl_applied, false), strategy_version
                    FROM auto_trades
                    WHERE signal_id = %s AND status = 'OPEN'
                """, (signal_id,))
                for row in cur.fetchall():
                    at_id, user_id, direction, entry, tp1, pos, already, sv = row
                    if already:
                        continue
                    pl = compute_gold_tp1_partial_pl(direction, entry, tp1, float(pos), partial_pct)
                    sv = _normalize_strategy_version(sv) or "V2"
                    cur.execute("""
                        UPDATE auto_trades
                        SET tp1_realized_pl = %s,
                            tp1_pl_applied = TRUE,
                            tp1_hit_at = COALESCE(tp1_hit_at, NOW()),
                            management_stage = 'TP1_PARTIAL',
                            partial_pct = %s,
                            strategy_variant = COALESCE(strategy_variant, 'GOLD_SCALPING')
                        WHERE id = %s AND COALESCE(tp1_pl_applied, false) = false
                    """, (pl, partial_pct, at_id))
                    if cur.rowcount:
                        apply_portfolio_pl(user_id, pl, strategy_version=sv)
                        total_applied += pl
        conn.close()
    except Exception as e:
        log_error("apply_gold_tp1_partial_once failed for %s: %s" % (signal_id, e))
    return total_applied


def detect_gold_protected_stop(pair, direction, atr_hint=None):
    """1M continuation structure → protected stop (not exact entry BE)."""
    try:
        candles = get_candles(pair, "1m", "forex", use_cache=True)
    except Exception as e:
        log_error("Gold protected-stop 1m fetch failed: %s" % e)
        return None
    if not candles or len(candles) < 15:
        return None
    atr = atr_hint
    if not atr or atr <= 0:
        trs = []
        for i in range(1, min(len(candles), 30)):
            c, p = candles[i], candles[i - 1]
            trs.append(max(c["high"] - c["low"], abs(c["high"] - p["close"]), abs(c["low"] - p["close"])))
        atr = (sum(trs[-14:]) / max(len(trs[-14:]), 1)) if trs else None
    if not atr or atr <= 0:
        return None
    buf = float(GOLD_V2_PROTECTED_ATR_BUFFER) * float(atr)
    highs, lows = [], []
    for i in range(2, len(candles) - 2):
        h, l = candles[i]["high"], candles[i]["low"]
        if h >= max(candles[j]["high"] for j in range(i - 2, i + 3)):
            highs.append((i, h))
        if l <= min(candles[j]["low"] for j in range(i - 2, i + 3)):
            lows.append((i, l))
    last = candles[-1]
    if direction == "BUY":
        if len(lows) < 2 or len(highs) < 1:
            return None
        if lows[-1][1] <= lows[-2][1]:
            return None
        if float(last["close"]) <= highs[-1][1]:
            return None
        return float(lows[-1][1] - buf)
    if len(highs) < 2 or len(lows) < 1:
        return None
    if highs[-1][1] >= highs[-2][1]:
        return None
    if float(last["close"]) >= lows[-1][1]:
        return None
    return float(highs[-1][1] + buf)


def finalize_gold_auto_trades(signal_id, exit_price, outcome_type, final_level):
    """Close remaining Gold size once (final_pl_applied). Combines with TP1 P/L."""
    if not DATABASE_URL or not signal_id:
        return
    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, user_id, direction, entry, position_size,
                           COALESCE(tp1_realized_pl, 0), COALESCE(partial_pct, %s),
                           COALESCE(final_pl_applied, false), strategy_version
                    FROM auto_trades
                    WHERE signal_id = %s AND status = 'OPEN'
                """, (GOLD_V2_PARTIAL_PCT, signal_id))
                for row in cur.fetchall():
                    at_id, user_id, direction, entry, pos, tp1_pl, partial_pct, done, sv = row
                    if done:
                        continue
                    rem_frac = 1.0 - (float(partial_pct) / 100.0)
                    rem_size = float(pos) * rem_frac
                    entry, exit_price = float(entry), float(exit_price)
                    if "BUY" in str(direction).upper():
                        rem_pl = rem_size * (exit_price - entry)
                    else:
                        rem_pl = rem_size * (entry - exit_price)
                    total_pl = float(tp1_pl) + rem_pl
                    # Reclassify BE if near zero
                    if abs(total_pl) <= GOLD_V2_BE_TOLERANCE:
                        outcome_type = "Break-even"
                    elif total_pl < 0:
                        outcome_type = "Loss"
                    sv = _normalize_strategy_version(sv) or "V2"
                    cur.execute("""
                        UPDATE auto_trades
                        SET status = 'CLOSED', final_level = %s, pl_amount = %s,
                            outcome_type = %s, management_stage = 'CLOSED',
                            final_pl_applied = TRUE, closed_at = NOW()
                        WHERE id = %s AND COALESCE(final_pl_applied, false) = false
                    """, (final_level, total_pl, outcome_type, at_id))
                    if cur.rowcount:
                        apply_portfolio_pl(user_id, rem_pl, strategy_version=sv)
        conn.close()
    except Exception as e:
        log_error("finalize_gold_auto_trades failed for %s: %s" % (signal_id, e))


def open_auto_trades_for_signal(pair, market_type, direction, entry, sl, tp1, tp2, tp3, signal_id, strategy_version="V2"):
    """
    Opens one sized position per auto-trading user for a freshly-fired
    signal, using SmartFX's own Entry/SL/TP levels - never a separate
    SL/TP calculation of its own.

    Forex pairs (see FOREX_CONTRACT_INFO) are sized in lots via pure
    risk-based calculation: (balance * risk%) / dollar risk per 1.00 lot
    from SmartFX's Entry/SL, floored to volume_step. If that is below
    min_volume (0.01), the broker minimum is used when balance can cover
    that lot's risk. Crypto pairs keep the original balance*risk% /
    SL-distance sizing, since "lots" aren't a crypto concept.
    """
    if not DATABASE_URL:
        return

    sv = _normalize_strategy_version(strategy_version)
    if sv not in ("V2", "V3"):
        log_error("Auto-trade skipped for Signal #%s: invalid strategy_version=%r" % (signal_id, strategy_version))
        return
    strategy_version = sv

    sl_distance = abs(entry - sl)
    if sl_distance <= 0:
        log_error(f"Auto-trade skipped for Signal #{signal_id}: SL distance is zero.")
        return

    users = get_auto_trading_users(strategy_version=strategy_version)
    if not users:
        return

    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                for user_id, risk_pct, _legacy_balance in users:
                    balance = get_portfolio_balance_for_strategy(user_id, strategy_version)
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
                        # Forex pair but sizing returned None (e.g. invalid
                        # levels, or balance too small for min_volume risk).
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
                             position_size, lot_size, strategy_version,
                             strategy_variant, management_stage, partial_pct,
                             tp1_pl_applied, final_pl_applied)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,FALSE,FALSE)
                        ON CONFLICT (user_id, signal_id) DO NOTHING
                    """, (
                        user_id, signal_id, pair, market_type, direction,
                        entry, sl, tp1, tp2, tp3, risk_pct, risk_amount,
                        position_size, lot_size, strategy_version,
                        strategy_variant, "ENTERED",
                        GOLD_V2_PARTIAL_PCT if strategy_variant == "GOLD_SCALPING" else None,
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
                    SELECT id, user_id, direction, entry, sl, tp1, tp2, tp3, position_size,
                           strategy_version
                    FROM auto_trades
                    WHERE signal_id = %s AND status = 'OPEN'
                """, (signal_id,))
                open_trades = cur.fetchall()

                for row in open_trades:
                    trade_id, user_id, direction, entry, sl, tp1, tp2, tp3, position_size, trade_sv = row
                    position_size = float(position_size)
                    trade_sv = _normalize_strategy_version(trade_sv) or "V2"

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
                        apply_portfolio_pl(user_id, pl_amount, strategy_version=trade_sv)
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

def get_candles(pair, timeframe, market_type, use_cache=True):
    """
    Fetch OHLC for pair/timeframe/market. Stage F6: optional shared ohlc_cache
    so V2 and V3 do not duplicate provider calls within TTL.
    V2 path unchanged functionally; V3 bundle reuses the same function.
    """
    cache_key = f"{pair}_{timeframe}_{market_type}"
    ttl = OHLC_CACHE_SECONDS.get(timeframe, 300)
    if use_cache:
        with state_lock:
            hit = ohlc_cache.get(cache_key)
        if hit and (time.time() - hit["fetched_at"]) < ttl and hit.get("candles"):
            return hit["candles"]

    if market_type == "crypto":
        kraken_symbol = CRYPTO_SYMBOL_MAP[pair]
        interval = KRAKEN_INTERVAL_MAP[timeframe]
        candles = fetch_kraken_ohlc(kraken_symbol, interval)
    else:
        interval = TWELVEDATA_INTERVAL_MAP[timeframe]
        candles = fetch_twelvedata_candles(pair, interval, CANDLE_LIMIT)

    if candles:
        with state_lock:
            ohlc_cache[cache_key] = {"candles": candles, "fetched_at": time.time()}
            # Keep forex monitor cache in sync for 1h/15m (existing behavior)
            if market_type == "forex" and timeframe in ("1h", "15m", "5m", "1m"):
                forex_candle_cache[f"{pair}_{timeframe}"] = candles
    return candles


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

def format_signal_message(pair, timeframe, result, trend, signal_id, market_type, strategy_version="V2"):
    sv = _normalize_strategy_version(strategy_version)
    if sv not in ("V2", "V3"):
        raise ValueError("format_signal_message: invalid strategy_version=%r" % (strategy_version,))

    direction = result.get("direction") if isinstance(result.get("direction"), str) else trend
    if isinstance(direction, str) and "BUY" in direction.upper():
        dir_label = "BUY"
    elif isinstance(direction, str) and "SELL" in direction.upper():
        dir_label = "SELL"
    else:
        dir_label = str(direction)

    variant = None
    if isinstance(result, dict):
        variant = result.get("strategy_variant")
    is_gold = (sv == "V2" and (variant == "GOLD_SCALPING" or pair == "XAU/USD"))

    engine_tag = "Gold V2" if is_gold else sv
    conf = result.get("confidence")
    conf_line = f"📊 Confidence: `{conf}%`\n" if conf is not None else ""

    lines = [
        f"📡 *SmartFX Signal #{signal_id}* [{engine_tag}]",
        f"",
        f"*{pair}* · {dir_label} · {timeframe}",
        f"",
        f"📍 Entry: `{result['entry']}`",
        f"🛑 SL: `{result['sl']}`",
        f"🎯 TP1: `{result['tp1']}`",
        f"🎯 TP2: `{result.get('tp2')}`",
    ]
    # Real TP3 only for non-Gold (V3 structural targets)
    if not is_gold and result.get("tp3") is not None:
        # Avoid showing a mirrored TP2 as a fake third target for Gold
        tp2, tp3 = result.get("tp2"), result.get("tp3")
        if tp3 is not None and (tp2 is None or abs(float(tp3) - float(tp2)) > 1e-12):
            lines.append(f"🎯 TP3: `{tp3}`")
    lines.append("")
    if conf_line:
        lines.append(conf_line.rstrip("\n"))
    lines.append("")
    lines.append("_Manage risk. Not financial advice._")
    return "\n".join(lines)



def _signal_state_key(pair, timeframe, strategy_version="V2"):
    """F4: PAIR_TIMEFRAME_STRATEGY so V2/V3 last_signals do not collide."""
    return f"{pair}_{timeframe}_{strategy_version}"


def is_duplicate_signal(pair, timeframe, direction, strategy_version="V2"):
    """Strategy-aware duplicate check. Default V2 preserves V2 path behavior."""
    key = _signal_state_key(pair, timeframe, strategy_version)
    with state_lock:
        return last_signals.get(key) == direction


def store_last_signal(pair, timeframe, direction, strategy_version="V2"):
    key = _signal_state_key(pair, timeframe, strategy_version)
    with state_lock:
        last_signals[key] = direction


def is_pair_in_cooldown(pair, strategy_version="V2"):
    """
    Strategy-specific pair cooldown: V2 and V3 do not block each other.
    Key: PAIR_STRATEGY (e.g. EUR/USD_V2).
    """
    sv = _normalize_strategy_version(strategy_version) or "V2"
    key = f"{pair}_{sv}"
    now = time.time()
    with state_lock:
        last_time = last_pair_signal_time.get(key)
        # Legacy pair-only key (pre-2.30) still honored for V2 only
        if last_time is None and sv == "V2":
            last_time = last_pair_signal_time.get(pair)

    if last_time is None:
        return False

    return (now - last_time) < PAIR_COOLDOWN_SECONDS


def mark_pair_signal_time(pair, strategy_version="V2"):
    """Record last signal time for this pair+strategy."""
    sv = _normalize_strategy_version(strategy_version) or "V2"
    key = f"{pair}_{sv}"
    with state_lock:
        last_pair_signal_time[key] = time.time()


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


def has_active_trade_for_pair(pair, strategy_version="V2"):
    """
    Strategy-specific active-risk gate: only blocks NEW signals from the
    same strategy on this pair while a pre-TP1 trade is open for that strategy.
    V2 open trade does not block V3 on the same pair (and vice versa).
    """
    sv = _normalize_strategy_version(strategy_version) or "V2"
    with state_lock:
        return any(
            t["pair"] == pair
            and not t.get("tp1_hit", False)
            and (_normalize_strategy_version(t.get("strategy_version")) or "V2") == sv
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
                           entry, sl, tp1, tp2, tp3, status, final_level, sent_at,
                           strategy_version
                    FROM signals
                    WHERE closed_at IS NULL
                """)
                rows = cur.fetchall()
        conn.close()
    except Exception as e:
        log_error(f"Failed to resume open trades from database: {e}")
        return

    resumed_count = 0
    for row in rows:
        # Compatible with 14- or 15-column SELECT
        if len(row) >= 15:
            (signal_id, pair, market_type, timeframe, direction,
             entry, sl, tp1, tp2, tp3, status, final_level, sent_at,
             strategy_version, strategy_variant) = row[:15]
        else:
            (signal_id, pair, market_type, timeframe, direction,
             entry, sl, tp1, tp2, tp3, status, final_level, sent_at,
             strategy_version) = row[:14]
            strategy_variant = None

        tp1_hit = status == "WIN"
        tp2_hit = tp1_hit and final_level == "TP2"

        _sv = _normalize_strategy_version(strategy_version) if strategy_version else None
        if _sv not in ("V2", "V3"):
            _sv = None
        if strategy_variant is None and pair == GOLD_V2_PAIR and _sv == "V2":
            strategy_variant = "GOLD_SCALPING"

        # Load Gold management state from auto_trades (source of truth)
        mgmt = {
            "management_stage": "ENTERED",
            "protected_stop": None,
            "tp1_realized_pl": None,
            "tp1_hit_at": None,
            "outcome_type": None,
            "gold_partial_closed": False,
            "partial_pct": GOLD_V2_PARTIAL_PCT,
        }
        try:
            conn2 = get_db_connection()
            with conn2:
                with conn2.cursor() as cur2:
                    cur2.execute("""
                        SELECT management_stage, protected_stop, tp1_realized_pl,
                               tp1_hit_at, outcome_type, COALESCE(tp1_pl_applied, false),
                               partial_pct, strategy_variant
                        FROM auto_trades
                        WHERE signal_id = %s AND status = 'OPEN'
                        LIMIT 1
                    """, (signal_id,))
                    ar = cur2.fetchone()
                    if ar:
                        if ar[0]:
                            mgmt["management_stage"] = ar[0]
                        mgmt["protected_stop"] = ar[1]
                        mgmt["tp1_realized_pl"] = float(ar[2]) if ar[2] is not None else None
                        mgmt["tp1_hit_at"] = ar[3].isoformat() if ar[3] else None
                        mgmt["outcome_type"] = ar[4]
                        mgmt["gold_partial_closed"] = bool(ar[5])
                        if ar[6] is not None:
                            mgmt["partial_pct"] = float(ar[6])
                        if ar[7]:
                            strategy_variant = ar[7]
                        if mgmt["gold_partial_closed"] or mgmt["management_stage"] in (
                            "TP1_PARTIAL", "PROTECTED"
                        ):
                            tp1_hit = True
            conn2.close()
        except Exception as e:
            log_error("Gold management resume lookup failed for %s: %s" % (signal_id, e))

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
            "opened_at": sent_at.isoformat() if hasattr(sent_at, "isoformat") else str(sent_at),
            "signal_id": signal_id,
            "strategy_version": _sv,
            "strategy_variant": strategy_variant,
            "tp1_hit": tp1_hit,
            "tp2_hit": tp2_hit,
            "tp3_hit": False,
            "management_stage": mgmt["management_stage"],
            "protected_stop": mgmt["protected_stop"],
            "tp1_realized_pl": mgmt["tp1_realized_pl"],
            "tp1_hit_at": mgmt["tp1_hit_at"],
            "outcome_type": mgmt["outcome_type"],
            "gold_partial_closed": mgmt["gold_partial_closed"],
            "partial_pct": mgmt["partial_pct"],
        }

        trade_id = f"{pair}_{timeframe}_{signal_id}"
        with state_lock:
            active_trades[trade_id] = trade
        resumed_count += 1

    if resumed_count:
        log_info(f"Resumed {resumed_count} still-open trade(s) from the database after restart.")
    else:
        log_info("No open trades to resume from the database.")


def open_trade(pair, timeframe, market_type, direction, result, signal_id, strategy_version="V2", strategy_variant=None):
    """Track open signal trade; strategy_version stored for dual-engine readiness."""
    trade_id = f"{pair}_{timeframe}_{strategy_version}_{int(time.time() * 1000)}"

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
        "strategy_version": strategy_version,
        "tp1_hit": False,
        "tp2_hit": False,
        "tp3_hit": False,
        "strategy_variant": strategy_variant or (result.get("strategy_variant") if isinstance(result, dict) else None),
        "management_stage": "ENTERED",
        "protected_stop": None,
        "tp1_hit_at": None,
        "tp1_realized_pl": None,
        "outcome_type": None,
        "gold_partial_closed": False,
    }

    with state_lock:
        active_trades[trade_id] = trade

    return trade_id


def close_trade(trade_id, trade, outcome, keep_tracking=False, trigger_info=None):
    pair = trade["pair"]

    with state_lock:
        if not keep_tracking:
            active_trades.pop(trade_id, None)

    _sv = _normalize_strategy_version(trade.get("strategy_version")) or "V2"
    _record_outcome_stats(pair, outcome, _sv)

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


def record_expiry_followup(trade):
    """
    Called once, right after a trade is marked EXPIRED. Inserts one
    row into expiry_followups for later (read-only) checking. Wrapped
    in try/except so that if this ever fails, it NEVER blocks or
    breaks the real expire_trade() flow above it - this is purely
    supplementary logging, not a critical path. Never changes any
    existing WIN/LOSS/EXPIRED status.
    """
    if not DATABASE_URL:
        return
    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO expiry_followups
                        (signal_id, pair, market_type, timeframe, direction,
                         entry, tp1, sl, followup_window_hours)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    trade.get("signal_id"), trade["pair"], trade["market_type"],
                    trade["timeframe"], trade["direction"], trade["entry"],
                    trade["tp1"], trade["sl"], EXPIRY_FOLLOWUP_WINDOW_HOURS,
                ))
        conn.close()
    except Exception as e:
        log_error(f"Failed to record expiry follow-up for signal "
                   f"#{trade.get('signal_id')}: {e}")


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
    record_expiry_followup(trade)


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
                        active_trades[trade_id]["tp1_hit_at"] = datetime.utcnow().isoformat()
                if _is_gold_v2_trade(trade):
                    # Gold: partial only — remaining stays open; no midpoint BE
                    apply_gold_tp1_partial_once(trade.get("signal_id"), trade)
                    with state_lock:
                        if trade_id in active_trades:
                            active_trades[trade_id]["management_stage"] = "TP1_PARTIAL"
                            active_trades[trade_id]["gold_partial_closed"] = True
                            active_trades[trade_id]["partial_pct"] = GOLD_V2_PARTIAL_PCT
                    db_update_auto_trade_management(
                        trade.get("signal_id"),
                        management_stage="TP1_PARTIAL",
                        tp1_hit_at=datetime.utcnow(),
                        strategy_variant="GOLD_SCALPING",
                        partial_pct=GOLD_V2_PARTIAL_PCT,
                    )
                    # Keep signal row open (WIN interim is historical behavior for cards)
                    close_trade(trade_id, trade, "WIN", keep_tracking=True, trigger_info=trigger_info)
                else:
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
            if _is_gold_v2_trade(trade):
                # --- Gold V2 post-TP1 lifecycle ---
                stage = trade.get("management_stage") or "TP1_PARTIAL"

                # Build protected stop from 1M structure when not yet set
                if stage == "TP1_PARTIAL" and trade.get("protected_stop") is None:
                    prot = detect_gold_protected_stop(
                        trade["pair"], direction, atr_hint=trade.get("atr")
                    )
                    if prot is not None:
                        with state_lock:
                            if trade_id in active_trades:
                                active_trades[trade_id]["protected_stop"] = prot
                                active_trades[trade_id]["management_stage"] = "PROTECTED"
                        db_update_auto_trade_management(
                            trade.get("signal_id"),
                            protected_stop=prot,
                            management_stage="PROTECTED",
                            strategy_variant="GOLD_SCALPING",
                        )
                        stage = "PROTECTED"

                # Monitor TP2 for remaining size
                tp2 = trade.get("tp2")
                if tp2 is not None:
                    hit_tp2 = (
                        (direction == "BUY" and price >= float(tp2))
                        or (direction == "SELL" and price <= float(tp2))
                    )
                    if hit_tp2:
                        finalize_gold_auto_trades(
                            trade.get("signal_id"), float(tp2), "Normal Win", "TP2"
                        )
                        db_update_signal_status(
                            trade.get("signal_id"), "WIN", final_level="TP2", closed=True
                        )
                        with state_lock:
                            active_trades.pop(trade_id, None)
                        continue

                # Protected stop hit (remaining)
                if trade.get("protected_stop") is not None or stage == "PROTECTED":
                    prot = trade.get("protected_stop")
                    if prot is not None:
                        prot = float(prot)
                        protected_hit = (
                            (direction == "BUY" and price <= prot)
                            or (direction == "SELL" and price >= prot)
                        )
                        if protected_hit:
                            # Classify from combined P/L inside finalize
                            finalize_gold_auto_trades(
                                trade.get("signal_id"), prot, "Protected Win", "PROTECTED"
                            )
                            db_update_signal_status(
                                trade.get("signal_id"), "WIN",
                                final_level="PROTECTED", closed=True,
                            )
                            with state_lock:
                                active_trades.pop(trade_id, None)
                            continue

                # Original SL still invalidates remaining if not protected yet
                if stage == "TP1_PARTIAL":
                    hit_sl = (
                        (direction == "BUY" and price <= float(trade["sl"]))
                        or (direction == "SELL" and price >= float(trade["sl"]))
                    )
                    if hit_sl:
                        finalize_gold_auto_trades(
                            trade.get("signal_id"), float(trade["sl"]), "Loss", "SL"
                        )
                        db_update_signal_status(
                            trade.get("signal_id"), "LOSS", final_level="SL", closed=True
                        )
                        with state_lock:
                            active_trades.pop(trade_id, None)
                        continue

                # Gold: never use exact-entry BE or TP3
                continue

            # Non-Gold: legacy exact-entry reverse stop after TP1
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

            if not trade.get("tp3_hit") and not _is_gold_v2_trade(trade):
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

    # Old smc_analysis module removed; trend cache unused by live scanners.
    trend = None

    with state_lock:
        trend_cache[cache_key] = {"trend": trend, "fetched_at": now}

    if market_type == "forex":
        time.sleep(FOREX_INNER_CALL_DELAY_SECONDS)

    return trend



# ==========================================================
# NEW GOLD V2 (smc_analysis_v2) — XAU/USD only, 15M → 5M → 1M
# ==========================================================
# Old V2 (smc_analysis.analyze_candles) stays disconnected.
# New V2 is strategy_version "V2" in the shared pipeline so dashboard
# AutoBot / portfolio_v2 / history filters continue to work. Historical
# old-V2 DB rows are not modified.

GOLD_V2_PAIR = "XAU/USD"
V2_SIGNAL_TIMEFRAME = "1m"


def _candles_to_v2_dataframe(candles):
    """Convert app candle list [{time, open, high, low, close}, ...] to DataFrame."""
    import pandas as pd

    if not candles:
        raise ValueError("empty candle list")
    rows = []
    for c in candles:
        rows.append({
            "open": float(c["open"]),
            "high": float(c["high"]),
            "low": float(c["low"]),
            "close": float(c["close"]),
            "time": c.get("time"),
        })
    df = pd.DataFrame(rows)
    try:
        df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
        if df["time"].notna().any():
            df = df.set_index("time")
        else:
            df = df.drop(columns=["time"], errors="ignore")
    except Exception:
        df = df.drop(columns=["time"], errors="ignore")
    return df


def fetch_v2_gold_candle_bundle(pair, market_type):
    """15m / 5m / 1m for Gold V2. Returns (df15, df5, df1) or None."""
    if pair != GOLD_V2_PAIR or market_type != "forex":
        return None
    series = {}
    for tf in ("15m", "5m", "1m"):
        try:
            candles = get_candles(pair, tf, market_type, use_cache=True)
            if not candles or len(candles) < 10:
                log_info("Gold V2 bundle skip %s: insufficient %s candles." % (pair, tf))
                return None
            series[tf] = _candles_to_v2_dataframe(candles)
        except Exception as e:
            log_error("Gold V2 candle fetch failed for %s %s: %s" % (pair, tf, e))
            return None
        time.sleep(FOREX_INNER_CALL_DELAY_SECONDS)
    return series["15m"], series["5m"], series["1m"]


def map_gold_v2_result_to_signal(raw):
    """Map smc_analysis_v2 SIGNAL dict → shared emit contract fields."""
    if not raw or raw.get("status") != "SIGNAL":
        return None
    direction = str(raw.get("direction") or "").upper()
    if direction not in ("BUY", "SELL"):
        return None
    entry = _as_float(raw.get("entry"))
    sl = _as_float(raw.get("sl"))
    tp1 = _as_float(raw.get("tp1"))
    if entry is None or sl is None or tp1 is None:
        return None
    tp2 = _as_float(raw.get("tp2"))
    # Schema / message paths expect tp3 NOT NULL — Gold V2 has TP1/TP2 only.
    # Mirror TP2 (or TP1) into tp3 for storage compatibility without inventing RR.
    tp3 = tp2 if tp2 is not None else tp1
    atr = None
    try:
        atr = _as_float((raw.get("mss") or {}).get("atr1"))
    except Exception:
        atr = None
    conf = raw.get("confidence")
    try:
        conf = int(conf) if conf is not None else None
    except (TypeError, ValueError):
        conf = None
    return {
        "direction": direction,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2 if tp2 is not None else tp1,
        "tp3": tp3,  # DB compatibility only — not a real third target for Gold
        "strategy_variant": "GOLD_SCALPING",
        "confidence": conf if conf is not None else 0,
        "atr": atr,
        "risk": raw.get("risk"),
        "factors": {
            "engine": "gold_v2",
            "reason": raw.get("reason"),
            "trend": raw.get("trend"),
            "sweep": raw.get("sweep"),
            "mss": raw.get("mss"),
            "tp1_rr": raw.get("tp1_rr"),
            "tp2_rr": raw.get("tp2_rr"),
            "confidence_is_informational": raw.get("confidence_is_informational", True),
        },
        "setup_explanation": raw.get("reason"),
        "support": None,
        "resistance": None,
        "_raw_gold_v2": raw,
    }


def run_v2_gold_analysis(pair, market_type):
    """Run new Gold V2 engine. Returns mapped signal dict or None."""
    bundle = fetch_v2_gold_candle_bundle(pair, market_type)
    if bundle is None:
        return None, None
    df15, df5, df1 = bundle
    bias = None
    try:
        trend = smc_analysis_v2.determine_15m_bias(df15)
        bias = trend.get("bias") if trend.get("bias") in ("BUY", "SELL") else None
    except Exception as e:
        log_error("Gold V2 bias error for %s: %s" % (pair, e))
    try:
        raw = smc_analysis_v2.analyze_gold_v2(df15, df5, df1)
    except Exception as e:
        log_error("Gold V2 analyze_gold_v2 error for %s: %s" % (pair, e))
        return None, bias
    mapped = map_gold_v2_result_to_signal(raw)
    return mapped, bias


def analyze_pair_v2(pair, market_type):
    """
    New Gold V2 entry from the shared scanner.
    Only XAU/USD + forex. Gated by is_strategy_enabled("V2").
    Uses shared emit path with strategy_version="V2".
    """
    if not is_strategy_enabled("V2"):
        return
    if pair != GOLD_V2_PAIR or market_type != "forex":
        return
    try:
        mapped, bias = run_v2_gold_analysis(pair, market_type)
        if mapped is None:
            trend = bias if bias in ("BUY", "SELL") else None
            db_update_pair_status(
                pair, market_type, trend, "WATCHING", None, None, strategy_version="V2"
            )
            return
        emit_signal_from_result(
            pair, market_type, V2_SIGNAL_TIMEFRAME, mapped,
            strategy_version="V2", strategy_variant="GOLD_SCALPING",
        )
        d = mapped["direction"]
        db_update_pair_status(
            pair, market_type, d, "SIGNAL",
            mapped.get("confidence"), d, strategy_version="V2",
        )
    except Exception as e:
        log_error("Gold V2 engine error for %s (isolated): %s" % (pair, e))


def analyze_pair(pair, market_type):
    """OLD V2 runtime disconnected (2.33.0).

    Previously ran smc_analysis.analyze_candles on 1H/15M with 4H trend.
    That engine is no longer invoked from the live scanner.

    New Gold V2 (smc_analysis_v2.py, 15M→5M→1M) is connected for XAU/USD.
    V3 continues via analyze_pair_v3 / safe_analyze.
    Historical V2 signals/trades in the database are left intact.
    """
    log_info(
        "Old V2 runtime skipped for %s (%s): disconnected; Gold V2 active when strategy V2 is enabled."
        % (pair, market_type)
    )
    return


# ==========================================================
# V3 STRATEGY ADAPTER (Stage F3) — gated by is_strategy_enabled("V3") (default OFF)
# ==========================================================
# Shared-record timeframe for V3 signals:
#   V3_SIGNAL_TIMEFRAME = "5m"  (final trigger TF inside analyze_v3).
#   V2 continues to use "1h" / "15m". Multi-TF path is in result["factors"].
# This is a record-label choice only — not a new V3 strategy rule.
V3_SIGNAL_TIMEFRAME = "5m"


def fetch_v3_candle_bundle(pair, market_type):
    """
    Stage F6: obtain 4h/1h/15m/5m for analyze_v3 via get_candles (shared OHLC cache).
    Returns (c4h, c1h, c15, c5) or None if any series is missing/failed.
    Failure isolation: returns None only — never raises to caller scan loops.
    Does not invent candles or mix timeframes.
    Gold/XAU is not part of the V3 market universe.
    """
    if market_type == "forex" and pair not in V3_FOREX_PAIRS:
        return None
    if market_type == "crypto" and pair not in V3_CRYPTO_PAIRS:
        return None
    series = {}
    for tf in ("4h", "1h", "15m", "5m"):
        try:
            candles = get_candles(pair, tf, market_type, use_cache=True)
            if not candles:
                log_info(f"V3 bundle skip {pair}: empty {tf} series.")
                return None
            series[tf] = candles
        except Exception as e:
            log_error(f"V3 candle fetch failed for {pair} {tf}: {e}")
            return None
        if market_type == "forex" and tf != "5m":
            # Pace forex provider calls when cache misses force network I/O
            time.sleep(FOREX_INNER_CALL_DELAY_SECONDS)
    return series["4h"], series["1h"], series["15m"], series["5m"]


def run_v3_analysis(pair, market_type):
    """Thin adapter: candles → smc_analysis_v3.analyze_v3.

    Returns (result, bias) where:
      result — signal dict from analyze_v3, or None if no complete signal
      bias   — 4H BUY/SELL from smc_analysis_v3.get_4h_bias on the same
               4H candles already fetched for this scan, or None

    Bias is computed here only so the scanner can still write
    pair_scan_status.trend when later V3 gates fail and result is None.
    Does not change V3 gates, confidence, or signal emission.
    """
    bundle = fetch_v3_candle_bundle(pair, market_type)
    if bundle is None:
        return None, None
    candles_4h, candles_1h, candles_15m, candles_5m = bundle
    bias = None
    try:
        b, _src = smc_analysis_v3.get_4h_bias(candles_4h)
        if b in ("BUY", "SELL"):
            bias = b
    except Exception as e:
        log_error(f"V3 get_4h_bias error for {pair}: {e}")
        bias = None
    try:
        result = smc_analysis_v3.analyze_v3(candles_4h, candles_1h, candles_15m, candles_5m)
        return result, bias
    except Exception as e:
        log_error(f"V3 analyze_v3 error for {pair}: {e}")
        return None, bias



# ==========================================================
# NORMALIZED SMARTFX SIGNAL CONTRACT (Stage F8)
# ==========================================================

def _parse_direction_buy_sell(direction_raw):
    s = str(direction_raw or "")
    u = s.upper()
    if "BUY" in u:
        return "BUY"
    if "SELL" in u:
        return "SELL"
    return None


def _as_float(value):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_strategy_signal(pair, market_type, timeframe, result, strategy_version):
    """(normalized_dict, None) or (None, error_reason). No invented levels."""
    sv = _normalize_strategy_version(strategy_version)
    if sv not in ("V2", "V3"):
        return None, "invalid or missing strategy_version=%r" % (strategy_version,)
    if not result or not isinstance(result, dict):
        return None, "result is empty or not a dict"
    direction = _parse_direction_buy_sell(result.get("direction"))
    if direction is None:
        return None, "unrecognized direction=%r" % (result.get("direction"),)
    entry = _as_float(result.get("entry"))
    sl = _as_float(result.get("sl"))
    tp1 = _as_float(result.get("tp1"))
    if entry is None or sl is None or tp1 is None:
        return None, "core levels invalid entry=%r sl=%r tp1=%r" % (
            result.get("entry"), result.get("sl"), result.get("tp1"))
    direction_display = result.get("direction")
    if not isinstance(direction_display, str) or direction not in str(direction_display):
        direction_display = direction
    normalized = {
        "strategy_version": sv,
        "pair": pair,
        "market_type": market_type,
        "timeframe": timeframe,
        "direction": direction,
        "direction_display": direction_display,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": _as_float(result.get("tp2")),
        "tp3": _as_float(result.get("tp3")),
        "confidence": _as_float(result.get("confidence")),
        "risk": result.get("risk"),
        "atr": _as_float(result.get("atr")),
        "factors": result.get("factors"),
        "support": result.get("support"),
        "resistance": result.get("resistance"),
        "setup_explanation": result.get("setup_explanation"),
        "zone_low": result.get("zone_low"),
        "zone_high": result.get("zone_high"),
        "_raw_result": result,
    }
    return normalized, None


def emit_signal_from_result(pair, market_type, timeframe, result, strategy_version, strategy_variant=None):
    """Shared emit via normalized contract (F8). Rejects invalid identity/levels."""
    # Tag Gold V2 before normalize so raw carries variant
    if strategy_variant is None and isinstance(result, dict):
        strategy_variant = result.get("strategy_variant")
    if strategy_variant is None and _normalize_strategy_version(strategy_version) == "V2" and pair == "XAU/USD":
        strategy_variant = "GOLD_SCALPING"
    if isinstance(result, dict) and strategy_variant:
        result = dict(result)
        result["strategy_variant"] = strategy_variant

    normalized, err = normalize_strategy_signal(
        pair, market_type, timeframe, result, strategy_version
    )
    if err:
        log_error("Signal rejected (%s %s): %s" % (pair, timeframe, err))
        return False

    direction = normalized["direction"]
    sv = normalized["strategy_version"]
    raw = normalized.get("_raw_result") or result
    if isinstance(raw, dict) and strategy_variant:
        raw = dict(raw)
        raw["strategy_variant"] = strategy_variant

    if is_duplicate_signal(pair, timeframe, direction, strategy_version=sv):
        return False
    if is_pair_in_cooldown(pair, strategy_version=sv):
        log_info("Skipping %s signal for %s (%s): pair cooldown active." % (sv, pair, timeframe))
        return False
    if has_active_trade_for_pair(pair, strategy_version=sv):
        log_info("Skipping %s signal for %s (%s): active trade exists." % (sv, pair, timeframe))
        return False

    store_last_signal(pair, timeframe, direction, strategy_version=sv)
    signal_id = get_next_signal_id()
    message = format_signal_message(pair, timeframe, raw, direction, signal_id, market_type, strategy_version=sv)
    sent = send_public_signal(message)
    if not sent:
        return False

    # Stats must run after a successful send (was unreachable due to bad indent)
    _record_signal_stats(pair, direction, market_type, timeframe, strategy_version=sv)

    log_info(
        "Signal #%s sent [%s%s]: %s %s (%s) confidence=%s entry=%s sl=%s tp1=%s"
        % (
            signal_id, sv,
            ("/" + strategy_variant) if strategy_variant else "",
            pair, direction, timeframe,
            normalized.get("confidence"), normalized.get("entry"),
            normalized.get("sl"), normalized.get("tp1"),
        )
    )
    mark_pair_signal_time(pair, strategy_version=sv)
    open_trade(
        pair, timeframe, market_type, direction, raw, signal_id,
        strategy_version=sv, strategy_variant=strategy_variant,
    )
    db_insert_signal(
        pair, market_type, timeframe, direction, raw, signal_id,
        strategy_version=sv, strategy_variant=strategy_variant,
    )
    open_auto_trades_for_signal(
        pair, market_type, direction,
        normalized["entry"], normalized["sl"], normalized["tp1"],
        normalized["tp2"], normalized["tp3"],
        signal_id, strategy_version=sv, strategy_variant=strategy_variant,
    )
    db_log_activity(
        "signal_sent",
        "Signal #%s [%s%s] sent: %s %s (%s) - confidence %s%%"
        % (
            signal_id, sv,
            ("/" + strategy_variant) if strategy_variant else "",
            pair, direction, timeframe, normalized.get("confidence"),
        ),
        strategy_version=sv,
    )
    send_push_to_all(
        "new_signal_alerts",
        "SmartFX Signal",
        "%s — %s [%s]\nConfidence: %s%%" % (pair, direction, sv, normalized.get("confidence")),
        url="dashboard.html?signal=%s" % signal_id,
    )
    return True



def analyze_pair_v3(pair, market_type):
    """
    Stage F7: V3 engine entry from the shared scanner.
    Independent of V2. No-op unless is_strategy_enabled("V3").
    Does not alter V3 strategy gates. Failures are logged and swallowed.

    Scanner note: when analyze_v3 returns None (later gate fail), we still
    write the 4H bias from get_4h_bias into pair_scan_status.trend so the
    dashboard can show BUY/SELL bias without a complete signal.
    """
    if not is_strategy_enabled("V3"):
        return
    # Explicit V3 universe (Gold never included)
    if market_type == "forex" and pair not in V3_FOREX_PAIRS:
        return
    if market_type == "crypto" and pair not in V3_CRYPTO_PAIRS:
        return
    try:
        result, bias = run_v3_analysis(pair, market_type)
        if result is None:
            # WATCHING: keep status/confidence/direction as before; only
            # surface existing 4H bias in trend when available.
            trend = bias if bias in ("BUY", "SELL") else None
            db_update_pair_status(
                pair, market_type, trend, "WATCHING", None, None, strategy_version="V3"
            )
            return
        emit_signal_from_result(
            pair, market_type, V3_SIGNAL_TIMEFRAME, result, strategy_version="V3",
        )
        direction = result.get("direction", "")
        if "BUY" in str(direction).upper():
            d = "BUY"
        elif "SELL" in str(direction).upper():
            d = "SELL"
        else:
            d = None
        db_update_pair_status(
            pair, market_type, None, "SIGNAL",
            result.get("confidence"), d, strategy_version="V3"
        )
    except Exception as e:
        log_error(f"V3 engine error for {pair} (isolated; V2 unaffected): {e}")


def safe_analyze(pair, market_type):
    """
    Shared scanner path per pair.

    NEW Gold V2 (smc_analysis_v2) — XAU/USD only, when is_strategy_enabled("V2").
    OLD V2 (smc_analysis.analyze_candles) remains disconnected.
    V3 — unchanged, when is_strategy_enabled("V3").
    """
    # --- NEW GOLD V2 ENGINE ---
    if is_strategy_enabled("V2"):
        try:
            analyze_pair_v2(pair, market_type)
        except Exception as e:
            log_error(f"Gold V2 engine error analyzing {pair}: {e}")

    # --- V3 ENGINE (unchanged) ---
    try:
        analyze_pair_v3(pair, market_type)
    except Exception as e:
        log_error(f"V3 engine error analyzing {pair} (isolated): {e}")


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
        counts = {}
        for _sv in ("V2", "V3"):
            for tf, n in _daily_bucket(_sv).get("timeframe_counts", {}).items():
                counts[tf] = counts.get(tf, 0) + n
    if not counts or max(counts.values()) == 0:
        return "N/A"
    return max(counts, key=counts.get).upper()


def reset_daily_stats():
    with state_lock:
        daily_stats["V2"] = _empty_daily_bucket()
        daily_stats["V3"] = _empty_daily_bucket()

    log_info("Daily stats reset for the new day (V2 + V3 buckets).")


def build_morning_report():
    with state_lock:
        v2 = dict(_daily_bucket("V2"))
        v3 = dict(_daily_bucket("V3"))

    active_count = count_active_risk_trades()
    nl = chr(10)

    return (
        f"🤖 *{BOT_NAME} Morning Report*" + nl + nl
        + "✅ Bot Status: ONLINE" + nl + nl
        + f"🏷 Strategies: V2={'ON' if is_strategy_enabled('V2') else 'OFF'} · V3={'ON' if is_strategy_enabled('V3') else 'OFF'}" + nl + nl
        + f"📅 Date: {datetime.utcnow().strftime('%d %B %Y')}" + nl + nl
        + f"📈 Crypto Pairs: {len(CRYPTO_PAIRS)}" + nl
        + f"🌱 Forex Pairs: {len(FOREX_PAIRS)}" + nl + nl
        + f"*V2 today:* signals {v2['signals']} · W {v2['wins']} · L {v2['losses']}" + nl
        + f"*V3 today:* signals {v3['signals']} · W {v3['wins']} · L {v3['losses']}" + nl + nl
        + f"🔄 Active Trades: {active_count}" + nl + nl
        + "Bot is healthy and scanning the markets..."
    )



def build_evening_report():
    with state_lock:
        v2 = dict(_daily_bucket("V2"))
        v3 = dict(_daily_bucket("V3"))

    active_count = count_active_risk_trades()
    best_pair, _ = get_best_worst_pairs()
    most_active_tf = get_most_active_timeframe()
    nl = chr(10)

    def _wr(b):
        t = b["wins"] + b["losses"]
        return round((b["wins"] / t) * 100, 1) if t else 0

    return (
        f"🌙 *{BOT_NAME} Evening Report*" + nl + nl
        + f"🏷 Strategies: V2={'ON' if is_strategy_enabled('V2') else 'OFF'} · V3={'ON' if is_strategy_enabled('V3') else 'OFF'}" + nl + nl
        + f"*V2:* signals {v2['signals']} · W {v2['wins']} · L {v2['losses']} · WR {_wr(v2)}%" + nl
        + f"*V3:* signals {v3['signals']} · W {v3['wins']} · L {v3['losses']} · WR {_wr(v3)}%" + nl + nl
        + f"🔄 Still Running (Active): {active_count}" + nl
        + f"Best Pair: {best_pair}" + nl
        + f"Most Active Timeframe: {most_active_tf}" + nl + nl
        + "✅ Bot Status: Running Normally" + nl + nl
        + "See you tomorrow."
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
            # Load latest V2/V3 switches BEFORE this cycle scans
            refresh_strategy_flags_from_db()
            run_crypto_analysis()
            run_forex_analysis()
        except Exception as e:
            log_error(f"Analysis loop error: {e}")

        update_heartbeat("analysis")
        db_update_bot_status()
        time.sleep(ANALYSIS_LOOP_SECONDS)


def check_expiry_followups():
    """
    Runs on the existing trade_monitor_loop cadence. For every
    unresolved expiry_followups row, checks the current price the
    same way monitor_trades() already does for live trades, and
    records a late TP1/SL hit if one occurs - or marks the row
    resolved with no hit once the follow-up window runs out.

    Read-only with respect to everything else: never touches the
    signals table, active_trades, or any WIN/LOSS/EXPIRED status.
    """
    if not DATABASE_URL:
        return

    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, signal_id, pair, market_type, timeframe,
                           direction, entry, tp1, sl, expired_at,
                           followup_window_hours
                    FROM expiry_followups
                    WHERE resolved = false
                """)
                columns = [desc[0] for desc in cur.description]
                pending = [dict(zip(columns, row)) for row in cur.fetchall()]
        conn.close()
    except Exception as e:
        log_error(f"Failed to fetch pending expiry follow-ups: {e}")
        return

    for row in pending:
        try:
            expired_at = row["expired_at"]
            if expired_at.tzinfo is None:
                hours_since_expired = (
                    datetime.utcnow() - expired_at
                ).total_seconds() / 3600.0
            else:
                hours_since_expired = (
                    datetime.now(expired_at.tzinfo) - expired_at
                ).total_seconds() / 3600.0

            direction = row["direction"]
            late_hit = None

            if row["market_type"] == "forex":
                fake_trade = {
                    "pair": row["pair"], "direction": direction,
                    "sl": row["sl"], "tp1": row["tp1"],
                    "market_type": "forex", "timeframe": row["timeframe"],
                }
                outcome, _ = check_forex_candles_for_hit(fake_trade)
                if outcome == "LOSS":
                    late_hit = "SL"
                elif outcome == "WIN":
                    late_hit = "TP1"

            if late_hit is None:
                price = get_current_price(row["pair"], row["market_type"])

                hit_sl = (
                    (direction == "BUY" and price <= row["sl"])
                    or (direction == "SELL" and price >= row["sl"])
                )
                hit_tp1 = (
                    (direction == "BUY" and price >= row["tp1"])
                    or (direction == "SELL" and price <= row["tp1"])
                )

                if hit_sl:
                    late_hit = "SL"
                elif hit_tp1:
                    late_hit = "TP1"

            if late_hit is not None:
                conn = get_db_connection()
                with conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            UPDATE expiry_followups
                            SET late_hit = %s,
                                late_hit_at = %s,
                                late_hit_hours_after_expiry = %s,
                                resolved = true
                            WHERE id = %s
                        """, (late_hit, datetime.utcnow(),
                              round(hours_since_expired, 2), row["id"]))
                conn.close()

            elif hours_since_expired >= row["followup_window_hours"]:
                conn = get_db_connection()
                with conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            UPDATE expiry_followups
                            SET resolved = true
                            WHERE id = %s
                        """, (row["id"],))
                conn.close()

        except Exception as e:
            log_error(
                f"Expiry follow-up check failed for signal "
                f"#{row.get('signal_id')}: {e}"
            )
            continue


def trade_monitor_loop():
    while True:
        try:
            monitor_trades()
            check_expiry_followups()
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
    ensure_bot_started_at()
    seed_signal_id_counter()
    resume_open_trades_from_db()
    refresh_strategy_flags_from_db()
    log_info(
        "Strategy control: V2=%s V3=%s" % (
            "ON" if is_strategy_enabled("V2") else "OFF",
            "ON" if is_strategy_enabled("V3") else "OFF",
        )
    )

    threading.Thread(target=analysis_loop, daemon=True).start()
    threading.Thread(target=trade_monitor_loop, daemon=True).start()
    threading.Thread(target=daily_summary_loop, daemon=True).start()
    threading.Thread(target=watchdog_loop, daemon=True).start()
    news_engine.start_news_engine_thread()

    log_info(
        f"Background threads started: analysis, trade monitor, daily summary, watchdog, news engine. "
        f"({BOT_NAME} v{VERSION} / gold-v2 / "
        f"v3 {getattr(smc_analysis_v3, 'V3_VERSION', '?')} "
        f"V2={'ON' if is_strategy_enabled('V2') else 'OFF'} "
        f"V3={'ON' if is_strategy_enabled('V3') else 'OFF'})"
    )
    db_log_activity(
        "bot_restart",
        f"Bot started ({BOT_NAME} v{VERSION} / gold-v2 / "
        f"v3 {getattr(smc_analysis_v3, 'V3_VERSION', '?')} "
        f"V2={'ON' if is_strategy_enabled('V2') else 'OFF'} "
        f"V3={'ON' if is_strategy_enabled('V3') else 'OFF'})",
    )

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
        v2 = dict(_global_bucket("V2"))
        v3 = dict(_global_bucket("V3"))
        active_count = len(active_trades)

    return jsonify({
        "status": "running",
        "version": VERSION,
        "bot_name": BOT_NAME,
        "active_trades": active_count,
        "V2": v2,
        "V3": v3,
        "errors": _ops_errors,
        "checked_at": datetime.utcnow().isoformat(),
    })




# ==========================================================
# ENTRY POINT
# ==========================================================

start_background_threads()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
