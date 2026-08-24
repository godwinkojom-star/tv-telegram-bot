"""
news_engine.py - SmartFX News Opportunity Engine (V1 scaffolding)

INDEPENDENCE BY DESIGN
=======================
This module is deliberately self-contained - its own DB connection
helper, its own logging, no imports from app.py or smc_analysis.py,
and it is NOT called from anywhere in the signal-creation pipeline.
app.py only starts this module's background loop as one more
independent thread (see start_news_engine_thread), the same way it
already runs separate analysis/trade-monitor/watchdog loops. That is
the ONLY connection point right now.

Per the agreed plan (pre-Sept-19, during the 30-day strategy trial):
- No news provider is configured yet - fetch_news_events() is a clear
  placeholder that returns nothing and says so.
- The loop still runs continuously and writes a real heartbeat to
  news_engine_state, so the watchdog/alerting pattern can be proven
  working *before* any real news data or signal integration exists.
- Nothing in this file can affect a signal's confidence, block a
  signal, or touch smc_analysis.py's strategy logic. Those columns
  (signals.news_tag, news_event_id, news_context) exist in the
  database already but nothing in this file - or anywhere else yet -
  writes to them. That only happens in a later, separate integration
  step, after the trial ends and a real provider is connected.
"""

import os
import time
import logging
import threading
from datetime import datetime

import psycopg

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [NEWS-ENGINE] %(message)s")
logger = logging.getLogger("news_engine")

DATABASE_URL = os.environ.get("DATABASE_URL")
NEWS_ENGINE_LOOP_SECONDS = 300  # 5 minutes - matches the bot-watchdog Edge Function's cadence


def log_info(msg):
    logger.info(msg)


def log_error(msg):
    logger.error(msg)


def get_db_connection():
    if not DATABASE_URL:
        return None
    return psycopg.connect(DATABASE_URL, connect_timeout=10)


def fetch_news_events():
    """
    PLACEHOLDER - no news provider is configured yet. Returns an empty
    result with a clear reason, rather than raising or pretending to
    have real data. This is the ONLY function that needs real work
    when a provider (e.g. an economic-calendar API) is chosen later -
    everything else in this module already works around whatever this
    function returns.
    """
    return {
        "events": [],
        "provider_configured": False,
        "reason": "No news provider configured yet - Engine idle by design (V1 scaffolding).",
    }


def update_engine_status(provider_configured, status_message):
    """
    Writes the engine's current status/heartbeat to its own table.
    This is a plain UPDATE - if it fails (DB briefly unreachable,
    etc.), the loop below simply logs it and tries again next cycle;
    it never raises up into anything that could affect the main bot.
    """
    conn = get_db_connection()
    if not conn:
        log_error("News Engine: DATABASE_URL not set - cannot write heartbeat.")
        return
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE news_engine_state
                    SET status_message = %s, provider_configured = %s, last_heartbeat_at = NOW()
                    WHERE id = 1
                """, (status_message, provider_configured))
        conn.close()
    except Exception as e:
        log_error(f"Failed to write News Engine heartbeat: {e}")


def run_cycle():
    """
    One full cycle: attempt to fetch events (currently always the
    placeholder), log/store a clear status, and update the heartbeat.
    Wrapped in its own try/except so a bug here can never propagate
    out and affect the thread that's actually calling this - the
    calling loop below already isolates this too, but this is a second
    layer, matching "the news feature disables itself, not your bot."
    """
    try:
        result = fetch_news_events()
        if result["provider_configured"]:
            status = f"HEALTHY — provider active — {len(result['events'])} upcoming events cached"
        else:
            status = "HEALTHY — Provider not configured — Engine idle"

        log_info(status)
        update_engine_status(result["provider_configured"], status)
    except Exception as e:
        log_error(f"News Engine cycle failed unexpectedly: {e}")
        update_engine_status(False, f"ERROR — {e}")


def news_engine_loop():
    log_info("News Engine background loop started (V1 scaffolding - no provider connected).")
    while True:
        run_cycle()
        time.sleep(NEWS_ENGINE_LOOP_SECONDS)


def start_news_engine_thread():
    """
    Called once from app.py's start_background_threads(), alongside
    the other independent loops (analysis, trade monitor, watchdog).
    This is the only place app.py touches this module at all - it
    does not call into, import from, or get called by anything in the
    signal-creation path.
    """
    thread = threading.Thread(target=news_engine_loop, daemon=True, name="news-engine")
    thread.start()
    log_info("News Engine thread launched.")
