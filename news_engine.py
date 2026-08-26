"""
news_engine.py - SmartFX News Opportunity Engine
"""
import os
import time
import logging
import threading
from datetime import datetime, timedelta

import requests
import psycopg
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("news_engine")
DATABASE_URL = os.getenv("DATABASE_URL")
FMP_API_KEY = os.getenv("FMP_API_KEY")
NEWS_ENGINE_LOOP_SECONDS = 300


def log_info(msg):
    logger.info(f"[NEWS-ENGINE] {msg}")


def log_error(msg):
    logger.error(f"[NEWS-ENGINE] {msg}")


def get_db_connection():
    if not DATABASE_URL:
        return None
    return psycopg.connect(DATABASE_URL, connect_timeout=10)


def ensure_news_tables():
    """Create the News Engine status and event tables if needed."""
    conn = get_db_connection()
    if not conn:
        log_error("News Engine: DATABASE_URL not set - cannot prepare database tables.")
        return False
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS news_engine_state (
                        id INTEGER PRIMARY KEY,
                        status_message TEXT,
                        provider_configured BOOLEAN NOT NULL DEFAULT FALSE,
                        last_heartbeat_at TIMESTAMPTZ
                    )
                """)
                cur.execute("""
                    INSERT INTO news_engine_state
                    (id, status_message, provider_configured, last_heartbeat_at)
                    VALUES (1, 'INITIALIZING', FALSE, NOW())
                    ON CONFLICT (id) DO NOTHING
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS news_events (
                        event_key TEXT PRIMARY KEY,
                        event_date TIMESTAMPTZ,
                        country TEXT,
                        currency TEXT,
                        event_name TEXT,
                        impact TEXT,
                        actual TEXT,
                        previous TEXT,
                        forecast TEXT,
                        source TEXT NOT NULL DEFAULT 'FMP',
                        fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                """)
        return True
    except Exception as e:
        log_error(f"Failed to prepare News Engine tables: {e}")
        return False
    finally:
        conn.close()


def fetch_news_events():
    """Fetch upcoming economic-calendar events from Financial Modeling Prep."""
    if not FMP_API_KEY:
        return {"events": [], "provider_configured": False,
                "reason": "FMP_API_KEY not configured."}
    try:
        today = datetime.utcnow().date()
        week_from_now = today + timedelta(days=7)
        url = "https://financialmodelingprep.com/stable/economic-calendar"
        params = {"from": today.isoformat(), "to": week_from_now.isoformat(),
                  "apikey": FMP_API_KEY}
        response = requests.get(url, params=params, timeout=15)
        if response.status_code != 200:
            log_error(f"FMP request failed: HTTP {response.status_code} - {response.text[:500]}")
            return {"events": [], "provider_configured": True,
                    "reason": f"FMP request failed (HTTP {response.status_code})."}
        data = response.json()
        if not isinstance(data, list):
            log_error("Unexpected FMP response format (expected a list).")
            return {"events": [], "provider_configured": True,
                    "reason": "Unexpected FMP response format."}
        wanted_currencies = {"USD", "EUR", "GBP", "JPY"}
        relevant_events = [event for event in data
                           if event.get("currency") in wanted_currencies]
        log_info(f"FMP fetch successful: {len(relevant_events)} relevant events found.")
        return {"events": relevant_events, "provider_configured": True,
                "reason": "FMP provider active."}
    except Exception as e:
        log_error(f"FMP fetch failed: {e}")
        return {"events": [], "provider_configured": True,
                "reason": f"FMP fetch error: {e}"}


def _event_key(event):
    return "|".join([
        str(event.get("date") or ""),
        str(event.get("country") or ""),
        str(event.get("currency") or ""),
        str(event.get("event") or ""),
    ])


def save_news_events(events):
    """Save fetched events to Supabase/Postgres without duplicate rows."""
    if not events:
        return
    conn = get_db_connection()
    if not conn:
        log_error("News Engine: DATABASE_URL not set - cannot save fetched events.")
        return
    saved_count = 0
    try:
        with conn:
            with conn.cursor() as cur:
                for event in events:
                    forecast = event.get("estimate", event.get("forecast"))
                    cur.execute("""
                        INSERT INTO news_events (
                            event_key, event_date, country, currency, event_name,
                            impact, actual, previous, forecast, source, fetched_at
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, 'FMP', NOW()
                        )
                        ON CONFLICT (event_key) DO UPDATE SET
                            event_date = EXCLUDED.event_date,
                            country = EXCLUDED.country,
                            currency = EXCLUDED.currency,
                            event_name = EXCLUDED.event_name,
                            impact = EXCLUDED.impact,
                            actual = EXCLUDED.actual,
                            previous = EXCLUDED.previous,
                            forecast = EXCLUDED.forecast,
                            fetched_at = NOW()
                    """, (
                        _event_key(event), event.get("date"), event.get("country"),
                        event.get("currency"), event.get("event"), event.get("impact"),
                        str(event.get("actual")) if event.get("actual") is not None else None,
                        str(event.get("previous")) if event.get("previous") is not None else None,
                        str(forecast) if forecast is not None else None,
                    ))
                    saved_count += 1
        log_info(f"Supabase save successful: {saved_count} news events stored/updated.")
    except Exception as e:
        log_error(f"Failed to save fetched news events: {e}")
    finally:
        conn.close()


def update_engine_status(provider_configured, status_message):
    """Write the engine heartbeat to its own table."""
    conn = get_db_connection()
    if not conn:
        log_error("News Engine: DATABASE_URL not set - cannot write heartbeat.")
        return
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE news_engine_state SET
                        status_message = %s,
                        provider_configured = %s,
                        last_heartbeat_at = NOW()
                    WHERE id = 1
                """, (status_message, provider_configured))
    except Exception as e:
        log_error(f"Failed to write News Engine heartbeat: {e}")
    finally:
        conn.close()


def run_cycle():
    """One complete News Engine cycle."""
    try:
        result = fetch_news_events()
        if result["provider_configured"]:
            if result["events"]:
                save_news_events(result["events"])
                status = ("HEALTHY — FMP provider active — "
                          f"{len(result['events'])} upcoming events fetched")
            else:
                status = "HEALTHY — FMP provider active — 0 relevant upcoming events"
        else:
            status = "HEALTHY — Provider not configured — Engine idle"
        log_info(status)
        update_engine_status(result["provider_configured"], status)
    except Exception as e:
        log_error(f"News Engine cycle failed unexpectedly: {e}")
        update_engine_status(False, f"ERROR — {e}")


def news_engine_loop():
    log_info("News Engine background loop started.")
    if not ensure_news_tables():
        log_error("News Engine database setup failed. The loop will continue, but database writes may fail.")
    while True:
        run_cycle()
        time.sleep(NEWS_ENGINE_LOOP_SECONDS)


def start_news_engine_thread():
    """Called once from app.py's start_background_threads()."""
    thread = threading.Thread(target=news_engine_loop, daemon=True, name="news-engine")
    thread.start()
    log_info("News Engine thread launched.")
