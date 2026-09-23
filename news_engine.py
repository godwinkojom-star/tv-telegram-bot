"""
==========================================================
 SmartFX News Engine (replacement)
==========================================================

Fetches High-Impact economic calendar data from the SmartFS
Forex Calendar Service, applies the existing news_filter.py
protection windows (30 min before / 15 min after, High only),
and exposes a clean protection API for app.py.

Completely independent of V2 Gold and V3 strategy scoring.
Does not lower confidence, predict direction, or close trades.
Only answers: "is this pair currently protected from NEW signals?"

Heartbeat is written to news_engine_state so the watchdog and
dashboard can distinguish overall bot health from news-engine health.
"""

from __future__ import annotations

import hashlib
import io
import logging
import os
import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests
from PIL import Image, ImageDraw, ImageFont, ImageFilter, ImageEnhance

import news_filter

logger = logging.getLogger("SmartFX.NewsEngine")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

FOREX_CALENDAR_SERVICE_URL = (
    os.environ.get("FOREX_CALENDAR_SERVICE_URL") or ""
).rstrip("/")

# Loop cadence: heartbeat + light processing every ~60s
NEWS_ENGINE_LOOP_SECONDS = 60

# How long a successful calendar fetch remains "fresh"
CALENDAR_CACHE_TTL_SECONDS = 300  # 5 minutes

# If last successful fetch is older than this, treat data as stale
CALENDAR_STALE_SECONDS = 2 * 3600  # 2 hours

# Request timeout
CALENDAR_REQUEST_TIMEOUT = 15

# Notification lead time (send upcoming alert this many minutes before window)
UPCOMING_ALERT_LEAD_MINUTES = 45  # so operators see it before the 30-min protect window

ASSETS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets")
NEWS_ARTWORK_PATH = os.path.join(ASSETS_DIR, "smartfx_news.png")

# ---------------------------------------------------------------------------
# In-memory state (process lifetime)
# ---------------------------------------------------------------------------

_lock = threading.Lock()
_thread_started = False

_calendar_events: List[Dict[str, Any]] = []
_last_fetch_ok_at: Optional[float] = None  # unix
_last_fetch_error: Optional[str] = None
_last_event_count: int = 0
_engine_status: str = "starting"  # starting | healthy | degraded | error
_status_message: str = "News engine starting"

# In-process dedup for alerts (backup to DB state)
_notified_upcoming: set = set()
_notified_ended: set = set()

# Optional callbacks injected by app.py so we can reuse Telegram / activity_log
# without creating circular imports.
_send_private_message = None
_send_private_photo = None
_db_log_activity = None
_get_db_connection = None
_database_url = None


def configure_callbacks(
    send_private_message=None,
    send_private_photo=None,
    db_log_activity=None,
    get_db_connection=None,
    database_url=None,
):
    """Called once from app.py after its helpers exist."""
    global _send_private_message, _send_private_photo, _db_log_activity
    global _get_db_connection, _database_url
    _send_private_message = send_private_message
    _send_private_photo = send_private_photo
    _db_log_activity = db_log_activity
    _get_db_connection = get_db_connection
    _database_url = database_url


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _event_key(event: Dict[str, Any]) -> str:
    """Stable identity for deduplication across restarts."""
    raw = f"{event.get('date_utc','')}|{event.get('currency','')}|{event.get('event','')}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def _parse_utc(date_utc_str: Optional[str]) -> Optional[datetime]:
    if not date_utc_str:
        return None
    try:
        return datetime.fromisoformat(date_utc_str.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def _font(size: int, bold: bool = False):
    path = (
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
        if bold
        else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
    )
    try:
        return ImageFont.truetype(path, size)
    except Exception:
        return ImageFont.load_default()


def _log_activity(event_type: str, message: str):
    if _db_log_activity:
        try:
            _db_log_activity(event_type, message)
        except Exception as e:
            logger.warning("activity_log write failed: %s", e)
    logger.info("[%s] %s", event_type, message)


# ---------------------------------------------------------------------------
# Database: news_engine_state + minimal notification dedup
# ---------------------------------------------------------------------------

def ensure_news_tables():
    """Create news_engine_state and news_notification_state if missing.
    Additive only — never drops or alters unrelated tables.
    """
    if not _database_url or not _get_db_connection:
        return
    try:
        conn = _get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS news_engine_state (
                        id INTEGER PRIMARY KEY DEFAULT 1,
                        last_heartbeat_at TIMESTAMPTZ,
                        status_message TEXT,
                        engine_status TEXT,
                        last_fetch_ok_at TIMESTAMPTZ,
                        last_fetch_error TEXT,
                        last_event_count INTEGER,
                        updated_at TIMESTAMPTZ
                    )
                """)
                cur.execute("""
                    INSERT INTO news_engine_state (id, status_message, engine_status, updated_at)
                    VALUES (1, 'initialized', 'starting', NOW())
                    ON CONFLICT (id) DO NOTHING
                """)
                # Minimal persistent dedup for upcoming / ended alerts
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS news_notification_state (
                        event_key TEXT NOT NULL,
                        notification_type TEXT NOT NULL,
                        notified_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        PRIMARY KEY (event_key, notification_type)
                    )
                """)
        conn.close()
        logger.info("news_engine_state and news_notification_state ready")
    except Exception as e:
        logger.error("Failed to ensure news tables: %s", e)


def _write_heartbeat():
    """Persist current engine health. Called every loop iteration."""
    if not _database_url or not _get_db_connection:
        return
    with _lock:
        status = _engine_status
        msg = _status_message
        last_ok = _last_fetch_ok_at
        last_err = _last_fetch_error
        count = _last_event_count

    last_ok_dt = (
        datetime.fromtimestamp(last_ok, tz=timezone.utc) if last_ok else None
    )
    try:
        conn = _get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO news_engine_state (
                        id, last_heartbeat_at, status_message, engine_status,
                        last_fetch_ok_at, last_fetch_error, last_event_count, updated_at
                    ) VALUES (1, NOW(), %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (id) DO UPDATE SET
                        last_heartbeat_at = NOW(),
                        status_message = EXCLUDED.status_message,
                        engine_status = EXCLUDED.engine_status,
                        last_fetch_ok_at = EXCLUDED.last_fetch_ok_at,
                        last_fetch_error = EXCLUDED.last_fetch_error,
                        last_event_count = EXCLUDED.last_event_count,
                        updated_at = NOW()
                """, (msg, status, last_ok_dt, last_err, count))
        conn.close()
    except Exception as e:
        logger.error("news_engine_state heartbeat write failed: %s", e)


def _already_notified(event_key: str, notification_type: str) -> bool:
    """Check persistent + in-memory dedup."""
    with _lock:
        if notification_type == "upcoming" and event_key in _notified_upcoming:
            return True
        if notification_type == "ended" and event_key in _notified_ended:
            return True

    if not _database_url or not _get_db_connection:
        return False
    try:
        conn = _get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM news_notification_state WHERE event_key = %s AND notification_type = %s",
                    (event_key, notification_type),
                )
                row = cur.fetchone()
        conn.close()
        if row:
            with _lock:
                if notification_type == "upcoming":
                    _notified_upcoming.add(event_key)
                else:
                    _notified_ended.add(event_key)
            return True
    except Exception as e:
        logger.warning("notification state read failed: %s", e)
    return False


def _mark_notified(event_key: str, notification_type: str):
    with _lock:
        if notification_type == "upcoming":
            _notified_upcoming.add(event_key)
        else:
            _notified_ended.add(event_key)

    if not _database_url or not _get_db_connection:
        return
    try:
        conn = _get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO news_notification_state (event_key, notification_type, notified_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (event_key, notification_type) DO NOTHING
                """, (event_key, notification_type))
        conn.close()
    except Exception as e:
        logger.warning("notification state write failed: %s", e)


# ---------------------------------------------------------------------------
# Calendar fetch
# ---------------------------------------------------------------------------

def fetch_calendar_events() -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    GET {FOREX_CALENDAR_SERVICE_URL}/calendar
    Returns (events_list, error_message_or_None).
    Never raises.
    """
    if not FOREX_CALENDAR_SERVICE_URL:
        return [], "FOREX_CALENDAR_SERVICE_URL is not set"

    url = f"{FOREX_CALENDAR_SERVICE_URL}/calendar"
    try:
        resp = requests.get(url, timeout=CALENDAR_REQUEST_TIMEOUT)
        if resp.status_code != 200:
            return [], f"HTTP {resp.status_code}: {resp.text[:200]}"
        data = resp.json()
        if not data.get("success"):
            return [], f"service returned success=false: {str(data)[:200]}"
        events = data.get("events") or []
        if not isinstance(events, list):
            return [], "events field is not a list"
        return events, None
    except requests.Timeout:
        return [], "calendar request timed out"
    except requests.RequestException as e:
        return [], f"network error: {e}"
    except Exception as e:
        return [], f"unexpected fetch error: {e}"


def _refresh_calendar_if_needed(force: bool = False) -> None:
    global _calendar_events, _last_fetch_ok_at, _last_fetch_error
    global _last_event_count, _engine_status, _status_message

    with _lock:
        last_ok = _last_fetch_ok_at
        age = (time.time() - last_ok) if last_ok else 999999

    if not force and last_ok and age < CALENDAR_CACHE_TTL_SECONDS:
        return

    events, err = fetch_calendar_events()

    with _lock:
        if err is None:
            _calendar_events = events
            _last_fetch_ok_at = time.time()
            _last_fetch_error = None
            _last_event_count = len(events)
            _engine_status = "healthy"
            _status_message = f"ok — {len(events)} events"
            logger.info("Calendar fetch OK: %d events", len(events))
        else:
            _last_fetch_error = err
            # Keep previous events if we have any; mark degraded/error
            if _last_fetch_ok_at and (time.time() - _last_fetch_ok_at) < CALENDAR_STALE_SECONDS:
                _engine_status = "degraded"
                _status_message = f"using cached data — last error: {err}"
            else:
                _engine_status = "error"
                _status_message = f"no usable calendar data — {err}"
            logger.warning("Calendar fetch failed: %s", err)
            _log_activity("news_calendar_fetch_failed", f"Calendar fetch failed: {err}")


# ---------------------------------------------------------------------------
# Public protection API (used by app.py)
# ---------------------------------------------------------------------------

def get_calendar_snapshot() -> List[Dict[str, Any]]:
    """Thread-safe copy of the latest events list."""
    with _lock:
        return list(_calendar_events)


def is_calendar_usable() -> bool:
    """True if we have data that is not critically stale."""
    with _lock:
        if not _calendar_events:
            return False
        if _last_fetch_ok_at is None:
            return False
        return (time.time() - _last_fetch_ok_at) < CALENDAR_STALE_SECONDS


def is_pair_protected(pair: str, reference_time: Optional[datetime] = None) -> bool:
    """
    True when the pair must NOT receive a NEW signal.

    Fail-safe: if calendar data is missing or critically stale,
    return True (block) so we never silently bypass protection.
    """
    if not is_calendar_usable():
        return True

    events = get_calendar_snapshot()
    has_news, _ = news_filter.has_upcoming_high_impact_news(
        events, pair, reference_time=reference_time
    )
    return bool(has_news)


def get_protection_info(
    pair: str, reference_time: Optional[datetime] = None
) -> Optional[Dict[str, Any]]:
    """
    Returns detail dict when protected, else None.

    Keys: event, currency, impact, date_utc, minutes_until,
          affected_pairs, reason
    """
    if not is_calendar_usable():
        return {
            "event": "Calendar unavailable",
            "currency": "—",
            "impact": "High",
            "date_utc": None,
            "minutes_until": None,
            "affected_pairs": [pair],
            "reason": "NEWS PROTECTION — calendar data unavailable or stale (fail-safe block)",
            "fail_safe": True,
        }

    events = get_calendar_snapshot()
    has_news, match = news_filter.has_upcoming_high_impact_news(
        events, pair, reference_time=reference_time
    )
    if not has_news or not match:
        return None

    currency = match.get("currency", "?")
    affected = news_filter.get_pairs_for_currency(currency)
    minutes = match.get("minutes_until")
    reason = (
        f"NEWS PROTECTION — {pair} blocked\n"
        f"Reason: High Impact {currency} event\n"
        f"Event: {match.get('event', '?')}\n"
        f"Time: {match.get('date_utc', '?')}\n"
        f"Protection window: active"
    )
    return {
        "event": match.get("event"),
        "currency": currency,
        "impact": match.get("impact"),
        "date_utc": match.get("date_utc"),
        "minutes_until": minutes,
        "affected_pairs": affected,
        "forecast": match.get("forecast") or "",
        "previous": match.get("previous") or "",
        "actual": match.get("actual"),
        "reason": reason,
        "fail_safe": False,
        "raw": match,
    }


def get_engine_status() -> Dict[str, Any]:
    with _lock:
        return {
            "engine_status": _engine_status,
            "status_message": _status_message,
            "last_fetch_ok_at": _last_fetch_ok_at,
            "last_fetch_error": _last_fetch_error,
            "last_event_count": _last_event_count,
            "calendar_usable": is_calendar_usable(),
        }


# ---------------------------------------------------------------------------
# News alert cards (Pillow)
# ---------------------------------------------------------------------------

def _generate_news_card(
    title: str,
    subtitle: str,
    event: Dict[str, Any],
    footer: str,
    accent: Tuple[int, int, int] = (255, 120, 40),
) -> bytes:
    """
    Build a branded SmartFX NEWS ALERT card using the local artwork.
    Returns PNG bytes.
    """
    W, H = 1200, 800
    img = Image.new("RGBA", (W, H), (8, 10, 14, 255))

    # Artwork (right side / background)
    try:
        art = Image.open(NEWS_ARTWORK_PATH).convert("RGBA")
        # Scale to cover height, keep aspect
        ratio = H / art.height
        new_w = int(art.width * ratio)
        art = art.resize((new_w, H), Image.LANCZOS)
        # Place on right, slightly dimmed
        enhancer = ImageEnhance.Brightness(art)
        art = enhancer.enhance(0.75)
        x_off = W - new_w + 40
        img.paste(art, (x_off, 0), art)
    except Exception as e:
        logger.warning("Could not load news artwork: %s", e)

    # Dark gradient panel on left for readability
    draw = ImageDraw.Draw(img)
    for x in range(0, 620):
        alpha = int(230 * (1 - x / 700))
        draw.line([(x, 0), (x, H)], fill=(6, 8, 12, min(255, 200 + alpha // 2)))

    # Accent bar
    draw.rectangle([0, 0, 8, H], fill=accent + (255,))

    # Header
    draw.text((40, 36), "SMARTFX NEWS ALERT", font=_font(28, bold=True), fill=(240, 240, 240, 255))
    draw.rounded_rectangle([40, 80, 280, 120], radius=10, fill=accent + (230,))
    draw.text((160, 100), subtitle.upper(), font=_font(18, bold=True), fill=(15, 15, 15, 255), anchor="mm")

    # Event block
    y = 160
    currency = event.get("currency") or "—"
    name = event.get("event") or "High Impact Event"
    draw.text((40, y), currency, font=_font(42, bold=True), fill=accent + (255,))
    y += 55
    # Wrap long event names
    max_chars = 32
    if len(name) > max_chars:
        parts = []
        words = name.split()
        line = ""
        for w in words:
            if len(line) + len(w) + 1 <= max_chars:
                line = (line + " " + w).strip()
            else:
                if line:
                    parts.append(line)
                line = w
        if line:
            parts.append(line)
        for p in parts[:3]:
            draw.text((40, y), p, font=_font(26, bold=True), fill=(245, 245, 245, 255))
            y += 34
    else:
        draw.text((40, y), name, font=_font(26, bold=True), fill=(245, 245, 245, 255))
        y += 40

    y += 12
    date_utc = event.get("date_utc") or "—"
    minutes = event.get("minutes_until")
    if minutes is not None:
        if minutes >= 0:
            time_line = f"{date_utc}  ·  in {abs(minutes):.0f} min"
        else:
            time_line = f"{date_utc}  ·  {abs(minutes):.0f} min ago"
    else:
        time_line = str(date_utc)
    draw.text((40, y), time_line, font=_font(18), fill=(170, 180, 190, 255))
    y += 40

    # Forecast / Previous
    forecast = (event.get("forecast") or "").strip()
    previous = (event.get("previous") or "").strip()
    if forecast or previous:
        draw.text((40, y), f"Forecast: {forecast or '—'}", font=_font(18), fill=(200, 200, 200, 255))
        y += 28
        draw.text((40, y), f"Previous: {previous or '—'}", font=_font(18), fill=(200, 200, 200, 255))
        y += 36

    # Affected pairs
    affected = event.get("affected_pairs") or []
    if affected:
        draw.text((40, y), "Affected pairs", font=_font(16, bold=True), fill=(140, 150, 160, 255))
        y += 26
        pairs_text = "  ·  ".join(affected[:8])
        draw.text((40, y), pairs_text, font=_font(17), fill=(220, 220, 220, 255))
        y += 40

    # Footer banner
    draw.rounded_rectangle([40, H - 110, 580, H - 40], radius=14, outline=accent + (200,), width=2)
    draw.text((310, H - 75), footer, font=_font(18, bold=True), fill=accent + (255,), anchor="mm")

    # Soft outer glow border
    try:
        border = Image.new("RGBA", img.size, (0, 0, 0, 0))
        bd = ImageDraw.Draw(border)
        bd.rounded_rectangle([4, 4, W - 5, H - 5], radius=18, outline=accent + (80,), width=6)
        border = border.filter(ImageFilter.GaussianBlur(8))
        img = Image.alpha_composite(img, border)
    except Exception:
        pass

    buf = io.BytesIO()
    img.convert("RGB").save(buf, format="PNG", optimize=True)
    return buf.getvalue()


def generate_upcoming_news_card(event: Dict[str, Any]) -> bytes:
    return _generate_news_card(
        title="SMARTFX NEWS ALERT",
        subtitle="HIGH IMPACT",
        event=event,
        footer="NEWS PROTECTION ACTIVE",
        accent=(255, 110, 30),
    )


def generate_protection_ended_card(event: Dict[str, Any]) -> bytes:
    return _generate_news_card(
        title="SMARTFX NEWS ALERT",
        subtitle="PROTECTION ENDED",
        event=event,
        footer="V2 / V3 NEWS PROTECTION OFF",
        accent=(60, 200, 120),
    )


# ---------------------------------------------------------------------------
# Notification cycle
# ---------------------------------------------------------------------------

def _process_notifications():
    """Detect approaching / ending High-Impact events and notify once."""
    if not is_calendar_usable():
        return

    events = get_calendar_snapshot()
    now = _now_utc()
    high_events = [e for e in events if e.get("impact") == "High"]

    for ev in high_events:
        event_time = _parse_utc(ev.get("date_utc"))
        if event_time is None:
            continue

        minutes_until = (event_time - now).total_seconds() / 60.0
        key = _event_key(ev)
        currency = ev.get("currency") or "?"
        affected = news_filter.get_pairs_for_currency(currency)

        # Enrich for card
        card_event = {
            **ev,
            "minutes_until": round(minutes_until, 1),
            "affected_pairs": affected,
        }

        # Upcoming: within lead time and still before/at event, inside or about to enter window
        # Window starts at -30 min relative to event (i.e. minutes_until <= 30)
        # We alert from UPCOMING_ALERT_LEAD_MINUTES down to just after the window opens
        if 0 <= minutes_until <= UPCOMING_ALERT_LEAD_MINUTES:
            if not _already_notified(key, "upcoming"):
                _send_upcoming_alert(card_event, key)

        # Ended: protection window has finished (minutes_until < -15)
        # Alert once shortly after the window closes
        if -25 <= minutes_until < -news_filter.DEFAULT_WINDOW_AFTER_MINUTES:
            if not _already_notified(key, "ended"):
                _send_ended_alert(card_event, key)


def _send_upcoming_alert(event: Dict[str, Any], key: str):
    try:
        img_bytes = generate_upcoming_news_card(event)
        caption = (
            f"*SMARTFX NEWS ALERT*\n"
            f"High Impact — {event.get('currency')} — {event.get('event')}\n"
            f"Time: {event.get('date_utc')}\n"
            f"NEWS PROTECTION ACTIVE for relevant pairs."
        )
        sent = False
        if _send_private_photo:
            sent = _send_private_photo(img_bytes, caption=caption)
        if not sent and _send_private_message:
            _send_private_message(caption)

        _mark_notified(key, "upcoming")
        _log_activity(
            "news_upcoming_alert",
            f"Upcoming High Impact: {event.get('currency')} {event.get('event')} at {event.get('date_utc')}",
        )
    except Exception as e:
        logger.error("Upcoming news alert failed: %s", e)


def _send_ended_alert(event: Dict[str, Any], key: str):
    try:
        img_bytes = generate_protection_ended_card(event)
        caption = (
            f"*NEWS PROTECTION ENDED*\n"
            f"{event.get('currency')} — {event.get('event')}\n"
            f"V2/V3 NEWS PROTECTION OFF\n"
            f"Protection window completed. SmartFX can resume normal signal scanning."
        )
        sent = False
        if _send_private_photo:
            sent = _send_private_photo(img_bytes, caption=caption)
        if not sent and _send_private_message:
            _send_private_message(caption)

        _mark_notified(key, "ended")
        _log_activity(
            "news_protection_ended",
            f"Protection ended: {event.get('currency')} {event.get('event')}",
        )
    except Exception as e:
        logger.error("Protection-ended alert failed: %s", e)


# ---------------------------------------------------------------------------
# Background loop
# ---------------------------------------------------------------------------

def _news_engine_loop():
    logger.info("News engine loop started (interval=%ss)", NEWS_ENGINE_LOOP_SECONDS)
    _log_activity("news_engine_started", "News engine background loop started")

    # First fetch immediately
    _refresh_calendar_if_needed(force=True)
    _write_heartbeat()

    while True:
        try:
            _refresh_calendar_if_needed(force=False)
            _process_notifications()
            _write_heartbeat()
        except Exception as e:
            logger.error("News engine loop iteration error: %s", e)
            with _lock:
                _engine_status = "error"
                _status_message = f"loop error: {e}"
            try:
                _write_heartbeat()
            except Exception:
                pass

        time.sleep(NEWS_ENGINE_LOOP_SECONDS)


def start_news_engine_thread():
    """Start the daemon background thread (idempotent)."""
    global _thread_started
    if _thread_started:
        return
    _thread_started = True

    ensure_news_tables()

    t = threading.Thread(target=_news_engine_loop, name="news-engine", daemon=True)
    t.start()
    logger.info("News engine thread launched")


# ---------------------------------------------------------------------------
# Convenience for tests / manual refresh
# ---------------------------------------------------------------------------

def force_refresh():
    _refresh_calendar_if_needed(force=True)
    return get_engine_status()
