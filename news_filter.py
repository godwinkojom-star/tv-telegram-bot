"""
==========================================================
 SmartFX News Engine — Step 4: Currency-Pair Mapping +
 High-Impact News Detection
==========================================================

ARCHITECTURE REQUIREMENT (per Godwin, do not violate):
This module is COMPLETELY INDEPENDENT from the SMC signal-scoring
logic (both V2's smc_analysis.py and V3's smc_analysis_v3.py). It
does not import from them, and neither of them imports from this
file. It never lowers a confidence score, changes signal wording, or
suppresses/blocks a signal automatically. All it does is answer one
question - "is there a relevant high-impact news event near this
pair right now?" - so a future warning/confirmation layer can use
that answer. What that layer eventually DOES with the answer is a
separate decision for later, after V3 is built and tested.

Nothing in this file hard-codes the monitoring window - it's a
module-level default that every function accepts as an optional
override, so the window can change later without editing this file
or anything that calls it.
"""

from datetime import datetime, timezone


# ==========================================================
# CONFIGURABLE DEFAULTS (not hard-coded into the logic below -
# every function takes these as optional parameters too)
# ==========================================================

DEFAULT_WINDOW_BEFORE_MINUTES = 30
DEFAULT_WINDOW_AFTER_MINUTES = 15

# First version: High only, per Godwin's decision. Adding "Medium"
# here later is the entire change needed to include it - nothing
# else in this file needs to know about impact levels.
DEFAULT_MONITORED_IMPACTS = {"High"}


# ==========================================================
# CURRENCY -> PAIR MAPPING
#
# Deliberately explicit, not derived from text-matching a currency
# code against a pair's ticker string. That would be fragile (e.g.
# "USD" happens to appear inside "USDT" by coincidence, "JPY" would
# never appear in "XAU/USD" even though gold can react to major USD
# events the same way forex does) and wouldn't let us make a
# considered call about which relationships are real versus
# incidental. This table IS that considered call, reviewable and
# editable in one place.
# ==========================================================

CURRENCY_PAIR_MAP = {
    "USD": ["EUR/USD", "GBP/USD", "USD/JPY", "XAU/USD", "BTCUSDT", "ETHUSDT", "SOLUSDT"],
    "EUR": ["EUR/USD"],
    "GBP": ["GBP/USD"],
    "JPY": ["USD/JPY"],
    # "All" (e.g. G20 Meetings) deliberately has no mapping - a
    # global/non-currency-specific event isn't tied to any one pair.
}


def get_pairs_for_currency(currency):
    """Which trading pairs a given news currency is considered relevant to."""
    return CURRENCY_PAIR_MAP.get(currency, [])


def get_currencies_for_pair(pair):
    """Reverse lookup: which news currencies matter for a given pair."""
    return [
        currency
        for currency, pairs in CURRENCY_PAIR_MAP.items()
        if pair in pairs
    ]


# ==========================================================
# WINDOW / RELEVANCE LOGIC
# ==========================================================

def _parse_utc(date_utc_str):
    """Parses the /calendar endpoint's date_utc field (e.g.
    '2026-09-04T12:30:00Z') into an aware UTC datetime. Returns None
    on anything unparseable, rather than raising - a single bad event
    in the feed should never crash a check for every other pair."""
    if not date_utc_str:
        return None
    try:
        return datetime.fromisoformat(date_utc_str.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def get_relevant_news_for_pair(
    calendar_events,
    pair,
    reference_time=None,
    window_before_minutes=DEFAULT_WINDOW_BEFORE_MINUTES,
    window_after_minutes=DEFAULT_WINDOW_AFTER_MINUTES,
    monitored_impacts=DEFAULT_MONITORED_IMPACTS,
):
    """
    Pure, read-only lookup. Given the events list already returned by
    the /calendar endpoint and a trading pair, returns every event
    that:
      - is for a currency mapped to this pair (see CURRENCY_PAIR_MAP)
      - has an impact level in monitored_impacts (High only, for now)
      - falls within [event_time - window_before, event_time + window_after]
        of reference_time (defaults to "right now" in UTC)

    Returns a list of dicts, each with the original event plus
    minutes_until (negative if the event already happened, inside the
    after-window) - sorted soonest-first. Empty list if nothing is
    relevant right now. Never raises on malformed input - a bad event
    is skipped, not fatal.

    This function makes NO decision about what to do with the result.
    It doesn't touch confidence, doesn't block anything, doesn't know
    what a "signal" even is. Purely informational.
    """
    if reference_time is None:
        reference_time = datetime.now(timezone.utc)

    relevant_currencies = set(get_currencies_for_pair(pair))
    if not relevant_currencies:
        return []

    results = []

    for event in calendar_events or []:
        try:
            if event.get("currency") not in relevant_currencies:
                continue
            if event.get("impact") not in monitored_impacts:
                continue

            event_time = _parse_utc(event.get("date_utc"))
            if event_time is None:
                continue

            minutes_until = (event_time - reference_time).total_seconds() / 60.0

            in_window = (
                -window_after_minutes <= minutes_until <= window_before_minutes
            )
            if not in_window:
                continue

            results.append({
                **event,
                "minutes_until": round(minutes_until, 1),
            })
        except Exception:
            # A single malformed event should never break the check
            # for every other pair/event - skip and keep going.
            continue

    results.sort(key=lambda e: e["minutes_until"])
    return results


def has_upcoming_high_impact_news(
    calendar_events,
    pair,
    reference_time=None,
    window_before_minutes=DEFAULT_WINDOW_BEFORE_MINUTES,
    window_after_minutes=DEFAULT_WINDOW_AFTER_MINUTES,
    monitored_impacts=DEFAULT_MONITORED_IMPACTS,
):
    """
    Convenience wrapper for the common case: just need a yes/no plus
    the single most relevant event, for a warning/confirmation layer
    to show the user. Still makes no decision - just answers the
    question and hands back the detail.

    Returns (True, event_dict) or (False, None).
    """
    matches = get_relevant_news_for_pair(
        calendar_events, pair, reference_time,
        window_before_minutes, window_after_minutes, monitored_impacts,
    )
    if matches:
        return True, matches[0]
    return False, None
