"""
==========================================================
 Expiry Follow-Up Tracking — ADDITIVE PATCH for app.py
==========================================================

Purpose: answer "was the SIGNAL_EXPIRY_SECONDS window too short?"
with real data, instead of guessing from one chart (Signal #0146).

This is purely observational. It does NOT:
  - change SIGNAL_EXPIRY_SECONDS or expire_trade()'s existing behavior
  - change what counts as WIN/LOSS/EXPIRED in the signals table
  - touch smc_analysis.py, V2's strategy logic, or V3 at all
  - affect the Aug 19 - Sep 19 comparison baseline in any way

It only ADDS: one new table (expiry_followups, already created),
one new insert at the point a trade expires, and one new lightweight
check running on the existing trade-monitor loop cadence.

Prerequisite (already done): the expiry_followups table exists in
Supabase (created via migration - see project smartfx-db).

------------------------------------------------------------
STEP 1 — Add this constant near SIGNAL_EXPIRY_SECONDS
------------------------------------------------------------
"""

# How much extra (read-only, non-authoritative) time to keep watching
# an already-expired signal, purely to log whether it would have hit
# TP1/SL if we'd kept waiting. Does not affect the real WIN/LOSS/
# EXPIRED result, which is already final by the time this runs.
EXPIRY_FOLLOWUP_WINDOW_HOURS = 48


"""
------------------------------------------------------------
STEP 2 — Add this new function anywhere near expire_trade()
------------------------------------------------------------
"""

def record_expiry_followup(trade):
    """
    Called once, right after a trade is marked EXPIRED. Inserts one
    row into expiry_followups for later (read-only) checking. Wrapped
    in try/except so that if this ever fails, it NEVER blocks or
    breaks the real expire_trade() flow above it - this is purely
    supplementary logging, not a critical path.

    Uses the same get_db_connection() / psycopg pattern as every other
    DB write in this file (db_update_signal_status, etc.) - NOT the
    Supabase Python client, which this project doesn't use.
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


"""
------------------------------------------------------------
STEP 3 — In expire_trade(), add ONE line right after the existing
db_update_signal_status(...) / close_auto_trades_for_signal(...)
calls at the bottom. Nothing above this line changes:
------------------------------------------------------------

    db_update_signal_status(signal_id, "EXPIRED", closed=True)
    close_auto_trades_for_signal(signal_id, "EXPIRED")
    record_expiry_followup(trade)          # <-- ADD THIS LINE ONLY

That's the entire change to expire_trade(). Everything else in that
function is untouched.
"""


"""
------------------------------------------------------------
STEP 4 — Add this new checking function
------------------------------------------------------------
"""

def check_expiry_followups():
    """
    Runs on the existing trade_monitor_loop cadence (see Step 5).
    For every unresolved expiry_followups row, checks the current
    price the same way monitor_trades() already does for live
    trades, and records a late TP1/SL hit if one occurs - or marks
    the row resolved with no hit once the follow-up window runs out.

    Read-only with respect to everything else: never touches the
    signals table, active_trades, or any WIN/LOSS/EXPIRED status.
    Uses the same get_db_connection()/psycopg pattern as the rest of
    this file, not the Supabase client.
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

            # Same forex-candle-range check monitor_trades() already
            # uses for live trades, so a hit that happened and reversed
            # between price-cache refreshes still gets caught here too.
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


"""
------------------------------------------------------------
STEP 5 — In trade_monitor_loop(), add ONE line. Before:
------------------------------------------------------------

    def trade_monitor_loop():
        while True:
            try:
                monitor_trades()
            except Exception as e:
                log_error(f"Trade monitor loop error: {e}")

            update_heartbeat("trade_monitor")
            time.sleep(TRADE_MONITOR_SECONDS)

After (only the try block changes):

    def trade_monitor_loop():
        while True:
            try:
                monitor_trades()
                check_expiry_followups()       # <-- ADD THIS LINE ONLY
            except Exception as e:
                log_error(f"Trade monitor loop error: {e}")

            update_heartbeat("trade_monitor")
            time.sleep(TRADE_MONITOR_SECONDS)

------------------------------------------------------------
That's the complete patch: 1 new constant, 2 new functions, 2 single-
line additions to existing functions. Nothing existing is modified or
removed.
------------------------------------------------------------

HOW TO READ THE RESULTS LATER (once this has been live a while):

    select
        pair, timeframe,
        count(*) filter (where resolved) as resolved_count,
        count(*) filter (where late_hit = 'TP1') as late_tp1_hits,
        count(*) filter (where late_hit = 'SL') as late_sl_hits,
        count(*) filter (where resolved and late_hit is null) as truly_dead,
        round(avg(late_hit_hours_after_expiry) filter (where late_hit = 'TP1'), 1)
            as avg_hours_late_for_tp1
    from expiry_followups
    group by pair, timeframe
    order by late_tp1_hits desc;

If late_tp1_hits is a meaningful chunk of resolved rows, and
avg_hours_late_for_tp1 is small (say, under a few hours), that's a
real signal the expiry window is cutting things off too early. If
late_tp1_hits is rare, or avg_hours_late_for_tp1 is large (many
hours/days later), #0146 was more likely a one-off and 12h is
probably fine as-is.
"""
