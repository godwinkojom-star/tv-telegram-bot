"""Isolated Gold Reversal Scalper runner.

Paper-only. Starts at most one background thread.
Uses BiQuote for Gold data to avoid Twelve Data credit pressure.
Publishes status for app.py Telegram / dashboard integration.
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from .biquote_adapter import get_gold_bundle
from .config import ScalperConfig
from .data_contract import classify_bundle
from .paper_engine import PaperEngine
from .performance import summarize
from .strategy import evaluate

logger = logging.getLogger("SmartFX.GoldRunner")

_runner_lock = threading.Lock()
_thread: Optional[threading.Thread] = None
_stop_event = threading.Event()
_engine: Optional[PaperEngine] = None
_config = ScalperConfig()
_last_status: Dict[str, Any] = {"state": "NOT_STARTED"}
_event_callbacks: List[Callable[[str, Dict[str, Any]], None]] = []
_position_loader: Optional[Callable[[], Dict[str, Any]]] = None
_loop_seconds = 30  # Gold loop cadence; data itself is freshness-gated


def register_event_callback(cb: Callable[[str, Dict[str, Any]], None]) -> None:
    """Register an app callback for persisted signal/trade lifecycle events."""
    if cb not in _event_callbacks:
        _event_callbacks.append(cb)


def set_position_loader(loader: Optional[Callable[[], Dict[str, Any]]]) -> None:
    """Provide a DB-backed loader so an open paper position survives restarts."""
    global _position_loader
    _position_loader = loader


def _emit(event_type: str, payload: Dict[str, Any]) -> None:
    for cb in list(_event_callbacks):
        try:
            cb(event_type, payload)
        except Exception as e:
            logger.warning("Gold event callback failed: %s", e)


def set_risk_percent(risk_percent: float) -> Dict[str, Any]:
    global _config, _engine
    with _runner_lock:
        _config = _config.with_risk(risk_percent)
        if _engine is not None:
            _engine.set_risk_percent(risk_percent)
        return {"ok": True, "risk_percent": float(_config.risk_percent)}


def get_status() -> Dict[str, Any]:
    with _runner_lock:
        base = dict(_last_status)
        if _engine is not None:
            st = _engine.status(base.get("state", "UPDATED"))
            base.update(st)
            base["closed_trade_records"] = list(_engine.closed_trades[-50:])
            base["performance"] = summarize(_engine.closed_trades)
        base["risk_percent"] = float(_config.risk_percent)
        base["point_value"] = float(_config.point_value)
        base["runner_alive"] = _thread is not None and _thread.is_alive()
        return base


def get_closed_trades(limit: int = 100) -> List[dict]:
    with _runner_lock:
        if _engine is None:
            return []
        return list(_engine.closed_trades[-limit:])


def _bundle_to_frames(bundle: Dict[str, Any]):
    one = bundle.get("entry")
    five = bundle.get("confirmation")
    df1 = getattr(one, "df", None)
    df5 = getattr(five, "df", None)
    if df1 is None and one is not None and getattr(one, "candles", None):
        df1 = pd.DataFrame(one.candles)
    if df5 is None and five is not None and getattr(five, "candles", None):
        df5 = pd.DataFrame(five.candles)
    return df1, df5


def _progress_for_evaluation(result: Dict[str, Any]) -> Dict[str, Any]:
    """Report completed strategy gates, never an estimated win probability."""
    if result.get("status") == "SIGNAL":
        return {"progress_pct": 100, "stage": "Signal confirmed · 1m reversal + 5m confirmation", "status": "SIGNAL"}
    reason = str(result.get("reason") or "no_signal")
    if reason.startswith("indicator_error"):
        pct, stage = 5, "Preparing indicators"
    elif reason in ("insufficient_1m_history", "insufficient_5m_history"):
        pct, stage = 10, "Building candle history"
    elif reason in ("invalid_volatility_data", "volatility_too_low", "volatility_expansion_too_high"):
        pct, stage = 25, "Checking volatility conditions"
    elif reason == "5m_confirmation_neutral":
        pct, stage = 35, "Waiting for 5m direction confirmation"
    elif reason == "no_confirmed_1m_reversal":
        pct, stage = 45, "Waiting for a confirmed 1m reversal"
    elif reason == "5m_confirmation_disagrees":
        pct, stage = 55, "1m reversal found · waiting for 5m agreement"
    elif reason in ("ema_structure_too_flat", "missing_atr"):
        pct, stage = 65, "Checking market structure / chop filter"
    elif reason in ("buy_momentum_not_confirmed", "sell_momentum_not_confirmed"):
        pct, stage = 75, "Checking reversal momentum"
    elif reason == "no_recent_directional_momentum":
        pct, stage = 82, "Waiting for directional momentum"
    elif reason in ("entry_too_far_from_fast_ema", "entry_overextended"):
        pct, stage = 90, "Reversal found · entry quality rejected"
    elif reason == "stop_distance_outside_allowed_range":
        pct, stage = 96, "Checking stop-loss distance"
    else:
        pct, stage = 15, "Scanning · no valid setup yet"
    return {"progress_pct": pct, "stage": stage, "status": "NO_SETUP"}


def _run_once() -> Dict[str, Any]:
    global _last_status, _engine
    bundle = get_gold_bundle(allow_refresh=True)
    status = classify_bundle(
        bundle,
        min_1m_bars=_config.minimum_1m_bars,
        min_5m_bars=_config.minimum_5m_bars,
    )
    result = {
        "state": status.state,
        "reason": status.reason,
        "ready_for_signal": status.ready_for_signal,
        "details": status.details,
        "provider": bundle.get("provider"),
        "meta_1m": bundle.get("meta_1m"),
        "meta_5m": bundle.get("meta_5m"),
    }
    if not status.ready_for_signal:
        raw_state = str(status.state or "NO_DATA").upper()
        if raw_state in ("NO_DATA", "WAIT_FOR_FRESH_DATA", "STALE_DATA", "INSUFFICIENT_CANDLES", "INSUFFICIENT_HISTORY"):
            progress_pct = 0 if raw_state in ("NO_DATA", "STALE_DATA", "WAIT_FOR_FRESH_DATA") else 10
            stage = {
                "NO_DATA": "Waiting for Gold market data",
                "STALE_DATA": "Waiting for fresh 1m / 5m candles",
                "WAIT_FOR_FRESH_DATA": "Waiting for fresh 1m / 5m candles",
                "INSUFFICIENT_CANDLES": "Building 1m / 5m candle history",
                "INSUFFICIENT_HISTORY": "Building 1m / 5m candle history",
            }.get(raw_state, "Waiting for Gold market data")
            scan_status = "WAITING_DATA" if progress_pct == 0 else "SCANNING"
        else:
            progress_pct, stage, scan_status = 0, status.reason or "Gold data not ready", "ERROR"
        details = status.details or {}
        progress_payload = {
            "status": scan_status, "progress_pct": progress_pct, "stage": stage,
            "reason": status.reason, "snapshot_status": raw_state,
            "candles_1m": details.get("1m_bars"), "candles_5m": details.get("5m_bars"),
            "direction": None, "last_update": pd.Timestamp.utcnow().isoformat(),
        }
        result.update(progress_payload)
        with _runner_lock:
            _last_status = {**result, "engine": None}
        _emit("gold_scan_update", progress_payload)
        return result

    df1, df5 = _bundle_to_frames(bundle)
    if df1 is None or df5 is None or df1.empty or df5.empty:
        result["state"] = "NO_DATA"
        result["reason"] = "frames_empty_after_bundle"
        with _runner_lock:
            _last_status = result
        return result

    diagnostic = evaluate(df1, df5, _config)
    progress = _progress_for_evaluation(diagnostic)
    progress_payload = {
        **progress,
        "reason": diagnostic.get("reason") or "confirmed_reversal",
        "direction": diagnostic.get("direction"),
        "snapshot_status": status.state,
        "candles_1m": len(df1),
        "candles_5m": len(df5),
        "last_update": pd.Timestamp.utcnow().isoformat(),
    }

    with _runner_lock:
        if _engine is None:
            _engine = PaperEngine(_config)
            if _position_loader is not None:
                try:
                    persisted = _position_loader() or {}
                    _engine.restore_state(persisted)
                except Exception as restore_error:
                    logger.exception("Could not restore persisted Gold paper position: %s", restore_error)
        prev_closed = len(_engine.closed_trades)
        prev_trade_id = _engine.position.trade_id if _engine.position else None
        engine_status = _engine.on_bar(df1, df5)
        result.update(engine_status)
        result.update(progress_payload)
        result["engine"] = engine_status
        _last_status = result
        _emit("gold_scan_update", progress_payload)

        # Emit events for Telegram / dashboard
        if len(_engine.closed_trades) > prev_closed:
            for trade in _engine.closed_trades[prev_closed:]:
                _emit("gold_trade_closed", trade)
        if _engine.position and _engine.position.trade_id != prev_trade_id:
            p = _engine.position
            _emit("gold_signal_opened", {
                "trade_id": p.trade_id,
                "direction": p.direction,
                "entry": p.entry_price,
                "sl": p.stop_loss,
                "risk_amount": p.risk_amount,
                "risk_percent": float(_config.risk_percent),
                "quantity": p.quantity,
                "point_value": float(_config.point_value),
                "initial_risk_per_unit": p.initial_risk_per_unit,
                "balance_before": p.initial_balance,
                "entry_time": str(p.entry_time),
                "hold_bars": p.hold_bars,
                "bar_time": str(df1.index[-1]),
            })
        if _engine.position:
            p = _engine.position
            _emit("gold_trade_progress", {
                "trade_id": p.trade_id,
                "direction": p.direction,
                "current_price": float(df1.iloc[-1]["close"]),
                "unrealized_pnl": p.unrealized_pnl,
                "current_rr": p.current_rr,
                "max_favorable_r": p.max_favorable_r,
                "reached_milestones": list(p.reached_milestones),
                "hold_bars": p.hold_bars,
                "bar_time": str(df1.index[-1]),
            })
        # Checkpoint only after any close/open events for this bar have been saved.
        _emit("gold_bar_processed", {"bar_time": str(df1.index[-1])})

    return result


def _loop():
    logger.info("Gold Reversal Scalper runner loop started (paper-only)")
    while not _stop_event.is_set():
        try:
            _run_once()
        except Exception as e:
            logger.exception("Gold runner iteration failed: %s", e)
            with _runner_lock:
                _last_status = {"state": "ERROR", "reason": str(e)}
            _emit("gold_runner_error", {"error": str(e)})
        _stop_event.wait(_loop_seconds)
    logger.info("Gold Reversal Scalper runner loop stopped")


def start_gold_runner(loop_seconds: int = 30) -> bool:
    """Idempotent start. Returns True if a new thread was started."""
    global _thread, _loop_seconds
    with _runner_lock:
        if _thread is not None and _thread.is_alive():
            return False
        _loop_seconds = max(15, int(loop_seconds))
        _stop_event.clear()
        _thread = threading.Thread(target=_loop, name="GoldScalperRunner", daemon=True)
        _thread.start()
        logger.info("Gold runner thread started")
        return True


def stop_gold_runner() -> None:
    _stop_event.set()
