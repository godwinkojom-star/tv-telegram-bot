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

logger = logging.getLogger("SmartFX.GoldRunner")

_runner_lock = threading.Lock()
_thread: Optional[threading.Thread] = None
_stop_event = threading.Event()
_engine: Optional[PaperEngine] = None
_config = ScalperConfig()
_last_status: Dict[str, Any] = {"state": "NOT_STARTED"}
_event_callbacks: List[Callable[[str, Dict[str, Any]], None]] = []
_loop_seconds = 30  # Gold loop cadence; data itself is freshness-gated


def register_event_callback(cb: Callable[[str, Dict[str, Any]], None]) -> None:
    """Optional: app.py can register to receive Gold events (signal, close, error)."""
    if cb not in _event_callbacks:
        _event_callbacks.append(cb)


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
        with _runner_lock:
            _last_status = {**result, "engine": None}
        return result

    df1, df5 = _bundle_to_frames(bundle)
    if df1 is None or df5 is None or df1.empty or df5.empty:
        result["state"] = "NO_DATA"
        result["reason"] = "frames_empty_after_bundle"
        with _runner_lock:
            _last_status = result
        return result

    with _runner_lock:
        if _engine is None:
            _engine = PaperEngine(_config)
        prev_closed = len(_engine.closed_trades)
        prev_pos = _engine.position.direction if _engine.position else None
        engine_status = _engine.on_bar(df1, df5)
        result.update(engine_status)
        _last_status = result

        # Emit events for Telegram / dashboard
        if engine_status.get("state") == "UPDATED" and _engine.position and prev_pos is None:
            _emit(
                "gold_signal_opened",
                {
                    "direction": _engine.position.direction,
                    "entry": _engine.position.entry_price,
                    "sl": _engine.position.stop_loss,
                    "tp": _engine.position.take_profit,
                    "rr": _engine.position.rr,
                    "risk_percent": float(_config.risk_percent),
                    "balance": _engine.balance,
                },
            )
        if len(_engine.closed_trades) > prev_closed:
            trade = _engine.closed_trades[-1]
            _emit("gold_trade_closed", trade)

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
