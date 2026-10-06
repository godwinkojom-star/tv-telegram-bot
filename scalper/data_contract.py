"""Small data contract between SmartFX's scanner and the experimental scalper."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict


@dataclass(frozen=True)
class ScalperDataStatus:
    state: str
    reason: str
    ready_for_signal: bool
    details: Dict[str, Any]


def classify_bundle(bundle: Dict[str, Any], min_1m_bars: int = 100, min_5m_bars: int = 100) -> ScalperDataStatus:
    """Convert shared-cache state into an explicit scalper decision.

    The strategy must never interpret stale/short data as a real market signal.
    """
    state = bundle.get("state", "NO_DATA")
    one = bundle.get("entry")
    five = bundle.get("confirmation")
    one_bars = len(getattr(one, "candles", []) or [])
    five_bars = len(getattr(five, "candles", []) or [])

    if state == "NO_DATA":
        return ScalperDataStatus(
            "NO_DATA", "shared cache has no Gold snapshot", False,
            {"1m_bars": one_bars, "5m_bars": five_bars},
        )

    if state == "STALE_DATA":
        return ScalperDataStatus(
            "WAIT_FOR_FRESH_DATA", "Gold 1m/5m snapshot is too old for scalping", False,
            {"1m_age": getattr(one, "age_seconds", None), "5m_age": getattr(five, "age_seconds", None)},
        )

    if one_bars < min_1m_bars or five_bars < min_5m_bars:
        return ScalperDataStatus(
            "INSUFFICIENT_CANDLES", "shared snapshot does not contain enough candles", False,
            {"1m_bars": one_bars, "5m_bars": five_bars},
        )

    return ScalperDataStatus(
        "READY", "shared Gold 1m/5m data is ready for strategy evaluation", True,
        {"1m_bars": one_bars, "5m_bars": five_bars},
    )
