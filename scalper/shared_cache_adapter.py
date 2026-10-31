"""SmartFX shared OHLC-cache adapter for the experimental scalper.

Read-only with respect to market-data ownership. The live app remains
the single owner of market-data fetching for the main strategy.
Gold primary path uses biquote_adapter; this module remains available
as a secondary/fallback reader of the app cache.
"""

from __future__ import annotations

from dataclasses import dataclass
from time import time
from typing import Any, Callable, Dict, Optional


@dataclass(frozen=True)
class CacheSnapshot:
    pair: str
    timeframe: str
    market_type: str
    candles: list
    fetched_at: Optional[float]
    age_seconds: Optional[float]
    state: str
    source: str = "smartfx_shared_ohlc_cache"

    @property
    def is_fresh(self) -> bool:
        return self.state == "FRESH"

    @property
    def is_usable(self) -> bool:
        return self.state in {"FRESH", "STALE"} and bool(self.candles)


def _cache_key(pair: str, timeframe: str, market_type: str) -> str:
    return f"{pair}_{timeframe}_{market_type}"


def _normalise_candles(value: Any) -> list:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    try:
        return list(value)
    except Exception:
        return []


def get_snapshot(
    app_module: Any,
    pair: str = "XAU/USD",
    timeframe: str = "1m",
    market_type: str = "forex",
    *,
    max_age_seconds: Optional[float] = None,
    allow_refresh: bool = False,
) -> CacheSnapshot:
    cache: Dict[str, Any] = getattr(app_module, "ohlc_cache", {})
    key = _cache_key(pair, timeframe, market_type)
    item = cache.get(key)

    if not item:
        if allow_refresh:
            return _refresh_via_app(app_module, pair, timeframe, market_type, max_age_seconds)
        return CacheSnapshot(pair, timeframe, market_type, [], None, None, "NO_DATA")

    candles = _normalise_candles(item.get("candles"))
    fetched_at = item.get("fetched_at")
    age = None
    if fetched_at is not None:
        try:
            age = max(0.0, time() - float(fetched_at))
        except (TypeError, ValueError):
            age = None

    if not candles:
        state = "NO_DATA"
    elif max_age_seconds is not None and (age is None or age > max_age_seconds):
        state = "STALE"
    else:
        state = "FRESH"

    return CacheSnapshot(pair, timeframe, market_type, candles, fetched_at, age, state)


def _refresh_via_app(
    app_module: Any,
    pair: str,
    timeframe: str,
    market_type: str,
    max_age_seconds: Optional[float],
) -> CacheSnapshot:
    getter: Optional[Callable[..., Any]] = getattr(app_module, "get_candles", None)
    if getter is None:
        return CacheSnapshot(pair, timeframe, market_type, [], None, None, "NO_DATA")
    try:
        getter(pair, timeframe, market_type, use_cache=True)
    except Exception:
        pass
    return get_snapshot(
        app_module,
        pair,
        timeframe,
        market_type,
        max_age_seconds=max_age_seconds,
        allow_refresh=False,
    )


def get_gold_scalper_bundle(
    app_module: Any,
    *,
    max_1m_age_seconds: float = 120.0,
    max_5m_age_seconds: float = 600.0,
    allow_refresh: bool = False,
) -> Dict[str, Any]:
    one = get_snapshot(
        app_module,
        "XAU/USD",
        "1m",
        "forex",
        max_age_seconds=max_1m_age_seconds,
        allow_refresh=allow_refresh,
    )
    five = get_snapshot(
        app_module,
        "XAU/USD",
        "5m",
        "forex",
        max_age_seconds=max_5m_age_seconds,
        allow_refresh=allow_refresh,
    )
    if one.state == "NO_DATA" or five.state == "NO_DATA":
        state = "NO_DATA"
    elif one.state == "STALE" or five.state == "STALE":
        state = "STALE_DATA"
    else:
        state = "READY"
    return {
        "state": state,
        "pair": "XAU/USD",
        "market_type": "forex",
        "entry": one,
        "confirmation": five,
        "uses_shared_cache": True,
        "provider_calls_owned_by_app": True,
    }
