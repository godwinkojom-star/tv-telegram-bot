"""SmartFX shared OHLC-cache adapter for the experimental scalper.

This module is deliberately read-only with respect to market-data ownership.
It does not contain a Twelve Data/Kraken API key, does not call a provider
endpoint directly, and does not create a second scheduler.

The live app remains the single owner of market-data fetching, rate limiting,
cache TTLs, and the in-flight dedupe lock.  The scalper consumes that existing
cache through the app's public get_candles() function.
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
    """Read a snapshot from SmartFX's existing cache.

    By default this function NEVER asks the provider for new data.  This is
    the important safety property for the experimental strategy: adding the
    scalper cannot silently add Twelve Data calls.

    ``allow_refresh=True`` is intentionally explicit.  It calls the existing
    app ``get_candles(..., use_cache=True)`` path, so the app still owns the
    provider request, TTL, rate limiter, and in-flight lock.  The adapter
    itself never knows or stores an API key.
    """
    cache: Dict[str, Any] = getattr(app_module, "ohlc_cache", {})
    key = _cache_key(pair, timeframe, market_type)
    item = cache.get(key)

    if not item:
        if allow_refresh:
            return _refresh_via_app(
                app_module, pair, timeframe, market_type, max_age_seconds
            )
        return CacheSnapshot(
            pair, timeframe, market_type, [], None, None, "NO_DATA"
        )

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

    return CacheSnapshot(
        pair, timeframe, market_type, candles, fetched_at, age, state
    )


def _refresh_via_app(
    app_module: Any,
    pair: str,
    timeframe: str,
    market_type: str,
    max_age_seconds: Optional[float],
) -> CacheSnapshot:
    """Use the app's existing fetch path, then re-read the cache."""
    getter: Optional[Callable[..., Any]] = getattr(app_module, "get_candles", None)
    if getter is None:
        return CacheSnapshot(pair, timeframe, market_type, [], None, None, "NO_DATA")

    try:
        # Critical: this is the existing SmartFX data owner.  No provider URL
        # or key exists anywhere in this module.
        getter(pair, timeframe, market_type, use_cache=True)
    except Exception:
        # The adapter never turns a data-provider exception into a second
        # request.  The caller can treat the resulting snapshot as NO_DATA.
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
    """Return the Gold 1m/5m bundle for the experimental scalper.

    This stage is intentionally conservative: it reports freshness instead
    of pretending that an old candle is live.  It also does not make a hidden
    provider request unless the caller explicitly sets ``allow_refresh``.
    """
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
