"""Dedicated BiQuote adapter for Gold (XAUUSD) 1m / 5m OHLC.

Designed so the Gold scalper does not consume Twelve Data credits.
Does not hard-code secrets. Optional BIQUOTE_API_KEY env var is
supported but not required by current public endpoints.

Never treats an isOpen=true bar as a completed candle for signals.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

logger = logging.getLogger("SmartFX.GoldBiQuote")

DEFAULT_BASE_URL = (os.environ.get("BIQUOTE_BASE_URL") or "https://biquote.io/api").rstrip("/")
REQUEST_TIMEOUT = float(os.environ.get("BIQUOTE_TIMEOUT", "12"))
# Gold-specific freshness (seconds). Independent of main app OHLC_CACHE_SECONDS.
DEFAULT_1M_MAX_AGE = 90.0
DEFAULT_5M_MAX_AGE = 360.0
DEFAULT_LIMIT = 300

# Simple process-local cache to avoid request storms from dashboard polls.
_cache: Dict[str, Dict[str, Any]] = {}
_cache_lock = __import__("threading").Lock()


def _api_key() -> Optional[str]:
    key = os.environ.get("BIQUOTE_API_KEY")
    return key.strip() if key else None


def _headers() -> Dict[str, str]:
    headers = {"Accept": "application/json", "User-Agent": "SmartFX-GoldScalper/1.0"}
    key = _api_key()
    if key:
        headers["Authorization"] = f"Bearer {key}"
        headers["X-API-Key"] = key
    return headers


def _parse_open_time(value: Any) -> Optional[pd.Timestamp]:
    if value is None:
        return None
    try:
        if isinstance(value, (int, float)):
            # assume ms or s
            v = float(value)
            if v > 1e12:
                v = v / 1000.0
            return pd.Timestamp(datetime.fromtimestamp(v, tz=timezone.utc))
        s = str(value).replace("Z", "+00:00")
        ts = pd.Timestamp(s)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        else:
            ts = ts.tz_convert("UTC")
        return ts
    except Exception:
        return None


def bars_to_dataframe(bars: List[Dict[str, Any]], drop_open: bool = True) -> pd.DataFrame:
    """Convert BiQuote bars list into a sorted OHLC DataFrame indexed by UTC time.

    Drops bars with isOpen=True by default so incomplete candles never drive signals.
    """
    rows = []
    for bar in bars or []:
        if not isinstance(bar, dict):
            continue
        if drop_open and bar.get("isOpen") is True:
            continue
        ts = _parse_open_time(bar.get("openTime") or bar.get("time") or bar.get("timestamp"))
        if ts is None:
            continue
        try:
            o = float(bar["open"])
            h = float(bar["high"])
            l = float(bar["low"])
            c = float(bar["close"])
        except (KeyError, TypeError, ValueError):
            continue
        if not all(map(lambda x: x == x and abs(x) != float("inf"), (o, h, l, c))):  # finite check
            continue
        rows.append(
            {
                "open": o,
                "high": h,
                "low": l,
                "close": c,
                "volume": float(bar.get("volume") or bar.get("tickVolume") or 0),
            }
        )
        rows[-1]["_ts"] = ts

    if not rows:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])

    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=["_ts"], keep="last")
    df = df.set_index("_ts").sort_index()
    df.index.name = None
    return df[["open", "high", "low", "close", "volume"]]


def fetch_ohlc(
    interval: str = "1m",
    limit: int = DEFAULT_LIMIT,
    *,
    symbol: str = "XAUUSD",
    use_cache: bool = True,
    max_age_seconds: Optional[float] = None,
) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
    """Fetch OHLC from BiQuote.

    Returns (dataframe_or_None, meta).
    meta always contains state, age_seconds, error, fetched_at, source.
    """
    interval = str(interval).lower()
    if interval not in ("1m", "5m", "15m", "1h"):
        return None, {
            "state": "ERROR",
            "error": f"unsupported interval {interval}",
            "source": "biquote",
        }

    cache_key = f"{symbol}_{interval}_{limit}"
    now = time.time()
    ttl = max_age_seconds
    if ttl is None:
        ttl = DEFAULT_1M_MAX_AGE if interval == "1m" else DEFAULT_5M_MAX_AGE

    if use_cache:
        with _cache_lock:
            hit = _cache.get(cache_key)
        if hit and (now - hit["fetched_at"]) < ttl and hit.get("df") is not None and not hit["df"].empty:
            age = now - hit["fetched_at"]
            return hit["df"].copy(), {
                "state": "FRESH" if age <= ttl else "STALE",
                "age_seconds": age,
                "error": None,
                "fetched_at": hit["fetched_at"],
                "source": "biquote_cache",
                "bars": len(hit["df"]),
            }

    url = f"{DEFAULT_BASE_URL}/{symbol}/ohlc"
    params = {"interval": interval, "limit": int(limit)}
    try:
        resp = requests.get(url, params=params, headers=_headers(), timeout=REQUEST_TIMEOUT)
        if resp.status_code != 200:
            return None, {
                "state": "ERROR",
                "error": f"HTTP {resp.status_code}",
                "source": "biquote",
                "fetched_at": now,
            }
        data = resp.json()
    except requests.Timeout:
        return None, {"state": "ERROR", "error": "timeout", "source": "biquote", "fetched_at": now}
    except Exception as e:
        return None, {"state": "ERROR", "error": str(e), "source": "biquote", "fetched_at": now}

    bars = data.get("bars") if isinstance(data, dict) else None
    if not isinstance(bars, list):
        return None, {
            "state": "ERROR",
            "error": "missing bars array",
            "source": "biquote",
            "fetched_at": now,
        }

    df = bars_to_dataframe(bars, drop_open=True)
    if df.empty:
        return None, {
            "state": "NO_DATA",
            "error": "no completed bars after filtering isOpen",
            "source": "biquote",
            "fetched_at": now,
        }

    with _cache_lock:
        _cache[cache_key] = {"df": df, "fetched_at": now}

    return df.copy(), {
        "state": "FRESH",
        "age_seconds": 0.0,
        "error": None,
        "fetched_at": now,
        "source": "biquote",
        "bars": len(df),
        "raw_bar_count": len(bars),
    }


def get_gold_bundle(
    *,
    limit_1m: int = DEFAULT_LIMIT,
    limit_5m: int = DEFAULT_LIMIT,
    max_1m_age: float = DEFAULT_1M_MAX_AGE,
    max_5m_age: float = DEFAULT_5M_MAX_AGE,
    allow_refresh: bool = True,
) -> Dict[str, Any]:
    """Return a bundle compatible with data_contract.classify_bundle and the runner."""
    df1, meta1 = fetch_ohlc("1m", limit_1m, use_cache=True, max_age_seconds=max_1m_age)
    df5, meta5 = fetch_ohlc("5m", limit_5m, use_cache=True, max_age_seconds=max_5m_age)

    if not allow_refresh:
        # already served from cache path above
        pass

    class _Snap:
        def __init__(self, df, meta):
            self.candles = df.to_dict("records") if df is not None and not df.empty else []
            self.df = df
            self.age_seconds = meta.get("age_seconds")
            self.state = meta.get("state", "NO_DATA")
            self.meta = meta

    one = _Snap(df1, meta1)
    five = _Snap(df5, meta5)

    if meta1.get("state") in ("ERROR", "NO_DATA") or meta5.get("state") in ("ERROR", "NO_DATA"):
        state = "NO_DATA"
    elif meta1.get("state") == "STALE" or meta5.get("state") == "STALE":
        state = "STALE_DATA"
    else:
        state = "READY"

    return {
        "state": state,
        "pair": "XAU/USD",
        "market_type": "forex",
        "entry": one,
        "confirmation": five,
        "uses_shared_cache": False,
        "provider": "biquote",
        "meta_1m": meta1,
        "meta_5m": meta5,
    }
