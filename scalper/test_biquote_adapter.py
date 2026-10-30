import unittest
from unittest import mock

import pandas as pd

from scalper import biquote_adapter as bq


SAMPLE_BARS = [
    {
        "openTime": "2026-10-27T17:26:00Z",
        "open": 4061.6,
        "high": 4062.1,
        "low": 4061.3,
        "close": 4061.4,
        "volume": 0,
        "tickVolume": 67,
        "isOpen": False,
    },
    {
        "openTime": "2026-10-27T17:27:00Z",
        "open": 4061.8,
        "high": 4061.8,
        "low": 4060.9,
        "close": 4061.0,
        "volume": 0,
        "tickVolume": 20,
        "isOpen": True,  # must be dropped
    },
]


class BiQuoteAdapterTests(unittest.TestCase):
    def test_bars_to_dataframe_drops_open(self):
        df = bq.bars_to_dataframe(SAMPLE_BARS, drop_open=True)
        self.assertEqual(len(df), 1)
        self.assertAlmostEqual(df.iloc[0]["close"], 4061.4)

    def test_bars_malformed_skipped(self):
        bad = SAMPLE_BARS + [{"openTime": "bad", "open": "x"}]
        df = bq.bars_to_dataframe(bad, drop_open=True)
        self.assertEqual(len(df), 1)

    def test_fetch_success_mocked(self):
        class Resp:
            status_code = 200

            def json(self):
                return {"symbol": "XAUUSD", "interval": "1m", "bars": SAMPLE_BARS}

        with mock.patch("scalper.biquote_adapter.requests.get", return_value=Resp()):
            # clear cache
            with bq._cache_lock:
                bq._cache.clear()
            df, meta = bq.fetch_ohlc("1m", limit=20, use_cache=False)
        self.assertIsNotNone(df)
        self.assertEqual(meta["state"], "FRESH")
        self.assertEqual(len(df), 1)

    def test_fetch_http_error(self):
        class Resp:
            status_code = 500
            text = "err"

        with mock.patch("scalper.biquote_adapter.requests.get", return_value=Resp()):
            with bq._cache_lock:
                bq._cache.clear()
            df, meta = bq.fetch_ohlc("1m", use_cache=False)
        self.assertIsNone(df)
        self.assertEqual(meta["state"], "ERROR")

    def test_timestamp_parsing(self):
        ts = bq._parse_open_time("2026-10-27T17:26:00Z")
        self.assertIsNotNone(ts)
        self.assertEqual(str(ts.tzinfo), "UTC")


if __name__ == "__main__":
    unittest.main()
