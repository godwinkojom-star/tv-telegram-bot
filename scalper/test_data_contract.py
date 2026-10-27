import unittest

from scalper.data_contract import classify_bundle


class _Snap:
    def __init__(self, n, age=None):
        self.candles = [{}] * n
        self.age_seconds = age


class DataContractTests(unittest.TestCase):
    def test_no_data(self):
        s = classify_bundle({"state": "NO_DATA", "entry": _Snap(0), "confirmation": _Snap(0)})
        self.assertEqual(s.state, "NO_DATA")
        self.assertFalse(s.ready_for_signal)

    def test_stale(self):
        s = classify_bundle(
            {"state": "STALE_DATA", "entry": _Snap(200, 500), "confirmation": _Snap(200, 500)}
        )
        self.assertEqual(s.state, "WAIT_FOR_FRESH_DATA")
        self.assertFalse(s.ready_for_signal)

    def test_insufficient(self):
        s = classify_bundle(
            {"state": "READY", "entry": _Snap(10), "confirmation": _Snap(10)},
            min_1m_bars=100,
            min_5m_bars=100,
        )
        self.assertEqual(s.state, "INSUFFICIENT_CANDLES")
        self.assertFalse(s.ready_for_signal)

    def test_ready(self):
        s = classify_bundle(
            {"state": "READY", "entry": _Snap(150), "confirmation": _Snap(120)},
            min_1m_bars=100,
            min_5m_bars=100,
        )
        self.assertEqual(s.state, "READY")
        self.assertTrue(s.ready_for_signal)


if __name__ == "__main__":
    unittest.main()
