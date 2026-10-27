import unittest
from unittest import mock

import pandas as pd

from scalper.config import ScalperConfig
from scalper.paper_engine import PaperEngine, _validate_levels


class PaperEngineTests(unittest.TestCase):
    def frame(self, ts, close=2000.0, high=None, low=None):
        return pd.DataFrame(
            [
                {
                    "open": close,
                    "high": high if high is not None else close + 0.2,
                    "low": low if low is not None else close - 0.2,
                    "close": close,
                }
            ],
            index=[pd.Timestamp(ts)],
        )

    def test_validate_levels_buy_sell(self):
        self.assertIsNone(_validate_levels("BUY", 2000, 1990, 2020))
        self.assertIsNone(_validate_levels("SELL", 2000, 2010, 1980))
        self.assertEqual(_validate_levels("BUY", 2000, 2010, 2020), "buy_levels_invalid_sl_entry_tp_order")
        self.assertEqual(_validate_levels("SELL", 2000, 1990, 1980), "sell_levels_invalid_tp_entry_sl_order")

    def test_risk_sizing_1_2_5_percent(self):
        for risk in (1.0, 2.0, 5.0):
            cfg = ScalperConfig(starting_balance=1000, risk_percent=risk, point_value=1)
            e = PaperEngine(cfg)
            e._open("BUY", pd.Timestamp("2026-01-01"), {"entry": 2000, "sl": 1990, "tp": 2020, "rr": 2})
            self.assertIsNotNone(e.position)
            self.assertAlmostEqual(e.position.risk_amount, 1000 * risk / 100.0)
            self.assertAlmostEqual(e.position.quantity, e.position.risk_amount / 10.0)

    def test_point_value_affects_pnl(self):
        e = PaperEngine(ScalperConfig(starting_balance=1000, risk_percent=1, point_value=2.0))
        e._open("BUY", pd.Timestamp("2026-01-01"), {"entry": 2000, "sl": 1990, "tp": 2020})
        e._close(2020, pd.Timestamp("2026-01-01 00:01"), "TP")
        # risk_per_unit = 10 * 2 = 20; qty = 10/20 = 0.5; move=20; pnl = 20*0.5*2 = 20
        self.assertAlmostEqual(e.balance, 1020.0)
        self.assertAlmostEqual(e.closed_trades[0]["pnl_r"], 2.0)

    def test_duplicate_bar_ignored(self):
        e = PaperEngine(ScalperConfig())
        d1 = self.frame("2026-01-01 00:00")
        d5 = self.frame("2026-01-01 00:00")
        with mock.patch("scalper.paper_engine.evaluate", return_value={"status": "NO_SIGNAL"}):
            first = e.on_bar(d1, d5)
            second = e.on_bar(d1, d5)
        self.assertEqual(first["state"], "UPDATED")
        self.assertEqual(second["state"], "DUPLICATE_BAR_IGNORED")
        self.assertEqual(e.bars_processed, 1)

    def test_stop_loss_wins_same_candle_ambiguity(self):
        e = PaperEngine(ScalperConfig())
        e._open("BUY", pd.Timestamp("2026-01-01"), {"entry": 2000, "sl": 1990, "tp": 2010})
        reason, px = e._bar_exit(e.position, {"low": 1989, "high": 2011})
        self.assertEqual(reason, "SL")
        self.assertEqual(px, 1990)

    def test_invalid_levels_rejected(self):
        e = PaperEngine(ScalperConfig())
        e._open("BUY", pd.Timestamp("2026-01-01"), {"entry": 2000, "sl": 2010, "tp": 2020})
        self.assertIsNone(e.position)
        self.assertIn("engine_error", e.last_signal or {})

    def test_timeout_exit(self):
        cfg = ScalperConfig(max_hold_bars=2, same_direction_cooldown_bars=0)
        e = PaperEngine(cfg)
        e._open("BUY", pd.Timestamp("2026-01-01 00:00"), {"entry": 2000, "sl": 1990, "tp": 2100})
        with mock.patch("scalper.paper_engine.evaluate", return_value={"status": "NO_SIGNAL"}):
            e.on_bar(self.frame("2026-01-01 00:01", 2001), self.frame("2026-01-01 00:00"))
            e.on_bar(self.frame("2026-01-01 00:02", 2002), self.frame("2026-01-01 00:00"))
        self.assertIsNone(e.position)
        self.assertEqual(e.closed_trades[-1]["exit_reason"], "TIMEOUT")

    def test_reversal_closes_then_opens(self):
        cfg = ScalperConfig(allow_immediate_reversal=True, same_direction_cooldown_bars=0)
        e = PaperEngine(cfg)
        e._open("BUY", pd.Timestamp("2026-01-01 00:00"), {"entry": 2000, "sl": 1990, "tp": 2100})
        sell_signal = {
            "status": "SIGNAL",
            "direction": "SELL",
            "entry": 2005,
            "sl": 2015,
            "tp": 1990,
            "rr": 1.5,
        }
        with mock.patch("scalper.paper_engine.evaluate", return_value=sell_signal):
            e.on_bar(self.frame("2026-01-01 00:01", 2005), self.frame("2026-01-01 00:00"))
        self.assertIsNotNone(e.position)
        self.assertEqual(e.position.direction, "SELL")
        self.assertEqual(e.closed_trades[-1]["exit_reason"], "REVERSAL")

    def test_risk_options_via_with_risk(self):
        cfg = ScalperConfig()
        for r in (1.0, 2.0, 5.0):
            c2 = cfg.with_risk(r)
            self.assertEqual(c2.risk_percent, r)
        with self.assertRaises(ValueError):
            cfg.with_risk(3.0)


if __name__ == "__main__":
    unittest.main()
