import unittest
from unittest import mock

import pandas as pd

from scalper.config import ScalperConfig
from scalper.paper_engine import PaperEngine, _validate_levels


class PaperEngineTests(unittest.TestCase):
    def frame(self, ts, close=2000.0, high=None, low=None):
        return pd.DataFrame([{
            "open": close, "high": high if high is not None else close + 0.2,
            "low": low if low is not None else close - 0.2, "close": close,
        }], index=[pd.Timestamp(ts)])

    def test_stop_validation_has_no_take_profit(self):
        self.assertIsNone(_validate_levels("BUY", 2000, 1990))
        self.assertIsNone(_validate_levels("SELL", 2000, 2010))
        self.assertEqual(_validate_levels("BUY", 2000, 2010), "buy_stop_must_be_below_entry")
        self.assertEqual(_validate_levels("SELL", 2000, 1990), "sell_stop_must_be_above_entry")

    def test_risk_sizing_1_2_5_percent(self):
        for risk in (1.0, 2.0, 5.0):
            engine = PaperEngine(ScalperConfig(starting_balance=1000, risk_percent=risk, point_value=1))
            engine._open("BUY", pd.Timestamp("2026-01-01"), {"entry": 2000, "sl": 1990})
            self.assertIsNotNone(engine.position)
            self.assertAlmostEqual(engine.position.risk_amount, 1000 * risk / 100)
            self.assertAlmostEqual(engine.position.quantity, engine.position.risk_amount / 10)

    def test_stop_loss_closes_trade(self):
        engine = PaperEngine(ScalperConfig(same_direction_cooldown_bars=0))
        engine._open("BUY", pd.Timestamp("2026-01-01"), {"entry": 2000, "sl": 1990})
        reason, price = engine._bar_exit(engine.position, {"low": 1989, "high": 2005})
        self.assertEqual((reason, price), ("SL", 1990))

    def test_no_fixed_tp_and_no_timeout(self):
        engine = PaperEngine(ScalperConfig(same_direction_cooldown_bars=0))
        engine._open("BUY", pd.Timestamp("2026-01-01"), {"entry": 2000, "sl": 1990})
        with mock.patch("scalper.paper_engine.evaluate", return_value={"status": "NO_SIGNAL"}):
            for minute in range(1, 36):
                result = engine.on_bar(self.frame(f"2026-01-01 00:{minute:02d}", 2001), self.frame("2026-01-01 00:00"))
        self.assertIsNotNone(engine.position)
        self.assertEqual(engine.closed_trades, [])
        self.assertEqual(result["state"], "UPDATED")

    def test_confirmed_opposite_signal_closes_then_opens(self):
        cfg = ScalperConfig(allow_immediate_reversal=True, same_direction_cooldown_bars=0)
        engine = PaperEngine(cfg)
        engine._open("BUY", pd.Timestamp("2026-01-01"), {"entry": 2000, "sl": 1990})
        sell = {"status": "SIGNAL", "direction": "SELL", "entry": 2005, "sl": 2015}
        with mock.patch("scalper.paper_engine.evaluate", return_value=sell):
            engine.on_bar(self.frame("2026-01-01 00:01", 2005), self.frame("2026-01-01 00:00"))
        self.assertIsNotNone(engine.position)
        self.assertEqual(engine.position.direction, "SELL")
        self.assertEqual(engine.closed_trades[-1]["exit_reason"], "REVERSAL")
        self.assertIsNone(engine.closed_trades[-1]["take_profit"])

    def test_max_favourable_r_milestones_are_recorded(self):
        engine = PaperEngine(ScalperConfig())
        engine._open("BUY", pd.Timestamp("2026-01-01"), {"entry": 2000, "sl": 1990})
        engine._update_excursion(engine.position, {"high": 2051, "low": 1999})
        self.assertGreaterEqual(engine.position.max_favorable_r, 4.0)
        self.assertEqual(engine.position.reached_milestones, [1, 2, 3, 4, 5])

    def test_persisted_position_restore(self):
        engine = PaperEngine(ScalperConfig())
        state = {"balance": 990, "position": {
            "trade_id": "gold-test-1", "direction": "BUY", "entry_price": 2000,
            "entry_time": "2026-01-01T00:00:00", "stop_loss": 1990,
            "risk_amount": 10, "quantity": 1, "initial_risk_per_unit": 10,
            "initial_balance": 990, "hold_bars": 4, "max_favorable_r": 2,
            "reached_milestones": [1, 2],
        }}
        engine.restore_state(state)
        self.assertEqual(engine.balance, 990)
        self.assertEqual(engine.position.trade_id, "gold-test-1")
        self.assertEqual(engine.position.max_favorable_r, 2)


if __name__ == "__main__":
    unittest.main()
