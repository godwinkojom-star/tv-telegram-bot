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

    def test_stop_validation(self):
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

    def test_buy_position_calculates_1r_lock_and_3r_target(self):
        engine = PaperEngine(ScalperConfig(point_value=1))
        engine._open("BUY", pd.Timestamp("2026-01-01"), {"entry": 2000, "sl": 1990})
        p = engine.position
        self.assertEqual(p.take_profit, 2030)
        self.assertEqual(p.active_stop_loss, 1990)
        self.assertFalse(p.breakeven_activated)

    def test_sell_position_calculates_1r_lock_and_3r_target(self):
        engine = PaperEngine(ScalperConfig(point_value=1))
        engine._open("SELL", pd.Timestamp("2026-01-01"), {"entry": 2000, "sl": 2010})
        p = engine.position
        self.assertEqual(p.take_profit, 1970)
        self.assertEqual(p.active_stop_loss, 2010)

    def test_original_stop_closes_before_profit_lock_trigger(self):
        engine = PaperEngine(ScalperConfig(point_value=1))
        engine._open("BUY", pd.Timestamp("2026-01-01"), {"entry": 2000, "sl": 1990})
        reason, price = engine._bar_exit(engine.position, {"low": 1989, "high": 2005}, pd.Timestamp("2026-01-01 00:01"))
        self.assertEqual((reason, price), ("STOP_LOSS", 1990))
        self.assertFalse(engine.position.breakeven_activated)

    def test_buy_reaches_1r_then_reverses_to_profit_lock(self):
        engine = PaperEngine(ScalperConfig(point_value=1))
        engine._open("BUY", pd.Timestamp("2026-01-01"), {"entry": 2000, "sl": 1990})
        p = engine.position
        engine._update_excursion(p, {"low": 2010.5, "high": 2011})
        reason, price = engine._bar_exit(p, {"low": 2010.5, "high": 2011}, pd.Timestamp("2026-01-01 00:01"))
        self.assertIsNone(reason)
        self.assertEqual(p.active_stop_loss, 2010)
        self.assertTrue(p.breakeven_activated)
        events = engine.drain_events()
        self.assertEqual(events[0][0], "gold_profit_lock_activated")
        reason, price = engine._bar_exit(p, {"low": 2009, "high": 2012}, pd.Timestamp("2026-01-01 00:02"))
        self.assertEqual((reason, price), ("PROFIT_LOCK", 2010))
        engine._close(price, pd.Timestamp("2026-01-01 00:02"), reason)
        self.assertAlmostEqual(engine.closed_trades[-1]["realized_rr"], 1.0)
        self.assertEqual(engine.closed_trades[-1]["result"], "WIN")

    def test_sell_reaches_1r_then_reverses_to_profit_lock(self):
        engine = PaperEngine(ScalperConfig(point_value=1))
        engine._open("SELL", pd.Timestamp("2026-01-01"), {"entry": 2000, "sl": 2010})
        p = engine.position
        engine._update_excursion(p, {"low": 1989, "high": 1989.5})
        reason, price = engine._bar_exit(p, {"low": 1989, "high": 1989.5}, pd.Timestamp("2026-01-01 00:01"))
        self.assertIsNone(reason)
        self.assertEqual(p.active_stop_loss, 1990)
        reason, price = engine._bar_exit(p, {"low": 1988, "high": 1991}, pd.Timestamp("2026-01-01 00:02"))
        self.assertEqual((reason, price), ("PROFIT_LOCK", 1990))

    def test_fixed_3r_target_closes_full_trade(self):
        engine = PaperEngine(ScalperConfig(point_value=1))
        engine._open("BUY", pd.Timestamp("2026-01-01"), {"entry": 2000, "sl": 1990})
        p = engine.position
        # Trigger +1R on an earlier bar; then target +3R on a later bar.
        engine._update_excursion(p, {"low": 2010.5, "high": 2011})
        engine._bar_exit(p, {"low": 2010.5, "high": 2011}, pd.Timestamp("2026-01-01 00:01"))
        engine.drain_events()
        reason, price = engine._bar_exit(p, {"low": 2011, "high": 2031}, pd.Timestamp("2026-01-01 00:02"))
        self.assertEqual((reason, price), ("TAKE_PROFIT", 2030))
        engine._close(price, pd.Timestamp("2026-01-01 00:02"), reason)
        self.assertAlmostEqual(engine.closed_trades[-1]["realized_rr"], 3.0)
        self.assertEqual(engine.closed_trades[-1]["exit_reason"], "TAKE_PROFIT")

    def test_no_2r_exit_or_partial_close(self):
        engine = PaperEngine(ScalperConfig(point_value=1))
        engine._open("BUY", pd.Timestamp("2026-01-01"), {"entry": 2000, "sl": 1990})
        p = engine.position
        engine._update_excursion(p, {"low": 2011, "high": 2025})
        reason, price = engine._bar_exit(p, {"low": 2011, "high": 2025}, pd.Timestamp("2026-01-01 00:01"))
        self.assertIsNone(reason)
        self.assertIs(engine.position, p)
        self.assertEqual(p.reached_milestones, [1])
        self.assertEqual(engine.drain_events()[0][0], "gold_profit_lock_activated")

    def test_ambiguous_same_bar_profit_lock_and_target_uses_lock(self):
        engine = PaperEngine(ScalperConfig(point_value=1))
        engine._open("BUY", pd.Timestamp("2026-01-01"), {"entry": 2000, "sl": 1990})
        reason, price = engine._bar_exit(engine.position, {"low": 2009, "high": 2031}, pd.Timestamp("2026-01-01 00:01"))
        self.assertEqual((reason, price), ("PROFIT_LOCK", 2010))

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

    def test_position_restore_preserves_profit_lock_and_target(self):
        engine = PaperEngine(ScalperConfig())
        state = {"balance": 990, "position": {
            "trade_id": "gold-test-1", "direction": "BUY", "entry_price": 2000,
            "entry_time": "2026-01-01T00:00:00", "stop_loss": 1990,
            "risk_amount": 10, "quantity": 1, "initial_risk_per_unit": 10,
            "initial_balance": 990, "hold_bars": 4, "max_favorable_r": 1,
            "reached_milestones": [1], "active_stop_loss": 2010,
            "take_profit": 2030, "breakeven_activated": True,
            "breakeven_time": "2026-01-01T00:04:00",
        }}
        engine.restore_state(state)
        self.assertEqual(engine.position.trade_id, "gold-test-1")
        self.assertEqual(engine.position.active_stop_loss, 2010)
        self.assertEqual(engine.position.take_profit, 2030)
        self.assertTrue(engine.position.breakeven_activated)

    def test_no_bar_timeout(self):
        engine = PaperEngine(ScalperConfig(same_direction_cooldown_bars=0))
        engine._open("BUY", pd.Timestamp("2026-01-01"), {"entry": 2000, "sl": 1990})
        # Bars remain below the +1R trigger and above the original SL.
        with mock.patch("scalper.paper_engine.evaluate", return_value={"status": "NO_SIGNAL"}):
            for minute in range(1, 36):
                result = engine.on_bar(self.frame(f"2026-01-01 00:{minute:02d}", 2001), self.frame("2026-01-01 00:00"))
        self.assertIsNotNone(engine.position)
        self.assertEqual(engine.closed_trades, [])
        self.assertEqual(result["state"], "UPDATED")


if __name__ == "__main__":
    unittest.main()
