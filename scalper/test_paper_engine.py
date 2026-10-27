import sys, types, unittest
import pandas as pd

# Isolate this patch bundle from the live strategy module; entry signals are controlled test fixtures.
strategy = types.ModuleType('scalper.strategy')
strategy.evaluate = lambda df1, df5, cfg: {'status':'NO_SIGNAL','reason':'fixture'}
sys.modules['scalper.strategy'] = strategy
from scalper.config import ScalperConfig
from scalper.paper_engine import PaperEngine

class PaperEngineTests(unittest.TestCase):
    def frame(self, ts, close=2000.0, high=None, low=None):
        return pd.DataFrame([{'open':close, 'high':high if high is not None else close+0.2,
                              'low':low if low is not None else close-0.2, 'close':close}], index=[pd.Timestamp(ts)])
    def test_signal_keys_sl_tp_and_risk_sizing(self):
        cfg=ScalperConfig(starting_balance=1000, risk_percent=1, point_value=1, same_direction_cooldown_bars=0)
        e=PaperEngine(cfg)
        e._open('BUY', pd.Timestamp('2026-01-01'), {'entry':2000,'sl':1990,'tp':2020,'rr':2})
        self.assertEqual(e.position.stop_loss,1990)
        self.assertEqual(e.position.take_profit,2020)
        self.assertAlmostEqual(e.position.risk_amount,10)
        self.assertAlmostEqual(e.position.quantity,1)
        self.assertAlmostEqual(e.position.rr,2)
    def test_duplicate_bar_does_not_process_twice(self):
        cfg=ScalperConfig()
        e=PaperEngine(cfg)
        d1=self.frame('2026-01-01 00:00')
        d5=self.frame('2026-01-01 00:00')
        first=e.on_bar(d1,d5)
        second=e.on_bar(d1,d5)
        self.assertEqual(first['state'],'UPDATED')
        self.assertEqual(second['state'],'DUPLICATE_BAR_IGNORED')
        self.assertEqual(e.bars_processed,1)
    def test_stop_loss_wins_same_candle_ambiguity(self):
        e=PaperEngine(ScalperConfig())
        e._open('BUY', pd.Timestamp('2026-01-01'), {'entry':2000,'sl':1990,'tp':2010})
        reason, px=e._bar_exit(e.position, {'low':1989,'high':2011})
        self.assertEqual(reason,'SL')
        self.assertEqual(px,1990)
    def test_realized_pnl_updates_isolated_balance(self):
        e=PaperEngine(ScalperConfig(starting_balance=1000, risk_percent=1, point_value=1))
        e._open('BUY', pd.Timestamp('2026-01-01'), {'entry':2000,'sl':1990,'tp':2020})
        e._close(2020, pd.Timestamp('2026-01-01 00:01'), 'TP')
        self.assertAlmostEqual(e.balance,1020)
        self.assertEqual(e.closed_trades[0]['result'],'WIN')
        self.assertAlmostEqual(e.closed_trades[0]['pnl_r'],2)

if __name__ == '__main__': unittest.main()
