"""Paper-trading state engine for the SmartFX Gold reversal scalper.

This module does not fetch market data. Feed it completed 1m/5m OHLC data from
the application's existing shared cache when integrating it later.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Optional
import pandas as pd

from .config import ScalperConfig
from .strategy import evaluate


@dataclass
class PaperPosition:
    direction: str
    entry_price: float
    entry_time: object
    stop_loss: float
    take_profit: float
    unrealized_pnl: float = 0.0


class PaperEngine:
    def __init__(self, config: Optional[ScalperConfig] = None):
        self.cfg = config or ScalperConfig()
        self.position: Optional[PaperPosition] = None
        self.closed_trades: list[dict] = []
        self.last_signal: Optional[dict] = None

    def on_bar(self, df1: pd.DataFrame, df5: pd.DataFrame) -> dict:
        """Process the newest completed 1m bar and return a status dictionary."""
        if df1.empty:
            return self.status("NO_DATA")

        now = df1.index[-1]
        price = float(df1.iloc[-1]["close"])

        # Existing position: check SL/TP first.
        if self.position is not None:
            p = self.position
            p.unrealized_pnl = (price - p.entry_price) if p.direction == "BUY" else (p.entry_price - price)
            reason = None
            if p.direction == "BUY":
                if float(df1.iloc[-1]["low"]) <= p.stop_loss:
                    reason = "SL"
                    exit_price = p.stop_loss
                elif float(df1.iloc[-1]["high"]) >= p.take_profit:
                    reason = "TP"
                    exit_price = p.take_profit
            else:
                if float(df1.iloc[-1]["high"]) >= p.stop_loss:
                    reason = "SL"
                    exit_price = p.stop_loss
                elif float(df1.iloc[-1]["low"]) <= p.take_profit:
                    reason = "TP"
                    exit_price = p.take_profit

            if reason:
                self._close(exit_price, now, reason)

        signal = evaluate(df1, df5, self.cfg)
        self.last_signal = signal

        # Reversal: close current position and immediately replace it.
        if signal.get("status") == "SIGNAL":
            direction = signal["direction"]
            if self.position is None:
                self._open(direction, price, now, signal)
            elif self.position.direction != direction and self.cfg.allow_immediate_reversal:
                self._close(price, now, "REVERSAL")
                self._open(direction, price, now, signal)

        return self.status("UPDATED")

    def _open(self, direction, price, timestamp, signal):
        self.position = PaperPosition(
            direction=direction,
            entry_price=float(price),
            entry_time=timestamp,
            stop_loss=float(signal["stop_loss"]),
            take_profit=float(signal["take_profit"]),
        )

    def _close(self, price, timestamp, reason):
        p = self.position
        if p is None:
            return
        pnl = (price - p.entry_price) if p.direction == "BUY" else (p.entry_price - price)
        self.closed_trades.append({
            "direction": p.direction,
            "entry_time": p.entry_time,
            "exit_time": timestamp,
            "entry_price": p.entry_price,
            "exit_price": float(price),
            "pnl": float(pnl),
            "exit_reason": reason,
        })
        self.position = None

    def status(self, state="UPDATED") -> dict:
        return {
            "state": state,
            "position": asdict(self.position) if self.position else None,
            "last_signal": self.last_signal,
            "closed_trades": len(self.closed_trades),
        }
