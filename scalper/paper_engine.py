"""Paper-only Gold Reversal Scalper engine.

Exit policy: move the protective stop to +1R once +1R is reached, then close
all remaining size at that locked stop or the fixed +3R take-profit. The
original stop remains active until the +1R trigger is reached. A fully
confirmed opposite strategy signal can still close an open trade.

Because this engine evaluates completed 1-minute OHLC candles rather than tick
 data, ambiguous same-candle stop/target ordering is handled conservatively:
 an already-active stop is checked first; the original stop is checked before
 a newly activated +1R stop; and if the newly activated +1R stop and +3R target
 are both touched in the same candle, the +1R stop is selected.
"""
from __future__ import annotations

import math
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any, Optional

import pandas as pd

from .config import ScalperConfig
from .strategy import evaluate


@dataclass
class PaperPosition:
    direction: str
    entry_price: float
    entry_time: Any
    stop_loss: float  # Original stop loss; retained for reporting/audit.
    risk_amount: float
    quantity: float
    initial_risk_per_unit: float
    initial_balance: float
    take_profit: float
    active_stop_loss: float
    breakeven_activated: bool = False
    breakeven_time: Any = None
    trade_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    hold_bars: int = 0
    unrealized_pnl: float = 0.0
    current_rr: float = 0.0
    max_favorable_r: float = 0.0
    reached_milestones: list[int] = field(default_factory=list)


def _validate_levels(direction: str, entry: float, sl: float) -> Optional[str]:
    if not all(math.isfinite(v) for v in (entry, sl)):
        return "non_finite_levels"
    if direction == "BUY" and sl >= entry:
        return "buy_stop_must_be_below_entry"
    if direction == "SELL" and sl <= entry:
        return "sell_stop_must_be_above_entry"
    if direction not in ("BUY", "SELL"):
        return "invalid_direction"
    return None


class PaperEngine:
    def __init__(self, config: Optional[ScalperConfig] = None):
        self.cfg = config or ScalperConfig()
        self.balance = float(self.cfg.starting_balance)
        self.position: Optional[PaperPosition] = None
        self.closed_trades: list[dict] = []
        self.last_signal: Optional[dict] = None
        self.last_processed_bar: Any = None
        self.last_entry_by_direction: dict[str, int] = {}
        self.bars_processed = 0
        self.pending_events: list[tuple[str, dict]] = []

    def set_risk_percent(self, risk_percent: float) -> None:
        self.cfg = self.cfg.with_risk(risk_percent)

    def drain_events(self) -> list[tuple[str, dict]]:
        events = list(self.pending_events)
        self.pending_events.clear()
        return events

    def restore_state(self, state: dict) -> None:
        """Restore the latest persisted balance/open position after a restart."""
        if not state:
            return
        self.balance = float(state.get("balance", self.balance))
        raw = state.get("position")
        self.last_processed_bar = state.get("last_processed_bar") or (raw or {}).get("last_processed_bar")
        if raw:
            entry = float(raw["entry_price"])
            original_sl = float(raw["stop_loss"])
            risk_per_unit = float(raw.get("initial_risk_per_unit") or abs(entry - original_sl))
            default_tp = entry + risk_per_unit * self.cfg.take_profit_r if str(raw["direction"]).upper() == "BUY" else entry - risk_per_unit * self.cfg.take_profit_r
            default_active_sl = entry + risk_per_unit * self.cfg.profit_lock_r if str(raw["direction"]).upper() == "BUY" else entry - risk_per_unit * self.cfg.profit_lock_r
            self.position = PaperPosition(
                direction=str(raw["direction"]).upper(),
                entry_price=entry,
                entry_time=raw.get("entry_time"),
                stop_loss=original_sl,
                risk_amount=float(raw["risk_amount"]),
                quantity=float(raw["quantity"]),
                initial_risk_per_unit=risk_per_unit,
                initial_balance=float(raw.get("initial_balance", self.balance)),
                take_profit=float(raw.get("take_profit") or default_tp),
                active_stop_loss=float(raw.get("active_stop_loss") or (default_active_sl if raw.get("breakeven_activated") else original_sl)),
                breakeven_activated=bool(raw.get("breakeven_activated", False)),
                breakeven_time=raw.get("breakeven_time"),
                trade_id=str(raw["trade_id"]),
                hold_bars=int(raw.get("hold_bars", 0)),
                unrealized_pnl=float(raw.get("unrealized_pnl", 0.0)),
                current_rr=float(raw.get("current_rr", 0.0)),
                max_favorable_r=float(raw.get("max_favorable_r", 0.0)),
                reached_milestones=list(raw.get("reached_milestones") or []),
            )

    def on_bar(self, df1: pd.DataFrame, df5: pd.DataFrame) -> dict:
        if df1 is None or df1.empty:
            return self.status("NO_DATA")
        now = df1.index[-1]
        if self.last_processed_bar is not None and now <= self.last_processed_bar:
            return self.status("DUPLICATE_BAR_IGNORED")
        self.last_processed_bar = now
        self.bars_processed += 1
        bar = df1.iloc[-1]
        price = float(bar["close"])

        if self.position is not None:
            p = self.position
            p.hold_bars += 1
            p.unrealized_pnl = self._pnl(p.direction, p.entry_price, price, p.quantity)
            p.current_rr = p.unrealized_pnl / p.risk_amount if p.risk_amount else 0.0
            self._update_excursion(p, bar)
            exit_reason, exit_price = self._bar_exit(p, bar, now)
            if exit_reason:
                self._close(float(exit_price), now, exit_reason)

        signal = (
            evaluate(df1, df5, self.cfg)
            if df5 is not None and not df5.empty
            else {"status": "NO_SIGNAL", "reason": "no_5m_data"}
        )
        self.last_signal = signal
        if signal.get("status") != "SIGNAL":
            return self.status("UPDATED")

        direction = str(signal.get("direction") or "").upper()
        if direction not in ("BUY", "SELL"):
            return self.status("INVALID_SIGNAL")

        # A fully confirmed opposite signal closes the old position. The same
        # signal may open the opposite position if immediate reversal is enabled.
        if self.position is not None:
            if self.position.direction == direction:
                return self.status("SAME_DIRECTION_POSITION_EXISTS")
            self._close(price, now, "REVERSAL")
            if not self.cfg.allow_immediate_reversal:
                return self.status("CLOSED_ON_CONFIRMED_REVERSAL")

        last_idx = self.last_entry_by_direction.get(direction)
        cooldown = max(0, int(self.cfg.same_direction_cooldown_bars))
        if last_idx is not None and self.bars_processed - last_idx <= cooldown:
            return self.status("COOLDOWN")

        self._open(direction, now, signal)
        return self.status("UPDATED")

    def _update_excursion(self, p: PaperPosition, bar) -> None:
        high, low = float(bar["high"]), float(bar["low"])
        if p.direction == "BUY":
            favourable_move = max(0.0, high - p.entry_price)
        else:
            favourable_move = max(0.0, p.entry_price - low)
        current_favourable_r = favourable_move / p.initial_risk_per_unit if p.initial_risk_per_unit else 0.0
        p.max_favorable_r = max(p.max_favorable_r, current_favourable_r)
        # Only 1R protection and the 3R target are actionable milestones.
        for milestone in (1, 3):
            if p.max_favorable_r >= milestone and milestone not in p.reached_milestones:
                p.reached_milestones.append(milestone)

    def _bar_exit(self, p: PaperPosition, bar, timestamp) -> tuple[Optional[str], Optional[float]]:
        high, low = float(bar["high"]), float(bar["low"])
        # First honour the stop that was active before this candle began.
        if p.direction == "BUY" and low <= p.active_stop_loss:
            reason = "PROFIT_LOCK" if p.breakeven_activated else "STOP_LOSS"
            return reason, p.active_stop_loss
        if p.direction == "SELL" and high >= p.active_stop_loss:
            reason = "PROFIT_LOCK" if p.breakeven_activated else "STOP_LOSS"
            return reason, p.active_stop_loss

        # Activate the +1R stop only after price has reached +1R.
        if not p.breakeven_activated:
            trigger = (
                p.entry_price + p.initial_risk_per_unit * self.cfg.profit_lock_r
                if p.direction == "BUY"
                else p.entry_price - p.initial_risk_per_unit * self.cfg.profit_lock_r
            )
            trigger_hit = high >= trigger if p.direction == "BUY" else low <= trigger
            if trigger_hit:
                # Original SL was checked above. From this point the new stop
                # protects +1R; if it is also touched in this candle, use it.
                p.active_stop_loss = trigger
                p.breakeven_activated = True
                p.breakeven_time = timestamp
                self.pending_events.append(("gold_profit_lock_activated", {
                    "trade_id": p.trade_id,
                    "direction": p.direction,
                    "entry_price": p.entry_price,
                    "original_stop_loss": p.stop_loss,
                    "active_stop_loss": p.active_stop_loss,
                    "take_profit": p.take_profit,
                    "trigger_price": trigger,
                    "current_price": trigger,
                    "trigger_r": float(self.cfg.profit_lock_r),
                    "current_rr": float(self.cfg.profit_lock_r),
                    "unrealized_pnl": self._pnl(p.direction, p.entry_price, trigger, p.quantity),
                    "risk_amount": p.risk_amount,
                    "max_favorable_r": p.max_favorable_r,
                    "reached_milestones": list(p.reached_milestones),
                    "hold_bars": p.hold_bars,
                    "bar_time": str(timestamp),
                }))
                if p.direction == "BUY" and low <= p.active_stop_loss:
                    return "PROFIT_LOCK", p.active_stop_loss
                if p.direction == "SELL" and high >= p.active_stop_loss:
                    return "PROFIT_LOCK", p.active_stop_loss

        # Target check comes after the active/profit-lock stop for conservative
        # treatment of ambiguous OHLC bars.
        target_hit = high >= p.take_profit if p.direction == "BUY" else low <= p.take_profit
        if target_hit:
            return "TAKE_PROFIT", p.take_profit
        return None, None

    def _open(self, direction: str, timestamp, signal: dict) -> None:
        entry = float(signal.get("entry", signal.get("price", float("nan"))))
        sl = float(signal.get("sl", signal.get("stop_loss", float("nan"))))
        level_err = _validate_levels(direction, entry, sl)
        if level_err:
            self.last_signal = {**signal, "engine_error": level_err}
            return
        risk_per_unit = abs(entry - sl)
        risk_amount = max(0.0, self.balance * float(self.cfg.risk_percent) / 100.0)
        quantity = risk_amount / (risk_per_unit * float(self.cfg.point_value)) if risk_per_unit > 0 else 0.0
        if not math.isfinite(quantity) or quantity <= 0:
            self.last_signal = {**signal, "engine_error": "invalid_position_size"}
            return
        target = entry + risk_per_unit * self.cfg.take_profit_r if direction == "BUY" else entry - risk_per_unit * self.cfg.take_profit_r
        self.position = PaperPosition(
            direction=direction,
            entry_price=entry,
            entry_time=timestamp,
            stop_loss=sl,
            risk_amount=risk_amount,
            quantity=quantity,
            initial_risk_per_unit=risk_per_unit,
            initial_balance=self.balance,
            take_profit=target,
            active_stop_loss=sl,
        )
        self.last_entry_by_direction[direction] = self.bars_processed

    def _close(self, price: float, timestamp, reason: str) -> None:
        p = self.position
        if p is None:
            return
        pnl = self._pnl(p.direction, p.entry_price, price, p.quantity)
        self.balance += pnl
        realized_r = pnl / p.risk_amount if p.risk_amount else 0.0
        self.closed_trades.append({
            "trade_id": p.trade_id,
            "pair": self.cfg.pair,
            "strategy": "GOLD_REVERSAL_SCALPER",
            "strategy_name": "Gold Reversal Scalper",
            "direction": p.direction,
            "entry_time": p.entry_time,
            "exit_time": timestamp,
            "entry_price": p.entry_price,
            "exit_price": float(price),
            "stop_loss": p.stop_loss,
            "active_stop_loss": p.active_stop_loss,
            "take_profit": p.take_profit,
            "breakeven_activated": p.breakeven_activated,
            "breakeven_time": p.breakeven_time,
            "exit_method": "FIXED_3R_TP_OR_1R_PROFIT_LOCK_OR_ORIGINAL_SL_OR_CONFIRMED_REVERSAL",
            "risk_amount": p.risk_amount,
            "risk_percent": float(self.cfg.risk_percent),
            "quantity": p.quantity,
            "point_value": float(self.cfg.point_value),
            "initial_risk_per_unit": p.initial_risk_per_unit,
            "realized_rr": realized_r,
            "max_favorable_r": p.max_favorable_r,
            "reached_milestones": list(p.reached_milestones),
            "hold_bars": p.hold_bars,
            "pnl": pnl,
            "pnl_r": realized_r,
            "balance_after": self.balance,
            "exit_reason": reason,
            "result": "WIN" if pnl > 0 else "LOSS" if pnl < 0 else "BREAKEVEN",
        })
        self.position = None

    def _pnl(self, direction: str, entry: float, exit_price: float, quantity: float) -> float:
        move = exit_price - entry if direction == "BUY" else entry - exit_price
        return move * quantity * float(self.cfg.point_value)

    def status(self, state: str = "UPDATED") -> dict:
        trades = self.closed_trades
        wins = sum(1 for t in trades if t["pnl"] > 0)
        losses = sum(1 for t in trades if t["pnl"] < 0)
        return {
            "state": state,
            "pair": self.cfg.pair,
            "strategy": "GOLD_REVERSAL_SCALPER",
            "strategy_name": "Gold Reversal Scalper",
            "balance": self.balance,
            "starting_balance": float(self.cfg.starting_balance),
            "net_pnl": self.balance - float(self.cfg.starting_balance),
            "risk_percent": float(self.cfg.risk_percent),
            "point_value": float(self.cfg.point_value),
            "profit_lock_r": float(self.cfg.profit_lock_r),
            "take_profit_r": float(self.cfg.take_profit_r),
            "position": asdict(self.position) if self.position else None,
            "last_signal": self.last_signal,
            "closed_trades": len(trades),
            "wins": wins,
            "losses": losses,
            "win_rate": (wins / len(trades) * 100) if trades else 0.0,
            "bars_processed": self.bars_processed,
        }
