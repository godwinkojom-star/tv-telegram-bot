"""Performance reporting for the SmartFX scalper backtester."""
from __future__ import annotations

from typing import Iterable
import math


def summarize(trades: Iterable) -> dict:
    trades = list(trades)
    pnls = [float(t.pnl) if hasattr(t, "pnl") else float(t.get("pnl", 0.0)) for t in trades]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))

    return {
        "total_trades": len(pnls),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": (len(wins) / len(pnls) * 100) if pnls else 0.0,
        "net_pnl": sum(pnls),
        "average_trade": (sum(pnls) / len(pnls)) if pnls else 0.0,
        "average_win": (gross_profit / len(wins)) if wins else 0.0,
        "average_loss": (-gross_loss / len(losses)) if losses else 0.0,
        "profit_factor": (gross_profit / gross_loss) if gross_loss else (math.inf if gross_profit else 0.0),
        "best_trade": max(pnls) if pnls else 0.0,
        "worst_trade": min(pnls) if pnls else 0.0,
    }


def equity_stats(equity_curve):
    if equity_curve is None or equity_curve.empty:
        return {"max_drawdown": 0.0, "max_drawdown_pct": 0.0}
    equity = equity_curve["equity"]
    peak = equity.cummax()
    dd = equity - peak
    pct = dd / peak.replace(0, float("nan")) * 100
    return {
        "max_drawdown": float(dd.min()),
        "max_drawdown_pct": float(pct.min()) if pct.notna().any() else 0.0,
    }
