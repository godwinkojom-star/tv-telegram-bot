"""Performance reporting for the SmartFX scalper paper / backtester."""
from __future__ import annotations

import math
from typing import Iterable


def summarize(trades: Iterable) -> dict:
    trades = list(trades)
    pnls = []
    for t in trades:
        if hasattr(t, "pnl"):
            v = float(t.pnl)
        else:
            v = float(t.get("pnl", 0.0))
        if math.isfinite(v):
            pnls.append(v)
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    dict_trades = [t for t in trades if isinstance(t, dict)]
    realized_rr = [float(t.get("realized_rr", t.get("pnl_r", 0)) or 0) for t in dict_trades]
    max_rr = [float(t.get("max_favorable_r", 0) or 0) for t in dict_trades]
    milestone_counts = {f"reached_{n}r": sum(1 for value in max_rr if value >= n) for n in (1, 2, 3, 4)}
    milestone_counts["reached_5r_plus"] = sum(1 for value in max_rr if value >= 5)
    for threshold in (1, 2, 3, 4):
        milestone_counts[f"reached_{threshold}r_pct"] = (
            milestone_counts[f"reached_{threshold}r"] / len(max_rr) * 100 if max_rr else 0.0
        )
    milestone_counts["reached_5r_plus_pct"] = (
        milestone_counts["reached_5r_plus"] / len(max_rr) * 100 if max_rr else 0.0
    )
    return {
        "total_trades": len(pnls),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": (len(wins) / len(pnls) * 100) if pnls else 0.0,
        "net_pnl": sum(pnls),
        "average_trade": (sum(pnls) / len(pnls)) if pnls else 0.0,
        "average_win": (gross_profit / len(wins)) if wins else 0.0,
        "average_loss": (-gross_loss / len(losses)) if losses else 0.0,
        "profit_factor": (gross_profit / gross_loss)
        if gross_loss
        else (math.inf if gross_profit else 0.0),
        "best_trade": max(pnls) if pnls else 0.0,
        "worst_trade": min(pnls) if pnls else 0.0,
        "average_realized_rr": (sum(realized_rr) / len(realized_rr)) if realized_rr else 0.0,
        "average_max_favorable_r": (sum(max_rr) / len(max_rr)) if max_rr else 0.0,
        **milestone_counts,
    }


def equity_stats(equity_curve) -> dict:
    if equity_curve is None or getattr(equity_curve, "empty", True):
        return {"max_drawdown": 0.0, "max_drawdown_pct": 0.0}
    equity = equity_curve["equity"]
    peak = equity.cummax()
    dd = equity - peak
    pct = dd / peak.replace(0, float("nan")) * 100
    return {
        "max_drawdown": float(dd.min()) if len(dd) else 0.0,
        "max_drawdown_pct": float(pct.min()) if pct.notna().any() else 0.0,
    }
