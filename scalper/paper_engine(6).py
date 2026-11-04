"""Persistent-compatible, paper-only Gold trade engine with the locked TP1-TP5 stop ladder."""
from __future__ import annotations
import math, uuid
from dataclasses import dataclass, asdict, field
from typing import Any, Optional
import pandas as pd
from .config import ScalperConfig
from .strategy import evaluate

@dataclass
class PaperPosition:
    direction: str
    entry_price: float
    entry_time: Any
    stop_loss: float                 # immutable original SL
    risk_amount: float
    quantity: float
    initial_risk_per_unit: float
    initial_balance: float
    take_profit: float              # TP5
    active_stop_loss: float         # original SL until TP2, then +1R/+2R/+3R
    breakeven_activated: bool = False
    breakeven_time: Any = None
    trade_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    hold_bars: int = 0
    unrealized_pnl: float = 0.0
    current_rr: float = 0.0
    max_favorable_r: float = 0.0
    reached_milestones: list[int] = field(default_factory=list)

class PaperEngine:
    def __init__(self, config: Optional[ScalperConfig]=None):
        self.cfg=config or ScalperConfig(); self.balance=float(self.cfg.starting_balance)
        self.position: Optional[PaperPosition]=None; self.closed_trades=[]; self.last_signal=None
        self.last_processed_bar=None; self.last_entry_by_direction={}; self.bars_processed=0; self.pending_events=[]
    def set_risk_percent(self, risk_percent): self.cfg=self.cfg.with_risk(risk_percent)
    def drain_events(self):
        e=list(self.pending_events); self.pending_events.clear(); return e
    def restore_state(self, state):
        if not state: return
        self.balance=float(state.get('balance',self.balance)); raw=state.get('position')
        self.last_processed_bar=state.get('last_processed_bar') or (raw or {}).get('last_processed_bar')
        if not raw: return
        entry=float(raw['entry_price']); sl=float(raw['stop_loss']); risk=float(raw.get('initial_risk_per_unit') or abs(entry-sl)); direction=str(raw['direction']).upper()
        tp5=entry+risk*5 if direction=='BUY' else entry-risk*5
        self.position=PaperPosition(direction,entry,raw.get('entry_time'),sl,float(raw['risk_amount']),float(raw['quantity']),risk,float(raw.get('initial_balance',self.balance)),float(raw.get('take_profit') or tp5),float(raw.get('active_stop_loss') or sl),bool(raw.get('breakeven_activated',False)),raw.get('breakeven_time'),str(raw['trade_id']),int(raw.get('hold_bars',0)),float(raw.get('unrealized_pnl',0)),float(raw.get('current_rr',0)),float(raw.get('max_favorable_r',0)),[int(x) for x in (raw.get('reached_milestones') or [])])
    def on_bar(self, df1, df5):
        if df1 is None or df1.empty: return self.status('NO_DATA')
        now=df1.index[-1]
        if self.last_processed_bar is not None and now<=self.last_processed_bar: return self.status('DUPLICATE_BAR_IGNORED')
        self.last_processed_bar=now; self.bars_processed+=1; bar=df1.iloc[-1]
        high,low,close=float(bar['high']),float(bar['low']),float(bar['close'])
        if self.position:
            p=self.position; p.hold_bars+=1; p.unrealized_pnl=self._pnl(p.direction,p.entry_price,close,p.quantity); p.current_rr=p.unrealized_pnl/p.risk_amount if p.risk_amount else 0
            self._update_excursion(p,high,low)
            reason,price=self._bar_exit(p,high,low,now)
            if reason: self._close(price,now,reason)
        signal=evaluate(df1,df5,self.cfg) if df5 is not None and not df5.empty else {'status':'NO_SIGNAL'}
        self.last_signal=signal
        if signal.get('status')!='SIGNAL': return self.status('UPDATED')
        direction=str(signal.get('direction') or '').upper()
        if direction not in ('BUY','SELL'): return self.status('INVALID_SIGNAL')
        reversal=False
        if self.position:
            if self.position.direction==direction: return self.status('SAME_DIRECTION_POSITION_EXISTS')
            self._close(close,now,'REVERSAL'); reversal=True
            if not self.cfg.allow_immediate_reversal: return self.status('CLOSED_ON_CONFIRMED_REVERSAL')
        last=self.last_entry_by_direction.get(direction); cooldown=max(0,int(self.cfg.same_direction_cooldown_bars))
        if not reversal and last is not None and self.bars_processed-last<=cooldown: return self.status('COOLDOWN')
        self._open(direction,now,signal); return self.status('UPDATED')
    def _update_excursion(self,p,high,low):
        favourable=(high-p.entry_price) if p.direction=='BUY' else (p.entry_price-low)
        p.max_favorable_r=max(p.max_favorable_r,max(0.0,favourable)/p.initial_risk_per_unit)
    def _level(self,p,r): return p.entry_price+(r*p.initial_risk_per_unit)*(1 if p.direction=='BUY' else -1)
    def _bar_exit(self,p,high,low,now):
        # Check the stop that was active before this completed candle.
        stop_hit=low<=p.active_stop_loss if p.direction=='BUY' else high>=p.active_stop_loss
        if stop_hit: return ('PROTECTED_STOP' if p.breakeven_activated else 'STOP_LOSS',p.active_stop_loss)
        # With OHLC candles, if the active stop and a target are both touched
        # in one candle, the stop wins. Only record milestones after this check.
        favourable=(high-p.entry_price) if p.direction=='BUY' else (p.entry_price-low)
        reached_r=max(0.0,favourable)/p.initial_risk_per_unit if p.initial_risk_per_unit else 0.0
        for milestone in (1,2,3,4,5):
            if reached_r>=milestone and milestone not in p.reached_milestones: p.reached_milestones.append(milestone)
        # TP5 closes the entire remaining position. TP1 only records; TP2-4 ratchet stop.
        target5=self._level(p,5)
        if (high>=target5 if p.direction=='BUY' else low<=target5): return 'TAKE_PROFIT_5R',target5
        reached=max(p.reached_milestones or [0])
        lock_r={2:1,3:2,4:3}.get(reached)
        if lock_r is not None:
            new_stop=self._level(p,lock_r)
            improves=(new_stop>p.active_stop_loss if p.direction=='BUY' else new_stop<p.active_stop_loss)
            if improves:
                p.active_stop_loss=new_stop; p.breakeven_activated=True; p.breakeven_time=now
                self.pending_events.append(('gold_stop_milestone_advanced',{'trade_id':p.trade_id,'direction':p.direction,'entry_price':p.entry_price,'original_stop_loss':p.stop_loss,'active_stop_loss':new_stop,'locked_r':lock_r,'milestone_reached':reached,'reached_milestones':list(p.reached_milestones),'bar_time':str(now)}))
                if (low<=new_stop if p.direction=='BUY' else high>=new_stop): return 'PROTECTED_STOP',new_stop
        return None,None
    def _open(self,direction,ts,signal):
        entry=float(signal.get('entry',signal.get('price',float('nan')))); sl=float(signal.get('sl',signal.get('stop_loss',float('nan'))))
        if not all(math.isfinite(x) for x in (entry,sl)) or (direction=='BUY' and sl>=entry) or (direction=='SELL' and sl<=entry): self.last_signal={**signal,'engine_error':'invalid_entry_or_stop'}; return
        risk=abs(entry-sl); amount=self.balance*float(self.cfg.risk_percent)/100; qty=amount/(risk*float(self.cfg.point_value))
        if risk<=0 or qty<=0 or not math.isfinite(qty): self.last_signal={**signal,'engine_error':'invalid_position_size'}; return
        tp5=entry+(5*risk if direction=='BUY' else -5*risk)
        self.position=PaperPosition(direction,entry,ts,sl,amount,qty,risk,self.balance,tp5,sl); self.last_entry_by_direction[direction]=self.bars_processed
    def _close(self,price,ts,reason):
        p=self.position
        if not p:return
        pnl=self._pnl(p.direction,p.entry_price,price,p.quantity); self.balance+=pnl; rr=pnl/p.risk_amount if p.risk_amount else 0
        self.closed_trades.append({'trade_id':p.trade_id,'pair':self.cfg.pair,'strategy':'GOLD_REVERSAL_SCALPER','strategy_name':'Gold Reversal Scalper','direction':p.direction,'entry_time':p.entry_time,'exit_time':ts,'entry_price':p.entry_price,'exit_price':float(price),'stop_loss':p.stop_loss,'active_stop_loss':p.active_stop_loss,'take_profit':p.take_profit,'breakeven_activated':p.breakeven_activated,'breakeven_time':p.breakeven_time,'exit_method':'TP1-TP5_MILESTONE_STOP_LADDER','risk_amount':p.risk_amount,'risk_percent':float(self.cfg.risk_percent),'quantity':p.quantity,'point_value':float(self.cfg.point_value),'initial_risk_per_unit':p.initial_risk_per_unit,'realized_rr':rr,'max_favorable_r':p.max_favorable_r,'reached_milestones':list(p.reached_milestones),'hold_bars':p.hold_bars,'pnl':pnl,'pnl_r':rr,'balance_after':self.balance,'exit_reason':reason,'result':'WIN' if pnl>0 else 'LOSS' if pnl<0 else 'BREAKEVEN'})
        self.position=None
    def _pnl(self,d,e,x,q): return ((x-e) if d=='BUY' else (e-x))*q*float(self.cfg.point_value)
    def status(self,state='UPDATED'):
        t=self.closed_trades; wins=sum(x['pnl']>0 for x in t); losses=sum(x['pnl']<0 for x in t)
        return {'state':state,'pair':self.cfg.pair,'strategy':'GOLD_REVERSAL_SCALPER','strategy_name':'Gold Reversal Scalper','balance':self.balance,'starting_balance':float(self.cfg.starting_balance),'net_pnl':self.balance-float(self.cfg.starting_balance),'risk_percent':float(self.cfg.risk_percent),'point_value':float(self.cfg.point_value),'profit_lock_r':None,'take_profit_r':5.0,'position':asdict(self.position) if self.position else None,'last_signal':self.last_signal,'closed_trades':len(t),'wins':wins,'losses':losses,'win_rate':wins/len(t)*100 if t else 0.0,'bars_processed':self.bars_processed}
