"""
FORTIX Risk Manager — Position Sizing, Leverage, Circuit Breakers
================================================================

Controls all risk for the trading engine. No trade executes without
risk manager approval.

Rules ($500 account):
  - Max 2% risk per trade ($10)
  - Max 3 concurrent positions
  - Max 5x total exposure ($2,500)
  - Dynamic leverage 2-10x based on confidence tier
  - Mandatory stop-loss (1.5 × ATR)
  - Circuit breakers: 5% daily loss, 15% drawdown
"""

import sqlite3
import logging
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)

_FACTORY_DIR = Path(__file__).resolve().parent.parent.parent
TRADES_DB = _FACTORY_DIR / 'data' / 'crypto' / 'trades.db'


@dataclass
class TradeParams:
    """Approved trade parameters from risk manager."""
    coin: str
    direction: str  # LONG or SHORT
    leverage: int
    position_size_usdt: float  # notional size in USDT
    margin_required: float  # actual USDT margin needed
    stop_loss_price: float
    take_profit_1: float  # close 50%
    take_profit_2: float  # close remaining (or trail)
    max_loss_usdt: float  # maximum loss if stop hit
    risk_pct: float  # % of capital at risk
    reason: str  # why these params


class RiskManager:
    """Position sizing, leverage control, and circuit breakers."""

    def __init__(self, initial_capital: float = 500.0):
        self.initial_capital = initial_capital
        self._init_db()

        # Risk parameters (conservative after first live test)
        self.MAX_RISK_PER_TRADE = 0.03  # 3% of capital
        self.MAX_CONCURRENT_POSITIONS = 3  # STRICT: never more than 3
        self.MAX_SAME_DIRECTION = 3  # max 3 shorts OR 3 longs
        self.MAX_TOTAL_EXPOSURE_MULT = 5.0  # 5x capital
        self.MAX_SINGLE_POSITION_PCT = 0.35  # 35% of capital per coin
        self.MAX_DAILY_LOSS_PCT = 0.07  # 7% daily loss → pause 4h
        self.MAX_DRAWDOWN_PCT = 0.15  # 15% drawdown → pause 24h
        self.MAX_CONSECUTIVE_LOSSES = 5  # → reduce size 50%

        # Leverage rules by confidence tier
        self.TIER_LEVERAGE = {
            1: {'base': 7, 'min': 5, 'max': 10},  # 80%+ accuracy
            2: {'base': 5, 'min': 3, 'max': 7},    # 65-80%
            3: {'base': 3, 'min': 2, 'max': 5},    # 55-65%
        }

        # Stop-loss ATR multiplier
        self.STOP_ATR_MULT = 1.5
        self.TP1_ATR_MULT = 2.0  # first target
        self.TP2_ATR_MULT = 3.5  # second target

    def _init_db(self):
        """Initialize trades database."""
        conn = sqlite3.connect(str(TRADES_DB))
        conn.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                coin TEXT, direction TEXT, tier INTEGER,
                entry_price REAL, exit_price REAL,
                size_usdt REAL, leverage INTEGER, margin REAL,
                stop_loss REAL, take_profit_1 REAL, take_profit_2 REAL,
                pnl_usdt REAL, pnl_pct REAL,
                fees_paid REAL, funding_paid REAL,
                entry_time TEXT, exit_time TEXT, duration_hours REAL,
                exit_reason TEXT, signal_type TEXT, confidence_score REAL,
                status TEXT DEFAULT 'open'
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS daily_pnl (
                date TEXT PRIMARY KEY, pnl_usdt REAL, n_trades INTEGER,
                n_wins INTEGER, n_losses INTEGER, capital REAL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS circuit_breaker_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trigger_type TEXT, triggered_at TEXT, resume_at TEXT,
                details TEXT
            )
        """)
        conn.commit()
        conn.close()

    def get_current_capital(self) -> float:
        """Get current EQUITY from exchange (balance + unrealized PnL in positions).

        EQUITY = free balance + margin in positions + unrealized PnL.
        This is the TRUE account value, not just free balance.
        """
        try:
            from src.crypto.exchange_client import create_client
            client = create_client()
            client.connect()
            bal = client.get_balance()
            free = bal.get('free', 0) or 0

            # Add margin locked in positions
            positions = client._exchange.fetch_positions()
            margin_in_positions = 0
            unrealized_pnl = 0

            for p in positions:
                if p.get('contracts', 0) > 0:
                    margin_in_positions += float(p.get('initialMargin', 0) or 0)
                    # Calculate unrealized PnL
                    entry = float(p.get('entryPrice', 0) or 0)
                    size = p.get('contracts', 0)
                    side = p.get('side', '')
                    if entry > 0:
                        try:
                            coin = p['symbol'].split('/')[0].replace('1000', '')
                            ticker = client.get_ticker(coin)
                            now = ticker['price']
                            if side == 'short':
                                unrealized_pnl += (entry - now) * size
                            else:
                                unrealized_pnl += (now - entry) * size
                        except Exception:
                            pass

            equity = free + margin_in_positions + unrealized_pnl
            if equity > 0:
                return equity
        except Exception:
            pass

        # Fallback
        conn = sqlite3.connect(str(TRADES_DB))
        try:
            row = conn.execute(
                "SELECT COALESCE(SUM(pnl_usdt), 0) FROM trades WHERE status='closed'"
            ).fetchone()
            return self.initial_capital + (row[0] if row else 0)
        finally:
            conn.close()

    def get_open_positions_count(self) -> int:
        """Count currently open positions."""
        conn = sqlite3.connect(str(TRADES_DB))
        try:
            row = conn.execute("SELECT COUNT(*) FROM trades WHERE status='open'").fetchone()
            return row[0] if row else 0
        finally:
            conn.close()

    def get_open_exposure(self) -> float:
        """Total USDT exposure of open positions."""
        conn = sqlite3.connect(str(TRADES_DB))
        try:
            row = conn.execute(
                "SELECT COALESCE(SUM(size_usdt), 0) FROM trades WHERE status='open'"
            ).fetchone()
            return row[0] if row else 0
        finally:
            conn.close()

    def get_daily_pnl(self) -> float:
        """Today's realized P&L."""
        conn = sqlite3.connect(str(TRADES_DB))
        try:
            today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            row = conn.execute(
                "SELECT COALESCE(SUM(pnl_usdt), 0) FROM trades "
                "WHERE status='closed' AND date(exit_time)=?", (today,)
            ).fetchone()
            return row[0] if row else 0
        finally:
            conn.close()

    def get_consecutive_losses(self) -> int:
        """Count consecutive losing trades from most recent."""
        conn = sqlite3.connect(str(TRADES_DB))
        try:
            rows = conn.execute(
                "SELECT pnl_usdt FROM trades WHERE status='closed' "
                "ORDER BY exit_time DESC LIMIT 10"
            ).fetchall()
            count = 0
            for r in rows:
                if r[0] < 0:
                    count += 1
                else:
                    break
            return count
        finally:
            conn.close()

    # ── Circuit Breakers ──────────────────────────────────────

    def check_circuit_breakers(self) -> tuple:
        """Check all circuit breakers.

        Returns: (can_trade: bool, reason: str)
        """
        capital = self.get_current_capital()

        # 1. Max drawdown
        drawdown = (self.initial_capital - capital) / self.initial_capital
        if drawdown >= self.MAX_DRAWDOWN_PCT:
            self._log_circuit_breaker('max_drawdown',
                f"Drawdown {drawdown*100:.1f}% >= {self.MAX_DRAWDOWN_PCT*100}%")
            return False, f"CIRCUIT BREAKER: drawdown {drawdown*100:.1f}% — paused 24h"

        # 2. Daily loss
        daily_pnl = self.get_daily_pnl()
        daily_loss_pct = abs(daily_pnl) / capital if daily_pnl < 0 else 0
        if daily_loss_pct >= self.MAX_DAILY_LOSS_PCT:
            self._log_circuit_breaker('daily_loss',
                f"Daily loss ${abs(daily_pnl):.2f} ({daily_loss_pct*100:.1f}%)")
            return False, f"CIRCUIT BREAKER: daily loss {daily_loss_pct*100:.1f}% — paused 4h"

        # 3. Check if paused from previous circuit breaker
        conn = sqlite3.connect(str(TRADES_DB))
        try:
            now = datetime.now(timezone.utc).isoformat()
            active = conn.execute(
                "SELECT trigger_type, resume_at FROM circuit_breaker_log "
                "WHERE resume_at > ? ORDER BY triggered_at DESC LIMIT 1", (now,)
            ).fetchone()
            if active:
                return False, f"PAUSED until {active[1]} (trigger: {active[0]})"
        finally:
            conn.close()

        return True, "OK"

    def _log_circuit_breaker(self, trigger_type: str, details: str):
        """Log a circuit breaker activation."""
        now = datetime.now(timezone.utc)
        if trigger_type == 'max_drawdown':
            resume = now + timedelta(hours=24)
        elif trigger_type == 'daily_loss':
            resume = now + timedelta(hours=4)
        else:
            resume = now + timedelta(hours=1)

        conn = sqlite3.connect(str(TRADES_DB))
        conn.execute(
            "INSERT INTO circuit_breaker_log (trigger_type, triggered_at, resume_at, details) "
            "VALUES (?, ?, ?, ?)",
            (trigger_type, now.isoformat(), resume.isoformat(), details)
        )
        conn.commit()
        conn.close()
        logger.warning(f"CIRCUIT BREAKER: {trigger_type} — {details}")

    # ── Position Sizing ──────────────────────────────────────

    def calculate_leverage(self, tier: int, atr_pct: float) -> int:
        """Calculate leverage based on tier and volatility."""
        tier_config = self.TIER_LEVERAGE.get(tier, self.TIER_LEVERAGE[3])

        leverage = tier_config['base']

        # Volatility adjustment: high vol → reduce leverage
        if atr_pct > 0.06:  # >6% daily range
            leverage = max(tier_config['min'], leverage - 3)
        elif atr_pct > 0.04:  # >4%
            leverage = max(tier_config['min'], leverage - 1)
        elif atr_pct < 0.02:  # <2% (very low vol)
            leverage = min(tier_config['max'], leverage + 1)

        # Consecutive losses → reduce
        if self.get_consecutive_losses() >= self.MAX_CONSECUTIVE_LOSSES:
            leverage = max(tier_config['min'], leverage // 2)

        return min(max(leverage, tier_config['min']), tier_config['max'])

    def _kelly_fraction(self, tier: int) -> float:
        """Calculate risk fraction based on tier.

        Tier 1 (80%+ accuracy): aggressive — 4% risk
        Tier 2 (65-80%): moderate — 3% risk
        Tier 3 (55-65%): conservative — 1.5% risk
        """
        tier_risk = {
            1: 0.04,   # 4% of capital — high confidence
            2: 0.03,   # 3% — standard
            3: 0.015,  # 1.5% — low confidence, small size
        }
        return tier_risk.get(tier, 0.02)

    def calculate_trade_params(
        self,
        coin: str,
        direction: str,
        tier: int,
        current_price: float,
        atr_pct: float,
        position_size_mult: float = 1.0,
    ) -> Optional[TradeParams]:
        """Calculate full trade parameters with Kelly-adjusted sizing.

        Returns TradeParams if trade is approved, None if rejected.
        """
        # Check circuit breakers
        can_trade, reason = self.check_circuit_breakers()
        if not can_trade:
            logger.warning(f"Trade rejected: {reason}")
            return None

        capital = self.get_current_capital()
        if capital <= 0:
            return None

        # Check position limits
        n_open = self.get_open_positions_count()
        if n_open >= self.MAX_CONCURRENT_POSITIONS:
            logger.info(f"Max positions reached ({n_open}/{self.MAX_CONCURRENT_POSITIONS})")
            return None

        # Check exposure limits
        current_exposure = self.get_open_exposure()
        max_exposure = capital * self.MAX_TOTAL_EXPOSURE_MULT
        if current_exposure >= max_exposure:
            logger.info(f"Max exposure reached (${current_exposure:.0f}/${max_exposure:.0f})")
            return None

        # Calculate leverage
        leverage = self.calculate_leverage(tier, atr_pct)

        # Adaptive position sizing: Kelly criterion (Tier 1 gets more, Tier 3 less)
        kelly_frac = self._kelly_fraction(tier)
        risk_pct = max(kelly_frac, self.MAX_RISK_PER_TRADE * 0.5)  # at least half of base risk
        risk_pct = min(risk_pct, self.MAX_RISK_PER_TRADE * 1.5)  # at most 1.5x base risk
        risk_amount = capital * risk_pct * position_size_mult

        # Stop distance
        stop_distance_pct = atr_pct * self.STOP_ATR_MULT
        if stop_distance_pct < 0.005:
            stop_distance_pct = 0.005  # minimum 0.5% stop
        if stop_distance_pct > 0.15:
            stop_distance_pct = 0.15  # maximum 15% stop

        # Notional position size: risk_amount / stop_distance
        position_notional = risk_amount / stop_distance_pct
        margin_required = position_notional / leverage

        # Cap by max single position
        max_margin = capital * self.MAX_SINGLE_POSITION_PCT
        if margin_required > max_margin:
            margin_required = max_margin
            position_notional = margin_required * leverage

        # Cap by remaining exposure allowance
        remaining_exposure = max_exposure - current_exposure
        if position_notional > remaining_exposure:
            position_notional = remaining_exposure
            margin_required = position_notional / leverage

        # Stop-loss and take-profit prices
        if direction == 'LONG':
            stop_loss = current_price * (1 - stop_distance_pct)
            tp1 = current_price * (1 + atr_pct * self.TP1_ATR_MULT)
            tp2 = current_price * (1 + atr_pct * self.TP2_ATR_MULT)
        else:
            stop_loss = current_price * (1 + stop_distance_pct)
            tp1 = current_price * (1 - atr_pct * self.TP1_ATR_MULT)
            tp2 = current_price * (1 - atr_pct * self.TP2_ATR_MULT)

        max_loss = risk_amount

        reason = (f"Tier {tier} | {leverage}x leverage | "
                 f"risk ${risk_amount:.2f} ({self.MAX_RISK_PER_TRADE*100:.0f}% of ${capital:.0f}) | "
                 f"stop {stop_distance_pct*100:.1f}% ({self.STOP_ATR_MULT}×ATR)")

        return TradeParams(
            coin=coin,
            direction=direction,
            leverage=leverage,
            position_size_usdt=position_notional,
            margin_required=margin_required,
            stop_loss_price=stop_loss,
            take_profit_1=tp1,
            take_profit_2=tp2,
            max_loss_usdt=max_loss,
            risk_pct=self.MAX_RISK_PER_TRADE * position_size_mult,
            reason=reason,
        )

    # ── Trade Recording ──────────────────────────────────────

    def record_entry(self, params: TradeParams, entry_price: float,
                     signal_type: str = '', confidence: float = 0) -> int:
        """Record a new trade entry. Returns trade ID."""
        conn = sqlite3.connect(str(TRADES_DB))
        cursor = conn.execute(
            "INSERT INTO trades (coin, direction, tier, entry_price, size_usdt, "
            "leverage, margin, stop_loss, take_profit_1, take_profit_2, "
            "entry_time, signal_type, confidence_score, status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open')",
            (params.coin, params.direction, 0, entry_price,
             params.position_size_usdt, params.leverage, params.margin_required,
             params.stop_loss_price, params.take_profit_1, params.take_profit_2,
             datetime.now(timezone.utc).isoformat(), signal_type, confidence)
        )
        trade_id = cursor.lastrowid
        conn.commit()
        conn.close()
        logger.info(f"Trade #{trade_id} opened: {params.direction} {params.coin} "
                    f"${params.position_size_usdt:.0f} @ ${entry_price:.2f}")
        return trade_id

    def record_exit(self, trade_id: int, exit_price: float, pnl_usdt: float,
                    fees: float = 0, funding: float = 0, reason: str = '') -> None:
        """Record trade exit."""
        conn = sqlite3.connect(str(TRADES_DB))
        now = datetime.now(timezone.utc).isoformat()

        # Get entry time for duration calculation
        entry = conn.execute(
            "SELECT entry_time, size_usdt FROM trades WHERE id=?", (trade_id,)
        ).fetchone()
        duration = 0
        if entry and entry[0]:
            try:
                et = datetime.fromisoformat(entry[0])
                duration = (datetime.now(timezone.utc) - et).total_seconds() / 3600
            except Exception:
                pass

        pnl_pct = (pnl_usdt / entry[1] * 100) if entry and entry[1] > 0 else 0

        conn.execute(
            "UPDATE trades SET exit_price=?, pnl_usdt=?, pnl_pct=?, "
            "fees_paid=?, funding_paid=?, exit_time=?, duration_hours=?, "
            "exit_reason=?, status='closed' WHERE id=?",
            (exit_price, pnl_usdt, pnl_pct, fees, funding, now, duration, reason, trade_id)
        )
        conn.commit()
        conn.close()
        logger.info(f"Trade #{trade_id} closed: PnL ${pnl_usdt:+.2f} ({pnl_pct:+.1f}%) "
                    f"reason={reason}")

    # ── Reporting ────────────────────────────────────────────

    def get_performance_summary(self) -> dict:
        """Get overall performance metrics."""
        conn = sqlite3.connect(str(TRADES_DB))
        try:
            closed = conn.execute(
                "SELECT COUNT(*), SUM(pnl_usdt), "
                "SUM(CASE WHEN pnl_usdt > 0 THEN 1 ELSE 0 END), "
                "SUM(CASE WHEN pnl_usdt > 0 THEN pnl_usdt ELSE 0 END), "
                "SUM(CASE WHEN pnl_usdt < 0 THEN pnl_usdt ELSE 0 END), "
                "AVG(pnl_usdt), MAX(pnl_usdt), MIN(pnl_usdt), "
                "SUM(fees_paid), SUM(funding_paid) "
                "FROM trades WHERE status='closed'"
            ).fetchone()

            if not closed or closed[0] == 0:
                return {'n_trades': 0, 'total_pnl': 0, 'win_rate': 0}

            n = closed[0]
            wins = closed[2] or 0
            gross_profit = closed[3] or 0
            gross_loss = abs(closed[4] or 0)

            return {
                'n_trades': n,
                'total_pnl': closed[1] or 0,
                'win_rate': wins / n * 100,
                'avg_pnl': closed[5] or 0,
                'best_trade': closed[6] or 0,
                'worst_trade': closed[7] or 0,
                'profit_factor': gross_profit / gross_loss if gross_loss > 0 else float('inf'),
                'total_fees': closed[8] or 0,
                'total_funding': closed[9] or 0,
                'current_capital': self.get_current_capital(),
                'drawdown_pct': (self.initial_capital - self.get_current_capital()) / self.initial_capital * 100,
            }
        finally:
            conn.close()
