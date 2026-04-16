"""
FORTIX Position Exit Manager — Smart Exit Logic
================================================

Manages open positions with intelligent exit rules:
  1. Trailing stop — moves with price, locks in profit
  2. Signal-based exit — close when signal disappears or reverses
  3. Funding fee tracking — close if funding costs exceed expected profit
  4. Time-based exit — close after 48h (funding drain)
  5. Hard stop — emergency close at -15% leveraged

All exits use reduceOnly to properly close positions.
"""

import time
import sqlite3
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from typing import Optional, Dict

logger = logging.getLogger(__name__)

_FACTORY_DIR = Path(__file__).resolve().parent.parent.parent
TRADES_DB = _FACTORY_DIR / 'data' / 'crypto' / 'trades.db'
MARKET_DB = _FACTORY_DIR / 'data' / 'crypto' / 'market.db'


@dataclass
class TrackedPosition:
    """A position being actively managed."""
    coin: str
    side: str  # 'short' or 'long'
    size: float
    entry_price: float
    leverage: int
    entry_time: str  # ISO format
    # Trailing stop state
    best_price: float = 0  # best price since entry (highest for long, lowest for short)
    trailing_stop: float = 0  # current trailing stop level
    # Partial exit tracking
    original_size: float = 0
    tp1_hit: bool = False


class PositionExitManager:
    """Manages exits for all open positions."""

    # Exit parameters
    HARD_STOP_PCT = -15.0  # leveraged % → emergency close
    TP1_PCT = 10.0  # leveraged % → close 50%
    TRAILING_ACTIVATION_PCT = 5.0  # leveraged % → start trailing
    TRAILING_DISTANCE_PCT = 3.0  # trail X% behind best price (unleveraged)
    MAX_HOLD_HOURS = 48  # close after 48h
    MAX_FUNDING_COST_PCT = 2.0  # if funding cost > 2% of position → close

    def __init__(self, exchange_client):
        self.exchange = exchange_client
        self.tracked: Dict[str, TrackedPosition] = {}
        self._funding_paid: Dict[str, float] = {}
        self._init_state_db()
        self._load_state()

    def _init_state_db(self):
        """Create persistence table for trailing stop state."""
        conn = sqlite3.connect(str(TRADES_DB))
        conn.execute("""
            CREATE TABLE IF NOT EXISTS position_state (
                coin TEXT PRIMARY KEY,
                side TEXT, size REAL, entry_price REAL, leverage INTEGER,
                entry_time TEXT, best_price REAL, trailing_stop REAL,
                original_size REAL, tp1_hit INTEGER DEFAULT 0,
                funding_paid REAL DEFAULT 0
            )
        """)
        conn.commit()
        conn.close()

    def _save_state(self):
        """Persist position state to DB (survives restarts)."""
        conn = sqlite3.connect(str(TRADES_DB))
        conn.execute("DELETE FROM position_state")
        for coin, pos in self.tracked.items():
            conn.execute(
                "INSERT OR REPLACE INTO position_state VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (coin, pos.side, pos.size, pos.entry_price, pos.leverage,
                 pos.entry_time, pos.best_price, pos.trailing_stop,
                 pos.original_size, 1 if pos.tp1_hit else 0,
                 self._funding_paid.get(coin, 0))
            )
        conn.commit()
        conn.close()

    def _load_state(self):
        """Load position state from DB (after restart)."""
        try:
            conn = sqlite3.connect(str(TRADES_DB))
            rows = conn.execute("SELECT * FROM position_state").fetchall()
            for r in rows:
                self.tracked[r[0]] = TrackedPosition(
                    coin=r[0], side=r[1], size=r[2], entry_price=r[3],
                    leverage=r[4], entry_time=r[5], best_price=r[6],
                    trailing_stop=r[7], original_size=r[8], tp1_hit=bool(r[9])
                )
                if r[10]:
                    self._funding_paid[r[0]] = r[10]
            conn.close()
            if self.tracked:
                logger.info(f"Loaded {len(self.tracked)} position states from DB")
        except Exception as e:
            logger.debug(f"No saved position state: {e}")

    def sync_positions(self):
        """Sync tracked positions with exchange."""
        try:
            positions = self.exchange.get_positions()
            exchange_coins = set()

            for pos in positions:
                coin = pos.symbol.split('/')[0].replace('1000', '')
                exchange_coins.add(coin)

                if coin not in self.tracked:
                    # New position found on exchange — start tracking
                    self.tracked[coin] = TrackedPosition(
                        coin=coin,
                        side=pos.side,
                        size=pos.size,
                        entry_price=pos.entry_price or 0,
                        leverage=pos.leverage or 5,
                        entry_time=datetime.now(timezone.utc).isoformat(),
                        best_price=pos.entry_price or 0,
                        original_size=pos.size,
                    )
                    logger.info(f"Tracking new position: {coin} {pos.side} {pos.size}")
                else:
                    # Update size (may have changed from partial exit)
                    self.tracked[coin].size = pos.size

            # Remove positions that no longer exist on exchange
            closed = [c for c in self.tracked if c not in exchange_coins]
            for c in closed:
                logger.info(f"Position closed externally: {c}")
                del self.tracked[c]

            # Persist state after sync
            self._save_state()

        except Exception as e:
            logger.error(f"Position sync failed: {e}")

    def check_all_exits(self, notify_fn=None) -> list:
        """Check all exit rules for all positions. Returns list of actions taken."""
        actions = []

        for coin, pos in list(self.tracked.items()):
            try:
                action = self._check_position(pos, notify_fn)
                if action:
                    actions.append(action)
            except Exception as e:
                logger.error(f"Exit check failed for {coin}: {e}")

        return actions

    def _check_position(self, pos: TrackedPosition, notify_fn=None) -> Optional[str]:
        """Check all exit rules for one position."""
        try:
            ticker = self.exchange.get_ticker(pos.coin)
            current_price = ticker['price']
        except Exception:
            return None

        if current_price <= 0 or pos.entry_price <= 0:
            return None

        # Calculate P&L
        if pos.side == 'short':
            pnl_pct = (pos.entry_price - current_price) / pos.entry_price * 100
        else:
            pnl_pct = (current_price - pos.entry_price) / pos.entry_price * 100

        pnl_leveraged = pnl_pct * pos.leverage

        # Update best price (for trailing stop)
        if pos.side == 'long':
            if current_price > pos.best_price:
                pos.best_price = current_price
        else:
            if pos.best_price == 0 or current_price < pos.best_price:
                pos.best_price = current_price

        # ── RULE 1: HARD STOP ──
        if pnl_leveraged <= self.HARD_STOP_PCT:
            return self._close_position(pos, current_price, 'HARD_STOP',
                f"Loss {pnl_leveraged:+.1f}% exceeded {self.HARD_STOP_PCT}%", notify_fn)

        # ── RULE 2: TRAILING STOP ──
        if pnl_leveraged >= self.TRAILING_ACTIVATION_PCT:
            # Calculate trailing stop level
            trail_dist = pos.entry_price * self.TRAILING_DISTANCE_PCT / 100

            if pos.side == 'long':
                new_trail = pos.best_price - trail_dist
                if new_trail > pos.trailing_stop:
                    pos.trailing_stop = new_trail
                if current_price <= pos.trailing_stop and pos.trailing_stop > 0:
                    return self._close_position(pos, current_price, 'TRAILING_STOP',
                        f"Price ${current_price:.4f} hit trailing stop ${pos.trailing_stop:.4f} "
                        f"(best was ${pos.best_price:.4f})", notify_fn)
            else:
                new_trail = pos.best_price + trail_dist
                if pos.trailing_stop == 0 or new_trail < pos.trailing_stop:
                    pos.trailing_stop = new_trail
                if current_price >= pos.trailing_stop and pos.trailing_stop > 0:
                    return self._close_position(pos, current_price, 'TRAILING_STOP',
                        f"Price ${current_price:.4f} hit trailing stop ${pos.trailing_stop:.4f} "
                        f"(best was ${pos.best_price:.4f})", notify_fn)

        # ── RULE 3: TAKE PROFIT 1 (close 50%) ──
        if pnl_leveraged >= self.TP1_PCT and not pos.tp1_hit and pos.size > 1:
            half = int(pos.size / 2)
            if half > 0:
                close_side = 'buy' if pos.side == 'short' else 'sell'
                result = self.exchange.close_position(pos.coin, close_side, half)
                if result:
                    pos.tp1_hit = True
                    pos.size -= half
                    msg = f"TP1 hit: closed 50% ({half}) at {pnl_leveraged:+.1f}%"
                    logger.info(f"💰 {pos.coin}: {msg}")
                    if notify_fn:
                        notify_fn(f"💰 TP1 {pos.coin}", msg)
                    return f"TP1 {pos.coin}"

        # ── RULE 4: TIME EXIT (48h) ──
        try:
            entry_dt = datetime.fromisoformat(pos.entry_time)
            hours_held = (datetime.now(timezone.utc) - entry_dt).total_seconds() / 3600
            if hours_held >= self.MAX_HOLD_HOURS:
                return self._close_position(pos, current_price, 'TIME_EXIT',
                    f"Held {hours_held:.0f}h > {self.MAX_HOLD_HOURS}h max, PnL: {pnl_leveraged:+.1f}%",
                    notify_fn)
        except Exception:
            pass

        # ── RULE 5: FUNDING FEE CHECK ──
        try:
            funding_cost = self._get_funding_cost(pos.coin, pos.side)
            if funding_cost is not None:
                cost_pct = funding_cost / (pos.entry_price * pos.size) * 100
                self._funding_paid[pos.coin] = self._funding_paid.get(pos.coin, 0) + cost_pct
                if self._funding_paid[pos.coin] > self.MAX_FUNDING_COST_PCT:
                    if pnl_leveraged < self._funding_paid[pos.coin]:
                        return self._close_position(pos, current_price, 'FUNDING_DRAIN',
                            f"Funding cost {self._funding_paid[pos.coin]:.2f}% > profit {pnl_leveraged:+.1f}%",
                            notify_fn)
        except Exception:
            pass

        return None

    def _close_position(self, pos: TrackedPosition, price: float,
                        reason: str, details: str, notify_fn=None) -> str:
        """Close a full position."""
        close_side = 'buy' if pos.side == 'short' else 'sell'
        result = self.exchange.close_position(pos.coin, close_side, pos.size)

        if result:
            # Calculate final P&L
            if pos.side == 'short':
                pnl = (pos.entry_price - price) * pos.original_size
            else:
                pnl = (price - pos.entry_price) * pos.original_size

            msg = f"{reason}: {pos.coin} {pos.side} | PnL: ${pnl:+.2f} | {details}"
            logger.info(msg)

            if notify_fn:
                emoji = "💰" if pnl > 0 else "🛑"
                notify_fn(f"{emoji} {reason} {pos.coin}", details)

            # Remove from tracking
            if pos.coin in self.tracked:
                del self.tracked[pos.coin]

            return msg
        else:
            logger.error(f"Failed to close {pos.coin}: {reason}")
            return f"FAILED to close {pos.coin}"

    def _get_funding_cost(self, coin: str, side: str) -> Optional[float]:
        """Get recent funding rate cost for this position."""
        try:
            conn = sqlite3.connect(str(MARKET_DB))
            row = conn.execute(
                "SELECT rate FROM funding_rates WHERE coin=? ORDER BY timestamp DESC LIMIT 1",
                (coin,)
            ).fetchone()
            conn.close()
            if row:
                rate = row[0]
                # If we're short and funding is negative → we PAY
                # If we're short and funding is positive → we RECEIVE
                if side == 'short':
                    return -rate  # negative rate = cost for shorts
                else:
                    return rate  # positive rate = cost for longs
        except Exception:
            pass
        return None

    def check_signal_reversal(self, coin: str, current_signals: list) -> bool:
        """Check if signals have reversed for a position."""
        if coin not in self.tracked:
            return False

        pos = self.tracked[coin]

        # Find signals for this coin
        coin_signals = [s for s in current_signals if s.coin == coin]

        if not coin_signals:
            return False

        # Check for reversal
        for sig in coin_signals:
            if pos.side == 'short' and sig.direction == 'BULLISH' and sig.strength >= 0.6:
                return True
            if pos.side == 'long' and sig.direction == 'BEARISH' and sig.strength >= 0.6:
                return True

        return False

    def get_status(self) -> str:
        """Get status of all tracked positions."""
        if not self.tracked:
            return "No open positions"

        lines = [f"Tracking {len(self.tracked)} positions:"]
        for coin, pos in self.tracked.items():
            try:
                ticker = self.exchange.get_ticker(coin)
                current = ticker['price']
                if pos.side == 'short':
                    pnl = (pos.entry_price - current) / pos.entry_price * 100 * pos.leverage
                else:
                    pnl = (current - pos.entry_price) / pos.entry_price * 100 * pos.leverage

                trail = f" trail=${pos.trailing_stop:.4f}" if pos.trailing_stop > 0 else ""
                tp1 = " [TP1✓]" if pos.tp1_hit else ""
                lines.append(f"  {coin:6s} {pos.side:5s} {pos.leverage}x: {pnl:+.1f}%{trail}{tp1}")
            except Exception:
                lines.append(f"  {coin:6s} {pos.side:5s}: price unavailable")

        return "\n".join(lines)
