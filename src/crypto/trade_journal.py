"""
FORTIX Trade Journal — Learning System
========================================
Records every trade with full market context.
Provides feedback to Opus for learning from mistakes.

Lifecycle: PENDING → FILLED → CLOSED (or PENDING → CANCELLED)
Each stage captures market snapshot for post-analysis.
"""

import json
import time
import uuid
import sqlite3
import logging
from pathlib import Path
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

_FACTORY_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = _FACTORY_DIR / 'data' / 'crypto' / 'market.db'


class TradeJournal:
    """Records trade lifecycle with market context for Opus learning."""

    def __init__(self, exchange=None):
        self.exchange = exchange
        self._init_table()

    def _conn(self):
        conn = sqlite3.connect(str(DB_PATH), timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _init_table(self):
        conn = self._conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fortix_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_id TEXT UNIQUE,
                coin TEXT NOT NULL,
                direction TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'PENDING',
                entry_type TEXT,

                entry_price REAL,
                fill_price REAL,
                sl_price REAL,
                tp_price REAL,
                exit_price REAL,

                regime TEXT,
                confidence REAL,
                reason TEXT,
                leverage INTEGER,

                fill_ob_imbalance REAL,
                fill_ob_buy_wall TEXT,
                fill_ob_sell_wall TEXT,
                fill_momentum_15m REAL,
                fill_funding_rate REAL,
                fill_atr_1h REAL,

                pnl_pct REAL,
                pnl_usd REAL,
                exit_reason TEXT,
                held_minutes REAL,

                price_at_cancel REAL,
                would_have_hit_tp INTEGER,
                would_have_hit_sl INTEGER,

                created_at TEXT,
                filled_at TEXT,
                closed_at TEXT,
                notional REAL,
                position_size REAL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ft_status ON fortix_trades(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ft_created ON fortix_trades(created_at)")
        conn.commit()
        conn.close()

    # === RECORD EVENTS ===

    def record_order_placed(self, coin, direction, entry_price, sl_price, tp_price,
                            leverage, confidence, reason, regime, size=0, notional=0) -> str:
        """Record limit order placement. Returns trade_id."""
        trade_id = str(uuid.uuid4())[:8]
        entry_type = 'AGGRESSIVE' if 'aggressive' in reason.lower() or 'near live' in reason.lower() else 'PATIENT'
        conn = self._conn()
        conn.execute("""
            INSERT INTO fortix_trades
            (trade_id, coin, direction, status, entry_type, entry_price, sl_price, tp_price,
             regime, confidence, reason, leverage, created_at, position_size, notional)
            VALUES (?, ?, ?, 'PENDING', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (trade_id, coin, direction, entry_type, entry_price, sl_price, tp_price,
              regime, confidence, reason, leverage,
              datetime.now(timezone.utc).isoformat(), size, notional))
        conn.commit()
        conn.close()
        return trade_id

    def record_fill(self, trade_id, fill_price, fill_amount) -> None:
        """Record order fill with market context snapshot."""
        context = self._capture_context(trade_id)
        conn = self._conn()
        conn.execute("""
            UPDATE fortix_trades SET
                status='FILLED', fill_price=?, position_size=?, filled_at=?,
                fill_ob_imbalance=?, fill_ob_buy_wall=?, fill_ob_sell_wall=?,
                fill_momentum_15m=?, fill_funding_rate=?, fill_atr_1h=?
            WHERE trade_id=?
        """, (fill_price, fill_amount, datetime.now(timezone.utc).isoformat(),
              context.get('ob_imbalance'), context.get('ob_buy_wall'),
              context.get('ob_sell_wall'), context.get('momentum_15m'),
              context.get('funding'), context.get('atr_1h'),
              trade_id))
        conn.commit()
        conn.close()

    def record_close(self, trade_id, exit_price, pnl_pct, pnl_usd,
                     exit_reason, held_minutes) -> None:
        """Record position close with result."""
        conn = self._conn()
        conn.execute("""
            UPDATE fortix_trades SET
                status='CLOSED', exit_price=?, pnl_pct=?, pnl_usd=?,
                exit_reason=?, held_minutes=?, closed_at=?
            WHERE trade_id=?
        """, (exit_price, pnl_pct, pnl_usd, exit_reason, held_minutes,
              datetime.now(timezone.utc).isoformat(), trade_id))
        conn.commit()
        conn.close()

    def record_cancel(self, trade_id, price_at_cancel) -> None:
        """Record order cancellation (unfilled)."""
        conn = self._conn()
        conn.execute("""
            UPDATE fortix_trades SET
                status='CANCELLED', price_at_cancel=?, closed_at=?
            WHERE trade_id=?
        """, (price_at_cancel, datetime.now(timezone.utc).isoformat(), trade_id))
        conn.commit()
        conn.close()

    # === MARKET CONTEXT SNAPSHOT ===

    def _capture_context(self, trade_id) -> dict:
        """Snapshot market state at fill time."""
        if not self.exchange:
            return {}
        conn = self._conn()
        row = conn.execute("SELECT coin FROM fortix_trades WHERE trade_id=?", (trade_id,)).fetchone()
        conn.close()
        if not row:
            return {}
        coin = row[0]

        ctx = {}
        try:
            symbol = self.exchange._symbol(coin)
            # Order book
            ob = self.exchange._exchange.fetch_order_book(symbol, limit=10)
            if ob:
                bid_vol = sum(b[1] * b[0] for b in ob['bids'][:10])
                ask_vol = sum(a[1] * a[0] for a in ob['asks'][:10])
                total = bid_vol + ask_vol
                if total > 0:
                    ctx['ob_imbalance'] = round((bid_vol - ask_vol) / total, 3)
                    biggest_bid = max(ob['bids'][:10], key=lambda x: x[1]*x[0])
                    biggest_ask = max(ob['asks'][:10], key=lambda x: x[1]*x[0])
                    ctx['ob_buy_wall'] = json.dumps({'price': biggest_bid[0], 'usd': round(biggest_bid[1]*biggest_bid[0])})
                    ctx['ob_sell_wall'] = json.dumps({'price': biggest_ask[0], 'usd': round(biggest_ask[1]*biggest_ask[0])})

            # Momentum 15m
            ohlcv = self.exchange._exchange.fetch_ohlcv(symbol, '15m', limit=2)
            if ohlcv and len(ohlcv) >= 2:
                ctx['momentum_15m'] = round((ohlcv[-1][4] - ohlcv[-2][1]) / ohlcv[-2][1] * 100, 3)

            # Funding
            ctx['funding'] = self.exchange.get_funding_rate(coin)

            # 1H ATR
            ohlcv_1h = self.exchange._exchange.fetch_ohlcv(symbol, '1h', limit=14)
            if ohlcv_1h and len(ohlcv_1h) >= 10:
                import numpy as np
                h = [c[2] for c in ohlcv_1h]
                l = [c[3] for c in ohlcv_1h]
                c = [c[4] for c in ohlcv_1h]
                trs = [max(h[i]-l[i], abs(h[i]-c[i-1]), abs(l[i]-c[i-1])) for i in range(1, len(ohlcv_1h))]
                price = c[-1]
                ctx['atr_1h'] = round(np.mean(trs[-14:]) / price, 5) if price > 0 else 0.01
        except Exception as e:
            logger.debug(f"Context capture {coin}: {e}")

        return ctx

    # === MISSED OPPORTUNITY TRACKING ===

    def fill_missed_opportunities(self) -> None:
        """Deep analysis of cancelled orders — what would have happened?
        For each: check if price reached entry, TP, SL in the hours after cancel.
        Run periodically (every 30 min)."""
        if not self.exchange:
            return

        conn = self._conn()
        # Use simple timestamp comparison (ISO format sorts correctly)
        from datetime import timedelta
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        rows = conn.execute("""
            SELECT trade_id, coin, direction, entry_price, sl_price, tp_price,
                   price_at_cancel, closed_at, entry_type
            FROM fortix_trades
            WHERE status='CANCELLED' AND would_have_hit_tp IS NULL
            AND closed_at < ?
        """, (cutoff,)).fetchall()

        if not rows:
            conn.close()
            return

        analyzed = 0
        for row in rows:
            trade_id, coin, direction, entry, sl, tp, cancel_price, closed_at, entry_type = row
            try:
                symbol = f"{coin}/USDT:USDT"
                # Get 1h candles after cancel (4 hours of data)
                ohlcv = self.exchange._exchange.fetch_ohlcv(symbol, '1h', limit=4)
                if not ohlcv or len(ohlcv) < 2:
                    continue

                # What did price do after we cancelled?
                max_high = max(c[2] for c in ohlcv)
                min_low = min(c[3] for c in ohlcv)

                hit_tp = 0
                hit_sl = 0

                if direction == 'LONG':
                    # Did price dip to our entry AND then reach TP?
                    price_reached_entry = min_low <= entry
                    price_reached_tp = max_high >= tp if tp else False
                    price_reached_sl = min_low <= sl if sl else False

                    if price_reached_entry and price_reached_tp:
                        hit_tp = 1
                    elif price_reached_entry and price_reached_sl:
                        hit_sl = 1
                    elif not price_reached_entry:
                        # Price never came to our level — we were right not to fill
                        # But what if we entered AGGRESSIVE at cancel_price?
                        if cancel_price and tp:
                            aggressive_tp = cancel_price + (tp - entry)  # same TP distance
                            aggressive_sl = cancel_price - (entry - sl) if sl else cancel_price * 0.997
                            if max_high >= aggressive_tp:
                                hit_tp = 2  # 2 = would have worked with AGGRESSIVE entry
                            elif min_low <= aggressive_sl:
                                hit_sl = 2
                else:  # SHORT
                    price_reached_entry = max_high >= entry
                    price_reached_tp = min_low <= tp if tp else False
                    price_reached_sl = max_high >= sl if sl else False

                    if price_reached_entry and price_reached_tp:
                        hit_tp = 1
                    elif price_reached_entry and price_reached_sl:
                        hit_sl = 1
                    elif not price_reached_entry:
                        if cancel_price and tp:
                            aggressive_tp = cancel_price - (entry - tp)
                            aggressive_sl = cancel_price + (sl - entry) if sl else cancel_price * 1.003
                            if min_low <= aggressive_tp:
                                hit_tp = 2
                            elif max_high >= aggressive_sl:
                                hit_sl = 2

                conn.execute("""
                    UPDATE fortix_trades SET would_have_hit_tp=?, would_have_hit_sl=?
                    WHERE trade_id=?
                """, (hit_tp, hit_sl, trade_id))
                analyzed += 1
            except Exception as e:
                logger.debug(f"Missed analysis {coin}: {e}")

        conn.commit()
        conn.close()
        if analyzed:
            logger.info(f"Missed opportunity analysis: {analyzed} orders checked")

    # === QUERY METHODS FOR OPUS ===

    def get_recent_results(self, n=10) -> str:
        """Last N closed trades as string for scan prompt, with entry context."""
        conn = self._conn()
        rows = conn.execute("""
            SELECT direction, coin, pnl_usd, exit_reason, held_minutes, entry_type, leverage,
                   regime, confidence, fill_ob_imbalance, fill_momentum_15m, fill_funding_rate
            FROM fortix_trades WHERE status='CLOSED'
            ORDER BY closed_at DESC LIMIT ?
        """, (n,)).fetchall()
        conn.close()

        if not rows:
            return "No completed trades yet."

        lines = []
        for row in rows:
            d, coin, pnl, reason, mins, etype, lev = row[:7]
            regime, conf, ob, mom, fund = row[7:12]
            pnl = pnl or 0
            mins = int(mins or 0)
            etype = etype or '?'
            ctx = []
            if regime: ctx.append(f"regime={regime}")
            if ob is not None: ctx.append(f"OB={ob:+.0%}")
            if mom is not None: ctx.append(f"mom={mom:+.1f}%")
            if fund is not None: ctx.append(f"fund={fund*100:+.3f}%")
            ctx_str = f" | {' '.join(ctx)}" if ctx else ""
            lines.append(f"  {d} {coin} {lev}x ({etype}): ${pnl:+.2f} [{reason}] {mins}min{ctx_str}")
        return "LAST TRADES:\n" + "\n".join(lines)

    def get_stats(self, days=7) -> str:
        """WR statistics by entry type, coin, exit reason."""
        conn = self._conn()
        cutoff = f"datetime('now', '-{days} days')"

        parts = []

        # By entry type
        rows = conn.execute(f"""
            SELECT entry_type,
                   SUM(CASE WHEN pnl_usd > 0 THEN 1 ELSE 0 END) as wins,
                   COUNT(*) as total,
                   AVG(pnl_usd) as avg_pnl
            FROM fortix_trades WHERE status='CLOSED' AND closed_at > {cutoff}
            GROUP BY entry_type
        """).fetchall()
        if rows:
            lines = []
            for etype, wins, total, avg in rows:
                wr = wins / total * 100 if total > 0 else 0
                lines.append(f"  {etype or '?'}: {wins}W/{total-wins}L ({wr:.0f}% WR) avg ${avg:.2f}")
            parts.append("BY ENTRY TYPE:\n" + "\n".join(lines))

        # By coin (top 5 best + worst)
        rows = conn.execute(f"""
            SELECT coin,
                   SUM(CASE WHEN pnl_usd > 0 THEN 1 ELSE 0 END) as wins,
                   COUNT(*) as total,
                   SUM(pnl_usd) as total_pnl
            FROM fortix_trades WHERE status='CLOSED' AND closed_at > {cutoff}
            GROUP BY coin ORDER BY total_pnl DESC
        """).fetchall()
        if rows:
            lines = []
            for coin, wins, total, pnl in rows:
                wr = wins / total * 100 if total > 0 else 0
                lines.append(f"  {coin}: {wins}W/{total-wins}L ({wr:.0f}%) ${pnl:+.2f}")
            parts.append("BY COIN:\n" + "\n".join(lines))

        # Missed opportunities (detailed)
        missed_patient = conn.execute(f"""
            SELECT COUNT(*) FROM fortix_trades
            WHERE status='CANCELLED' AND would_have_hit_tp=1 AND closed_at > {cutoff}
        """).fetchone()[0]
        missed_aggressive = conn.execute(f"""
            SELECT COUNT(*) FROM fortix_trades
            WHERE status='CANCELLED' AND would_have_hit_tp=2 AND closed_at > {cutoff}
        """).fetchone()[0]
        total_cancelled = conn.execute(f"""
            SELECT COUNT(*) FROM fortix_trades
            WHERE status='CANCELLED' AND would_have_hit_tp IS NOT NULL AND closed_at > {cutoff}
        """).fetchone()[0]
        if total_cancelled > 0:
            miss_lines = []
            if missed_patient:
                miss_lines.append(f"  {missed_patient} would have hit TP at PATIENT entry (price came to level)")
            if missed_aggressive:
                miss_lines.append(f"  {missed_aggressive} would have hit TP with AGGRESSIVE entry (price didn't come to level but moved in right direction)")
            safe = total_cancelled - missed_patient - missed_aggressive
            sl_hits = conn.execute(f"""
                SELECT COUNT(*) FROM fortix_trades
                WHERE status='CANCELLED' AND would_have_hit_sl>0 AND closed_at > {cutoff}
            """).fetchone()[0]
            if sl_hits:
                miss_lines.append(f"  {sl_hits} would have hit SL (good that we didn't enter)")
            miss_lines.append(f"  {safe} correctly skipped (no TP or SL hit)")
            parts.append(f"MISSED ORDERS ({total_cancelled} analyzed):\n" + "\n".join(miss_lines))

        conn.close()
        return "\n".join(parts) if parts else "Not enough data yet."

    def build_scan_feedback(self) -> str:
        """Complete feedback block for scan prompt."""
        recent = self.get_recent_results(10)
        stats = self.get_stats(7)
        if recent == "No completed trades yet." and stats == "Not enough data yet.":
            return ""
        return f"{recent}\n\n{stats}"

    # === FOR DAILY ANALYSIS ===

    def get_trades_for_analysis(self, hours=24) -> list:
        """Get detailed trade data for Opus daily self-analysis."""
        conn = self._conn()
        rows = conn.execute(f"""
            SELECT trade_id, coin, direction, status, entry_type,
                   entry_price, fill_price, sl_price, tp_price, exit_price,
                   regime, confidence, reason, leverage,
                   fill_ob_imbalance, fill_momentum_15m, fill_funding_rate, fill_atr_1h,
                   pnl_pct, pnl_usd, exit_reason, held_minutes,
                   price_at_cancel, would_have_hit_tp, would_have_hit_sl,
                   created_at, filled_at, closed_at
            FROM fortix_trades
            WHERE created_at > datetime('now', '-{hours} hours')
            ORDER BY created_at
        """).fetchall()
        conn.close()
        return rows
