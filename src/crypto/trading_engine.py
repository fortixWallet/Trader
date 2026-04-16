"""
FORTIX Trading Engine — Automated Crypto Futures Trading
========================================================

Main daemon that connects all systems:
  Data Collection → Signal Generation → Confidence Tiers →
  Risk Management → Order Execution → Position Management

Runs continuously. Executes only Tier 1 and Tier 2 trades.
"""

import os
import sys
import time
import json
import signal
import sqlite3
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.crypto.exchange_client import ExchangeClient, create_client
from src.crypto.risk_manager import RiskManager, TradeParams
from src.crypto.confidence_tiers import scan_all_opportunities, TradeSuggestion
from src.crypto.position_exit_manager import PositionExitManager

logger = logging.getLogger('trading_engine')

_FACTORY_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = _FACTORY_DIR / 'data' / 'crypto' / 'market.db'
LOG_DIR = _FACTORY_DIR / 'logs'
LOG_DIR.mkdir(exist_ok=True)


class TradingEngine:
    """Main trading daemon."""

    # Schedule intervals (minutes)
    DATA_COLLECTION_INTERVAL = 10
    SIGNAL_SCAN_INTERVAL = 30
    POSITION_CHECK_INTERVAL = 5
    HEALTH_CHECK_INTERVAL = 1
    PNL_LOG_INTERVAL = 240  # 4 hours
    DAILY_IMPROVE_HOUR = 2  # 02:00 UTC

    def __init__(self, initial_capital: float = 500.0, dry_run: bool = False):
        self.initial_capital = initial_capital
        self.dry_run = dry_run
        self._running = False
        self._last_run = {}

        # Initialize components
        self.exchange = create_client()
        self.risk_manager = RiskManager(initial_capital)
        self.exit_manager = None  # initialized after exchange connects

        # Telegram notification
        self._tg_token = os.environ.get('TELEGRAM_BOT_TOKEN', '')
        self._tg_chat = os.environ.get('TELEGRAM_CHAT_ID', '')

        # Graceful shutdown
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def _shutdown(self, signum, frame):
        logger.info("Shutdown signal received")
        self._running = False

    def _notify(self, title: str, body: str):
        """Send Telegram notification."""
        if not self._tg_token or not self._tg_chat:
            return
        try:
            import requests
            text = f"<b>{title}</b>\n{body}"
            requests.post(
                f"https://api.telegram.org/bot{self._tg_token}/sendMessage",
                json={'chat_id': self._tg_chat, 'text': text, 'parse_mode': 'HTML'},
                timeout=10
            )
        except Exception:
            pass

    def _should_run(self, task: str, interval_min: int) -> bool:
        """Check if enough time has passed to run a task."""
        last = self._last_run.get(task, 0)
        if time.time() - last >= interval_min * 60:
            self._last_run[task] = time.time()
            return True
        return False

    def _write_heartbeat(self):
        """Write heartbeat for watchdog."""
        hb_path = _FACTORY_DIR / 'data' / 'crypto' / 'heartbeat.txt'
        hb_path.write_text(str(int(time.time())))

    # ── Data Collection ─────────────────────────────────────

    def collect_data(self):
        """Run data collection pipeline (main + Solana)."""
        try:
            from src.crypto.data_collector import collect_all
            result = collect_all(heartbeat_fn=self._write_heartbeat)
            logger.info(f"Data collected: {sum(result.values())} rows")
        except Exception as e:
            logger.error(f"Data collection failed: {e}")

        # Solana ecosystem data
        try:
            from src.crypto.solana_monitor import collect_all_solana
            sol_result = collect_all_solana()
            logger.info(f"Solana data: {sum(sol_result.values())} items")
        except Exception as e:
            logger.error(f"Solana collection failed: {e}")

    # ── Signal Processing ───────────────────────────────────

    def scan_and_trade(self):
        """FAST scan: V3 signals → regime → coin risk → TRADE.

        No heavy forecast_engine. No ML rebuild. Just proven signals.
        """
        logger.info("Fast scan: V3 signals...")

        try:
            from src.crypto.signal_system import SignalSystem
            ss = SignalSystem()
            scan = ss.scan_all()
            regime = scan.get('btc_regime', 'sideways')

            # PROVEN bearish signals only (70%+ accuracy, 100+ samples)
            PROVEN_BEARISH = {'compound_bearish', 'crowded_long', 'seller_dominance', 'post_pump'}

            # Build fast suggestions from V3 signals
            from src.crypto.confidence_tiers import TradeSuggestion
            from src.crypto.coin_risk_scorer import get_safe_leverage

            suggestions = []
            seen_coins = set()

            for sig in scan['signals']:
                if sig.coin in seen_coins:
                    continue

                # Only proven bearish signals
                if sig.signal_type in PROVEN_BEARISH and sig.direction == 'BEARISH':
                    # Regime check
                    bull_regimes = ('strong_bull', 'mild_bull')
                    is_contrarian = regime in bull_regimes  # bearish in bull = 85%

                    # Minimum strength
                    if sig.strength < 0.15 and not is_contrarian:
                        continue

                    # Confidence based on signal quality + regime
                    if sig.signal_type == 'compound_bearish':
                        tier = 1
                        confidence = 0.85
                    elif is_contrarian and sig.strength >= 0.30:
                        tier = 1
                        confidence = 0.80
                    elif sig.strength >= 0.50:
                        tier = 1
                        confidence = 0.75
                    else:
                        tier = 2
                        confidence = 0.65

                    confirmations = [f'v3_{sig.signal_type}']
                    if is_contrarian:
                        confirmations.append('regime_contrarian')

                    suggestions.append(TradeSuggestion(
                        coin=sig.coin,
                        direction='SHORT',
                        tier=tier,
                        confidence_score=confidence,
                        expected_accuracy=confidence,
                        confirmations=confirmations,
                        signal_strength=sig.strength,
                        regime=regime,
                        regime_alignment=is_contrarian,
                        entry_reason=f"{sig.signal_type} ({sig.strength:.2f}) in {regime}",
                        position_size_mult=1.0 if tier == 1 else 0.5,
                    ))
                    seen_coins.add(sig.coin)

            # Only strong_bull regime for LONG (validated at 76%)
            if regime == 'strong_bull':
                for sig in scan['signals']:
                    if sig.direction == 'BULLISH' and sig.coin not in seen_coins:
                        if sig.signal_type == 'compound_bullish' and sig.strength >= 0.4:
                            suggestions.append(TradeSuggestion(
                                coin=sig.coin, direction='LONG', tier=2,
                                confidence_score=0.65, expected_accuracy=0.65,
                                confirmations=[f'v3_{sig.signal_type}', 'regime_alignment'],
                                signal_strength=sig.strength, regime=regime,
                                regime_alignment=True,
                                entry_reason=f"{sig.signal_type} in strong_bull",
                                position_size_mult=0.5,
                            ))
                            seen_coins.add(sig.coin)

            # Sort by confidence
            suggestions.sort(key=lambda s: (-s.tier, -s.confidence_score))
            actionable = suggestions

        except Exception as e:
            logger.error(f"Fast scan failed: {e}")
            return

        if not actionable:
            logger.info("No signals meeting criteria")
            return

        logger.info(f"Found {len(actionable)} actionable signals")

        # Check how many positions we already have ON EXCHANGE
        if not self.dry_run:
            try:
                existing = self.exchange.get_positions()
                n_open = len(existing)
                if n_open >= self.risk_manager.MAX_CONCURRENT_POSITIONS:
                    logger.info(f"Max positions reached ({n_open}/{self.risk_manager.MAX_CONCURRENT_POSITIONS})")
                    return
                slots_available = self.risk_manager.MAX_CONCURRENT_POSITIONS - n_open
            except Exception:
                slots_available = 1
        else:
            slots_available = self.risk_manager.MAX_CONCURRENT_POSITIONS

        # Portfolio diversification: count REAL positions from exchange (not local state)
        existing_coins = set()
        existing_directions = {'long': 0, 'short': 0}
        existing_sectors = set()

        if not self.dry_run:
            try:
                real_positions = self.exchange.get_positions()
                for pos in real_positions:
                    coin = pos.symbol.split('/')[0].replace('1000', '')
                    existing_coins.add(coin)
                    existing_directions[pos.side] += 1
                logger.info(f"Real positions: {len(real_positions)} ({existing_directions})")
            except Exception as e:
                logger.warning(f"Could not check real positions: {e}")

        COIN_SECTORS = {
            'BTC': 'L1', 'ETH': 'L1', 'SOL': 'L1', 'BNB': 'L1', 'ADA': 'L1',
            'AVAX': 'L1', 'DOT': 'L1', 'XRP': 'L1',
            'UNI': 'DeFi', 'AAVE': 'DeFi', 'LDO': 'DeFi', 'CRV': 'DeFi', 'LINK': 'DeFi',
            'FET': 'AI', 'RENDER': 'AI', 'TAO': 'AI',
            'DOGE': 'Meme', 'SHIB': 'Meme', 'PEPE': 'Meme', 'WIF': 'Meme', 'BONK': 'Meme',
            'JUP': 'Solana', 'RAY': 'Solana', 'PYTH': 'Solana', 'JTO': 'Solana',
            'BOME': 'Solana', 'POPCAT': 'Solana', 'MEW': 'Solana', 'DRIFT': 'Solana',
        }

        if not self.dry_run:
            try:
                for pos in self.exchange.get_positions():
                    coin = pos.symbol.split('/')[0].replace('1000', '')
                    existing_coins.add(coin)
                    existing_directions[pos.side] += 1
                    existing_sectors.add(COIN_SECTORS.get(coin, 'Other'))
            except Exception:
                pass

        # Sort: Tier 1 first, then by confidence
        actionable.sort(key=lambda s: (s.tier, -s.confidence_score))

        trades_made = 0
        for suggestion in actionable:
            if trades_made >= slots_available:
                break

            coin = suggestion.coin
            direction_key = 'short' if suggestion.direction == 'SHORT' else 'long'
            sector = COIN_SECTORS.get(coin, 'Other')

            # Skip if already in this coin
            if coin in existing_coins:
                continue

            # Max 2 same direction (cross-pair hedging: force balance)
            if existing_directions[direction_key] >= self.risk_manager.MAX_SAME_DIRECTION:
                logger.info(f"Max {direction_key}s ({existing_directions[direction_key]}), skipping {coin}")
                continue

            # Max 1 per sector (diversification)
            if sector in existing_sectors and len(existing_coins) >= 2:
                logger.info(f"Already in {sector} sector, skipping {coin}")
                continue

            self._process_suggestion(suggestion)
            existing_coins.add(coin)
            existing_directions[direction_key] += 1
            existing_sectors.add(sector)
            trades_made += 1

    def _process_suggestion(self, suggestion: TradeSuggestion):
        """Process a single trade suggestion."""
        coin = suggestion.coin
        direction = suggestion.direction

        # Check position limit FIRST (from exchange, not local DB)
        if not self.dry_run:
            try:
                all_positions = self.exchange.get_positions()
                n_open = len(all_positions)
                if n_open >= self.risk_manager.MAX_CONCURRENT_POSITIONS:
                    logger.info(f"Max positions ({n_open}/{self.risk_manager.MAX_CONCURRENT_POSITIONS}), skipping {coin}")
                    return
            except Exception:
                pass

            # Skip if already have position in this coin
            existing = self.exchange.get_position(coin)
            if existing:
                logger.info(f"Already in {coin}, skipping")
                return

        # Get current price and ATR
        try:
            conn = sqlite3.connect(str(DB_PATH))
            conn.execute("PRAGMA journal_mode=WAL")

            # ATR from technical analyzer
            from src.crypto.technical_analyzer import analyze_coin
            tech = analyze_coin(conn, coin)
            current_price = tech.get('price', 0)
            atr_pct = tech.get('atr_pct', 0.03)

            conn.close()
        except Exception as e:
            logger.error(f"Failed to get price/ATR for {coin}: {e}")
            return

        if current_price <= 0:
            return

        # Calculate trade parameters
        params = self.risk_manager.calculate_trade_params(
            coin=coin,
            direction=direction,
            tier=suggestion.tier,
            current_price=current_price,
            atr_pct=atr_pct,
            position_size_mult=suggestion.position_size_mult,
        )

        if not params:
            logger.info(f"Risk manager rejected {coin} {direction}")
            return

        # Execute trade
        if self.dry_run:
            logger.info(f"[DRY RUN] Would {direction} {coin}: "
                       f"${params.position_size_usdt:.0f} @ ${current_price:.2f} "
                       f"({params.leverage}x) SL=${params.stop_loss_price:.2f} "
                       f"TP=${params.take_profit_1:.2f}")
            self._notify(
                f"🔔 DRY RUN: {direction} {coin}",
                f"Tier {suggestion.tier} | {params.leverage}x | "
                f"${params.position_size_usdt:.0f}\n"
                f"Entry: ${current_price:.2f}\n"
                f"SL: ${params.stop_loss_price:.2f}\n"
                f"TP: ${params.take_profit_1:.2f}\n"
                f"Confirms: {', '.join(suggestion.confirmations)}"
            )
            return

        self._execute_trade(coin, direction, params, suggestion)

    def _execute_trade(self, coin: str, direction: str, params: TradeParams,
                       suggestion: TradeSuggestion):
        """Execute a trade on the exchange."""
        try:
            # Set leverage (margin mode set via leverage params on MEXC)
            self.exchange.set_leverage(coin, params.leverage, direction)

            # Calculate amount in base currency
            ticker = self.exchange.get_ticker(coin)
            price = ticker['price']
            if price <= 0:
                return

            amount = params.position_size_usdt / price

            # Get contract info for rounding
            info = self.exchange.get_contract_info(coin)
            min_amount = info.get('min_amount', 0) or 1
            amount = max(int(amount), int(min_amount))

            if amount < min_amount:
                logger.warning(f"{coin}: amount {amount} below minimum {min_amount}")
                return

            # Coin risk scoring — adjust leverage per coin
            from src.crypto.coin_risk_scorer import get_safe_leverage, get_stop_multiplier
            safe_lev = get_safe_leverage(coin, direction)
            if params.leverage > safe_lev:
                logger.info(f"{coin}: leverage reduced {params.leverage}x → {safe_lev}x (risk scoring)")
                params.leverage = safe_lev
                # Recalculate position size with new leverage
                amount = int(params.position_size_usdt / price)
                margin_needed = amount * price / params.leverage

            # Adjust stop for volatile coins
            coin_stop_mult = get_stop_multiplier(coin)
            if coin_stop_mult != 1.5:
                atr_pct = tech.get('atr_pct', 0.03)
                if direction == 'LONG':
                    params.stop_loss_price = price * (1 - atr_pct * coin_stop_mult)
                    params.take_profit_1 = price * (1 + atr_pct * coin_stop_mult * 1.5)
                else:
                    params.stop_loss_price = price * (1 + atr_pct * coin_stop_mult)
                    params.take_profit_1 = price * (1 - atr_pct * coin_stop_mult * 1.5)

            # Verify we have enough free margin
            bal = self.exchange.get_balance()
            margin_needed = amount * price / params.leverage
            if margin_needed > bal['free'] * 0.9:
                logger.warning(f"{coin}: need ${margin_needed:.2f} margin, only ${bal['free']:.2f} free")
                return

            # Place main order
            side = 'buy' if direction == 'LONG' else 'sell'
            order = self.exchange.place_market_order(coin, side, amount)

            if not order:
                logger.error(f"Order failed for {coin}")
                return

            entry_price = order.price or price

            # EXCHANGE-SIDE STOP-LOSS (executes instantly on exchange, not in our code)
            sl_side = 'sell' if direction == 'LONG' else 'buy'
            sl_result = self.exchange.place_stop_loss(coin, sl_side, amount, params.stop_loss_price)
            if sl_result:
                logger.info(f"Exchange stop-loss set: {coin} @ ${params.stop_loss_price:.4f}")
            else:
                logger.warning(f"Failed to set exchange stop-loss for {coin}! Software stop only.")

            # Record trade
            trade_id = self.risk_manager.record_entry(
                params, entry_price,
                signal_type=suggestion.confirmations[0] if suggestion.confirmations else '',
                confidence=suggestion.confidence_score
            )

            # Notify
            self._notify(
                f"{'🟢' if direction=='LONG' else '🔴'} {direction} {coin} (Tier {suggestion.tier})",
                f"Entry: ${entry_price:.4f} | Size: {amount} contracts\n"
                f"Leverage: {params.leverage}x | Risk: ${params.max_loss_usdt:.2f}\n"
                f"SL: ${params.stop_loss_price:.4f} | TP: ${params.take_profit_1:.4f}\n"
                f"Confirms: {', '.join(suggestion.confirmations[:3])}"
            )

            logger.info(f"TRADE EXECUTED: {direction} {coin} #{trade_id} "
                        f"{amount} contracts @ ${entry_price:.4f} ({params.leverage}x)")

        except Exception as e:
            logger.error(f"Trade execution failed for {coin}: {e}")
            self._notify(f"Trade FAILED: {coin}", str(e)[:200])

    # ── Position Management ─────────────────────────────────

    def check_positions(self):
        """Check and manage open positions via exit manager."""
        if self.dry_run:
            return

        try:
            if not self.exit_manager:
                self.exit_manager = PositionExitManager(self.exchange)

            # Sync with exchange
            self.exit_manager.sync_positions()

            # Check all exit rules (trailing stop, TP, time, funding)
            actions = self.exit_manager.check_all_exits(notify_fn=self._notify)

            if actions:
                for a in actions:
                    logger.info(f"Exit action: {a}")

            # Check signal reversals
            try:
                from src.crypto.signal_system import SignalSystem
                ss = SignalSystem()
                scan = ss.scan_all()
                for coin in list(self.exit_manager.tracked.keys()):
                    if self.exit_manager.check_signal_reversal(coin, scan['signals']):
                        pos = self.exit_manager.tracked[coin]
                        logger.warning(f"Signal REVERSED for {coin} — closing")
                        close_side = 'buy' if pos.side == 'short' else 'sell'
                        self.exchange.close_position(coin, close_side, pos.size)
                        self._notify(f"🔄 REVERSAL {coin}",
                                    f"Signal reversed — closed {pos.side} position")
                        if coin in self.exit_manager.tracked:
                            del self.exit_manager.tracked[coin]
            except Exception as e:
                logger.debug(f"Signal reversal check failed: {e}")

        except Exception as e:
            logger.error(f"Position check failed: {e}")

    # ── Main Loop ────────────────────────────────────────────

    def run(self):
        """Main trading loop."""
        logger.info(f"FORTIX Trading Engine starting (capital=${self.initial_capital}, "
                    f"dry_run={self.dry_run})")

        # Connect to exchange
        if not self.dry_run:
            if not self.exchange.connect():
                logger.error("Failed to connect to exchange")
                return

            balance = self.exchange.get_balance()
            logger.info(f"Exchange balance: ${balance['total']:.2f}")

            # Initialize exit manager
            self.exit_manager = PositionExitManager(self.exchange)
            self.exit_manager.sync_positions()

            self._notify("Trading Engine Started",
                        f"Capital: ${self.initial_capital}\n"
                        f"Exchange balance: ${balance['total']:.2f}\n"
                        f"Mode: {'DRY RUN' if self.dry_run else 'LIVE'}\n"
                        f"Positions: {len(self.exit_manager.tracked)}")
        else:
            logger.info("DRY RUN mode — no real trades")
            self._notify("Trading Engine Started (DRY RUN)",
                        f"Capital: ${self.initial_capital}")

        self._running = True

        while self._running:
            try:
                self._write_heartbeat()

                # Check circuit breakers
                can_trade, reason = self.risk_manager.check_circuit_breakers()
                if not can_trade:
                    logger.warning(f"Trading paused: {reason}")
                    time.sleep(60)
                    continue

                # Data collection (every 10 min)
                if self._should_run('collect_data', self.DATA_COLLECTION_INTERVAL):
                    self.collect_data()

                # Signal scan and trade (every 30 min)
                if self._should_run('scan_trade', self.SIGNAL_SCAN_INTERVAL):
                    self.scan_and_trade()

                # Position management (every 5 min)
                if self._should_run('positions', self.POSITION_CHECK_INTERVAL):
                    self.check_positions()

                # Performance logging (every 4h)
                if self._should_run('pnl_log', self.PNL_LOG_INTERVAL):
                    perf = self.risk_manager.get_performance_summary()
                    if perf['n_trades'] > 0:
                        logger.info(f"Performance: {perf['n_trades']} trades, "
                                   f"PnL: ${perf['total_pnl']:+.2f}, "
                                   f"Win rate: {perf['win_rate']:.1f}%")

                # Post-trade learning (every 4h)
                if self._should_run('post_trade', self.PNL_LOG_INTERVAL):
                    try:
                        from src.crypto.post_trade_analyzer import run_post_trade_analysis
                        run_post_trade_analysis()
                    except Exception as e:
                        logger.debug(f"Post-trade analysis: {e}")

                # Gemini chart analysis for open positions (every 4h)
                if self._should_run('chart_analysis', self.PNL_LOG_INTERVAL):
                    if not self.dry_run and self.exit_manager and self.exit_manager.tracked:
                        try:
                            from src.crypto.chart_analyzer import analyze_chart
                            for coin in list(self.exit_manager.tracked.keys())[:3]:
                                result = analyze_chart(coin)
                                if result.get('has_data'):
                                    pos = self.exit_manager.tracked.get(coin)
                                    if pos:
                                        rec = result.get('recommendation', 'HOLD')
                                        if (pos.side == 'short' and rec == 'BUY') or \
                                           (pos.side == 'long' and rec == 'SELL'):
                                            logger.warning(f"Gemini DISAGREES: {coin} {pos.side}, "
                                                         f"chart says {rec} ({result.get('pattern','?')})")
                                            self._notify(f"⚠️ Chart disagrees: {coin}",
                                                       f"Position: {pos.side}\nChart: {rec}\n"
                                                       f"Pattern: {result.get('pattern','?')}")
                        except Exception as e:
                            logger.debug(f"Chart analysis: {e}")

                time.sleep(30)  # 30-second main loop

            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"Main loop error: {e}", exc_info=True)
                time.sleep(60)

        logger.info("Trading Engine stopped")
        self._notify("Trading Engine Stopped", "Graceful shutdown")


def main():
    """CLI entry point."""
    import argparse
    parser = argparse.ArgumentParser(description='FORTIX Trading Engine')
    parser.add_argument('--dry-run', action='store_true', help='Run without executing real trades')
    parser.add_argument('--capital', type=float, default=500.0, help='Initial capital ($)')
    parser.add_argument('--scan-once', action='store_true', help='Scan once and exit')
    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(str(LOG_DIR / 'trading_engine.log')),
        ]
    )

    # Load env
    env_path = _FACTORY_DIR / '.env'
    if env_path.exists():
        for line in open(env_path):
            if '=' in line and not line.startswith('#'):
                k, v = line.strip().split('=', 1)
                os.environ[k.strip()] = v.strip()

    engine = TradingEngine(initial_capital=args.capital, dry_run=args.dry_run)

    if args.scan_once:
        # Just scan and show opportunities
        suggestions = scan_all_opportunities()
        from src.crypto.confidence_tiers import format_opportunities
        print(format_opportunities(suggestions))

        # Show what risk manager would do
        rm = RiskManager(args.capital)
        print(f"\nCapital: ${rm.get_current_capital():.2f}")
        can, reason = rm.check_circuit_breakers()
        print(f"Circuit breakers: {reason}")
        return

    engine.run()


if __name__ == '__main__':
    main()
