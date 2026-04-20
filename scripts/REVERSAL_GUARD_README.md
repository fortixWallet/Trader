# Reversal Guard — Інструкція по впровадженню

## Що це
Кожні 15 хвилин перевіряє відкриті позиції — чи їхній напрямок ще актуальний.
Якщо сигнал розвернувся (LONG → SHORT або SHORT → LONG) — закриває позицію
замість чекати SL -10%.

## Тестування
```bash
# Один день
python3 scripts/backtest_reversal_guard.py 2026-04-20

# Діапазон
python3 scripts/backtest_reversal_guard.py 2026-04-18 2026-04-20

# Місяць
python3 scripts/backtest_reversal_guard.py 2026-03-01 2026-04-20
```

## Результат тесту (18-20 квітня)
- 5 reversal exits за 3 дні
- Середній ROI при exit: +3.0% (навіть в плюсі!)
- Зекономлено $2,253 порівняно з SL
- PnL improvement: +$411

## Впровадження в live

В файлі `src/crypto/trader_bybit.py`, в функції `_signal_monitor_loop()`,
додати **ПІСЛЯ** Step 0 (pre-scan sync) і **ПЕРЕД** Step 1 (refresh candles):

```python
# Step 0.5: Reversal Guard — close positions with reversed signal
from src.crypto.signal_scanner import scan_coin
_scan_conn = sqlite3.connect(str(DB_PATH), timeout=5)
for coin in list(self._tracked.keys()):
    tracked = self._tracked[coin]
    try:
        result = scan_coin(_scan_conn, coin)
        if result['signal'] == 'NEUTRAL':
            continue
        sig_dir = 'SHORT' if 'SHORT' in result['signal'] else 'LONG'
        if result['confidence'] < 0.75:
            continue
        # Check conflict
        conflict = (
            (tracked.direction == 'LONG' and sig_dir == 'SHORT') or
            (tracked.direction == 'SHORT' and sig_dir == 'LONG')
        )
        if conflict:
            close_side = 'sell' if tracked.direction == 'LONG' else 'buy'
            result = self.exchange.close_position(coin, close_side, tracked.size)
            if result:
                logger.info(f"REVERSAL EXIT: {tracked.direction} {coin} → signal now {sig_dir}, closed")
                self._notify(
                    f"🔄 REVERSAL: {coin}",
                    f"Was {tracked.direction}, signal now {sig_dir}\nClosed to minimize loss")
            del self._tracked[coin]
    except Exception as e:
        logger.debug(f"Reversal check {coin}: {e}")
try:
    _scan_conn.close()
except:
    pass
```

## Логіка
- LONG позиція + SHORT сигнал (conf ≥ 75%) → ЗАКРИТИ
- SHORT позиція + LONG сигнал (conf ≥ 75%) → ЗАКРИТИ
- Будь-яка позиція + NEUTRAL → ТРИМАТИ (відсутність сигналу ≠ розворот)
- Будь-яка позиція + той самий напрямок → ТРИМАТИ (підтверджено)

## Ризики
- False reversal: закриємо позицію яка б досягла TP
- Бектест показує це рідко (5 разів за 3 дні)
- Середній ROI при exit = +3.0% (зазвичай ще в плюсі)
