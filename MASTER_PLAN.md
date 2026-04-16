# FORTIX MASTER PLAN — Ідеальна Торгова Машина

## Ціль: автоматизована система яка генерує $1,000-10,000/день

---

## ФАЗА 1: SHORT-TERM PATTERN ENGINE (Тиждень 1-2)

### 1.1 4h Pattern Matching Model

**Що будуємо:** ML модель яка передбачує напрямок ціни на наступні 4-12 годин з accuracy 58-65%.

**Дані для тренування:**
- 30+ монет × 2+ роки × 6 свічок/день = 130,000+ зразків
- Кожен зразок: вікно 20 свічок (4h) → що було через 4h/8h/12h
- Features (40+):
  - Price: 20 normalized returns, trend direction, trend strength
  - Volume: 20 normalized volumes, volume trend, volume anomaly (z-score)
  - RSI: значення, напрямок (зростає/падає), швидкість зміни
  - Bollinger: position, bandwidth (стискається = breakout скоро)
  - Momentum: ret_4h, ret_12h, ret_24h, ret_3d
  - Volatility: ATR, std of returns, expansion/compression
  - Funding rate: рівень + напрямок зміни
  - L/S ratio: рівень + напрямок
  - Taker buy/sell: хто домінує
  - Orderbook imbalance: bid/ask pressure
  - BTC correlation: moves with BTC or decoupling
  - Time features: hour_of_day (0-23), day_of_week, is_weekend
  - Market regime: bull/bear/sideways (від daily)
  - Cross-coin: чи інші монети вже рухнулись (sector leader)

**Модель:** LightGBM (швидкий, працює на CPU, handles NaN)
- 3 класи: UP (>0.5%), DOWN (<-0.5%), FLAT
- Walk-forward validation: train < Oct 2025, test >= Oct 2025
- Мінімальна accuracy для деплою: 58%

**Інтеграція:**
- Daily signal → НАПРЯМОК (SHORT/LONG)
- 4h model → ТАЙМИНГ (вхід/вихід)
- Результат: 5-15 trades per day замість 1 trade per week

**Бюджет:** $0 (CPU, існуючі дані)
**Час:** 2-3 дні

### 1.2 News Reaction System

**Що будуємо:** Система яка миттєво реагує на новини і торгує на їх основі.

**Компоненти:**

**A. Real-time News Feed:**
- Twitter/X API: моніторинг 50+ крипто інфлюенсерів в реальному часі
- CoinDesk + CoinTelegraph RSS (вже є)
- CryptoPanic API (потрібно виправити — 404 errors)
- Reddit r/cryptocurrency (для retail sentiment)

**B. News Impact Classifier (Claude API):**
- Кожна новина → Claude Haiku оцінює:
  - Impact score (1-10)
  - Direction (BULLISH/BEARISH/NEUTRAL)
  - Affected coins
  - Expected duration of impact (hours/days)
  - Speed of impact (instant/gradual)
- Вартість: ~$0.01-0.03 per news item, ~$5-10/day

**C. Historical News → Price Database:**
- Зібрати всі минулі новини з impact >= 7
- Записати що сталось з ціною після (1h, 4h, 24h, 7d)
- Побудувати патерни: "SEC announces X" → BTC -5% в перші 4h
- Використовувати для прогнозу: нова схожа новина → передбачити реакцію

**D. Breaking News Trading:**
- IF breaking news + impact >= 9 + direction clear:
  - ENTER позицію НЕГАЙНО (market order)
  - Leverage 10x (високий confidence)
  - Take profit 1-3% (швидкий scalp)
  - Hold 15-60 minutes max
- Потенціал: $50-200 per event (трапляється 1-3 рази/тиждень)

**Бюджет:** Claude API ~$5-10/day, Twitter API ~$15-30/month
**Час:** 3-4 дні

### 1.3 Quick Fixes для поточної системи

- Amount мінімум 1 contract (SOL, ETH, BTC)
- Ризик 7-10% для дорогих монет
- Лічильник shorts з біржі (не локальний)
- Telegram notification fix (order recovery)

**Бюджет:** $0
**Час:** кілька годин

---

## ФАЗА 2: VISUAL INTELLIGENCE (Тиждень 2-3)

### 2.1 Chart Pattern CNN

**Що будуємо:** Convolutional Neural Network яка "дивиться" на графік і прогнозує рух.

**Архітектура:**
```
Input: 224x224 PNG chart image (candlesticks + volume + indicators)
  ↓
ResNet-18 (pretrained on ImageNet, fine-tuned)
  ↓
Custom head: 3 neurons (UP / DOWN / FLAT)
  ↓
Output: probability distribution
```

**Training Data:**
- Генерувати chart images для кожного (coin, 4h_window) в історії
- 130,000+ images з labels
- Augmentation: різні zoom levels, з індикаторами і без
- Train/test split: temporal (not random)

**Де тренувати:**
- Google Colab Pro ($10/month) — GPU T4/A100
- Або: Lambda Labs cloud GPU ($0.50-1.50/hour)
- Або: vast.ai ($0.10-0.30/hour для RTX 3090)
- Тренування: ~4-8 годин GPU time
- Бюджет: $5-20

**Expected accuracy:** 58-65% (незалежний від числових features)

**Ensemble з числовою моделлю:**
- Numbers model: 60% accuracy
- Visual model: 60% accuracy
- Ensemble (якщо незалежні): ~68-72% accuracy
- Це ЗНАЧНО краще ніж кожна окремо

### 2.2 Gemini Vision Enhanced

**Що будуємо:** Використати Gemini 2.5 не просто для "BUY/SELL", а для детального аналізу:

**Prompt engineering:**
- Не просто "what pattern?" а:
  - "Where are the support/resistance levels?"
  - "Is volume confirming the move?"
  - "What's the risk/reward from current price?"
  - "How similar is this to [specific historical pattern]?"
- Batch аналіз кожні 4 години для топ-10 монет
- Вартість: ~$0.01 per image, $2-5/day

### 2.3 Multi-Timeframe Visual Analysis

- Weekly chart → macro trend
- Daily chart → swing direction
- 4h chart → entry/exit timing
- 1h chart → exact entry point
- Gemini аналізує ВСІ 4 timeframes для одної монети
- Consensus: якщо всі 4 кажуть DOWN → ДУЖЕ сильний сигнал

**Бюджет:** Gemini API ~$5/day, GPU ~$20 one-time training
**Час:** 5-7 днів

---

## ФАЗА 3: DEEP LEARNING ENGINE (Тиждень 3-4)

### 3.1 LSTM/Transformer Price Predictor

**Що будуємо:** Нейронна мережа яка вчиться на ПОСЛІДОВНОСТЯХ даних (не окремих точках).

**Архітектура:**
```
Input: sequence of 100 timesteps × 50 features
  ↓
Bidirectional LSTM (128 units) × 2 layers
  OR
Temporal Transformer (4 heads, 2 layers)
  ↓
Dense(64) → Dense(3) → Softmax
  ↓
Output: P(UP), P(DOWN), P(FLAT)
```

**Переваги над LightGBM:**
- Розуміє ПОСЛІДОВНОСТІ (патерн "head & shoulders" = послідовність рухів)
- Може вчити ДОВГОСТРОКОВІ залежності
- Обробляє multi-timeframe нативно

**Training:**
- 3+ роки даних × 30 монет
- GPU: 8-24 години тренування
- Hyperparameter tuning: Optuna (автоматичний пошук)
- Expected accuracy: 60-67%

### 3.2 Reinforcement Learning Agent

**Що будуємо:** Агент який вчиться ТОРГУВАТИ, а не просто прогнозувати.

**Різниця:**
- Supervised learning: "ціна піде вгору" → 60% правильно
- RL agent: "яку дію зробити щоб МАКСИМІЗУВАТИ прибуток?" → вчиться на P&L

**Архітектура:**
- State: поточний портфель + ринкові дані + відкриті позиції
- Actions: BUY/SELL/HOLD × position_size × leverage
- Reward: realized P&L - fees - funding
- Algorithm: PPO або SAC (state-of-the-art)

**Переваги:**
- Вчиться position sizing АВТОМАТИЧНО
- Вчиться коли НЕ торгувати
- Оптимізує не accuracy а ПРИБУТОК
- Враховує fees, slippage, funding

**Training:**
- Simulation environment з нашими даними
- GPU: 24-48 годин
- Expected improvement: +20-30% profit vs supervised

### 3.3 Ensemble of Everything

```
LightGBM (4h features)     → 60% accuracy
CNN (chart images)          → 60% accuracy  
LSTM (sequences)            → 63% accuracy
Gemini Vision               → 58% accuracy
Daily signals (V3)          → 70-85% accuracy
News classifier             → event-based
RL agent                    → position sizing
────────────────────────────────────────────
ENSEMBLE                    → 72-78% accuracy
                            + optimal position sizing
                            + news reaction
                            = MAXIMUM PROFIT
```

**Бюджет:** GPU ~$50-100, Claude API ~$50/month
**Час:** 2 тижні

---

## ФАЗА 4: SCALE TO $10K+/DAY (Місяць 2+)

### 4.1 Capital Scaling

| Capital | Risk/trade | Trades/day | Daily P&L | Monthly |
|---------|-----------|-----------|-----------|---------|
| $500 | $50 | 10 | $50-100 | $1,500-3,000 |
| $5,000 | $500 | 15 | $500-1,000 | $15,000-30,000 |
| $50,000 | $5,000 | 20 | $5,000-10,000 | $150,000-300,000 |
| $500,000 | $50,000 | 20 | $50,000-100,000 | $1.5M-3M |

### 4.2 Multi-Exchange

- MEXC (зараз)
- Binance (найбільша ліквідність)
- Bybit (швидке виконання)
- OKX (додатково)
- Cross-exchange arbitrage можливості

### 4.3 Copy Trading Platform

- Публікувати сигнали на MEXC copy trading
- 10-15% від прибутку копіювальників
- 1000 копіювальників × $1000 avg × 50% monthly × 10% = $500K/month passive

### 4.4 Telegram Signal Bot (платний)

- Підписка $50-200/month per user
- Автоматичні сигнали з нашої системи
- 500 підписників × $100 = $50K/month

### 4.5 Fund Management

- Коли track record 6+ місяців profitable
- Зібрати $1-5M від інвесторів
- 2% management fee + 20% performance fee
- $5M fund × 50% annual return × 20% = $500K/year performance fee

---

## ІНФРАСТРУКТУРА

### Сервери

**Зараз:** MacBook (CPU) — достатньо для Фази 1

**Фаза 2-3:**
- VPS з GPU: Hetzner ($50-100/month) або Lambda Labs
- Або: dedicated server з RTX 4090 ($200-300/month)
- Uptime 99.9% (не залежить від ноутбука)

**Фаза 4:**
- Colocation server в дата-центрі біржі (low latency)
- Redundant connections
- $500-1000/month

### APIs та підписки

| Service | Cost | Use |
|---------|------|-----|
| Claude API (Anthropic) | ~$50-100/month | News analysis, meta-analyst |
| Gemini API (Google) | ~$10-30/month | Chart analysis |
| MEXC API | Free | Trading |
| CoinGlass | $29/month | Derivatives data |
| CryptoQuant | ~$30/month | On-chain data |
| Twitter API | $15-30/month | Real-time news |
| Birdeye | Free tier | Solana data |
| Google Colab Pro | $10/month | GPU training |
| VPS (production) | $100-300/month | 24/7 uptime |
| **TOTAL** | **$275-550/month** | |

Окупність: при 10 trades/day × $10 avg profit = $300/day = покриває все за 2 дні.

### Моніторинг

- Grafana dashboard: P&L, positions, signals в реальному часі
- Telegram bot: всі сповіщення
- PagerDuty або similar: critical alerts (SMS, phone call)
- Weekly automated report: accuracy, P&L, drawdown, signal performance

---

## TIMELINE

```
ТИЖДЕНЬ 1 (зараз):
  День 1: 4h feature extraction + training dataset
  День 2: LightGBM training + walk-forward validation  
  День 3: Integration з trading engine
  День 4: Live testing (мін. позиції)
  День 5: Fix bugs, tune parameters
  День 6-7: Accumulate live results

ТИЖДЕНЬ 2:
  День 8: News reaction system
  День 9: Historical news → price impact DB
  День 10: Breaking news trading logic
  День 11-12: Gemini multi-timeframe analysis
  День 13-14: Chart image generation pipeline

ТИЖДЕНЬ 3:
  День 15-16: CNN training (Google Colab GPU)
  День 17-18: LSTM/Transformer training
  День 19-20: Ensemble integration
  День 21: Full system backtest

ТИЖДЕНЬ 4:
  День 22-23: RL agent training
  День 24-25: Optimization + parameter tuning
  День 26-27: Migrate to VPS (24/7 uptime)
  День 28: Copy trading setup
  День 29-30: Documentation + monitoring dashboard
```

---

## МЕТРИКИ УСПІХУ

| Метрика | Тиждень 1 | Тиждень 2 | Тиждень 4 | Місяць 3 |
|---------|-----------|-----------|-----------|----------|
| Accuracy | 58% | 63% | 70%+ | 75%+ |
| Trades/day | 5-10 | 10-15 | 15-25 | 20-30 |
| Daily P&L | $10-30 | $30-80 | $100-300 | $1,000+ |
| Drawdown max | 15% | 12% | 8% | 5% |
| Sharpe | 1.5 | 2.0 | 2.5 | 3.0+ |
| Capital | $125 | $500 | $5,000 | $50,000+ |

---

## ДИНАМІЧНЕ УПРАВЛІННЯ ПОЗИЦІЯМИ

### Зміна leverage в реальному часі

Позиція відкрита → ринок рухається → змінюємо параметри:

**Сценарій 1: Позиція в прибутку +5%**
- Збільшити leverage (3x → 5x) — прибуток захищений trailing stop
- Або: додати до позиції (averaging up) — збільшити розмір
- Risk: зміщується тільки на ПРИБУТОК, не на початковий капітал

**Сценарій 2: Позиція в збитку -3%**
- Зменшити leverage (7x → 3x) — менший ризик ліквідації
- Або: зменшити позицію на 50% — зафіксувати половину збитку
- НЕ: додавати до збиткової (averaging down = смерть)

**Сценарій 3: Волатильність різко зросла**
- Автоматично зменшити leverage
- Розширити stop-loss (щоб не вибило на шумі)
- Зменшити розмір позиції

**Сценарій 4: Сильна новина ПО нашій позиції**
- Якщо новина підтверджує наш напрямок → збільшити leverage, add to position
- Якщо новина проти нас → закрити 50-100% НЕГАЙНО

**Сценарій 5: Funding rate змінився**
- Якщо ми SHORT і funding став дуже негативний (ми платимо) → зменшити або закрити
- Якщо ми SHORT і funding позитивний (нам платять) → тримати довше

### Зміна монети (swap position)

Система виявляє що інша монета має КРАЩИЙ сигнал:
- Закриваємо поточну (наприклад BNB SHORT)
- Відкриваємо нову (наприклад SOL SHORT — сильніший сигнал)
- Swap займає 2 секунди
- Оптимізує капітал: завжди в НАЙКРАЩІЙ можливості

### Часткове закриття та re-entry

```
Позиція: SHORT SOL 10 contracts

Ціна впала 1.5%:
  → Close 5 contracts (take profit $4.20)
  → Keep 5 contracts (trailing stop)

Ціна відскочила 0.5%:
  → Re-enter 5 contracts (нова ціна, кращий entry)

Ціна впала ще 1%:
  → Close all (total profit: $7.50 замість $6.30 якщо тримати)
```

Результат: 3 mini-trades замість 1, кожен з профітом.

### Position Hedging

Маємо BNB SHORT → BNB раптово росте:
- Відкрити тимчасовий BNB LONG (хедж)
- Зачекати поки рух закінчиться
- Закрити хедж → SHORT продовжує працювати
- Втрати мінімальні замість stop-out

### Auto-scaling по результатах

| Win streak | Action | Reason |
|-----------|--------|--------|
| 3 wins | +20% position size | Momentum, система працює |
| 5 wins | +50% position size | Strong confidence |
| 2 losses | -30% position size | Щось змінилось |
| 3 losses | -50% + pause 4h | Переосмислити |
| 5 losses | STOP trading | Аналіз, retrain |

---

## РИЗИКИ ТА МІТИГАЦІЯ

| Ризик | Ймовірність | Вплив | Мітигація |
|-------|-------------|-------|-----------|
| Model overfit | Висока | Середній | Walk-forward validation, regularization |
| Exchange downtime | Низька | Високий | Multi-exchange, stop-loss on exchange |
| Black swan event | Низька | Критичний | Circuit breakers, max drawdown 15% |
| API costs exceed budget | Середня | Низький | Rate limiting, caching |
| Regulatory changes | Низька | Високий | Multi-jurisdiction, compliance |
| Competition (others copy) | Середня | Низький | Continuous improvement, speed advantage |

---

## ПЕРШІ КРОКИ (сьогодні)

1. Валідувати 4h pattern matching на історії
2. Якщо >55% → будувати trading logic
3. Fix current bugs (min amount, counter)
4. Збільшити ризик до 7-10% для SOL/ETH
5. Engine: check кожні 5 хв замість 30

Цей план — жива документація. Оновлюється щотижня на основі результатів.
