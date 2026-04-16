# PROFI — Trading Knowledge Base
## Повна база знань для Claude Trader Agent

---

## 1. PRICE ACTION

### Support / Resistance
- Support = рівень де ціна ЗУПИНЯЛАСЬ падати мінімум 2 рази
- Resistance = рівень де ціна ЗУПИНЯЛАСЬ рости мінімум 2 рази
- Коли support пробивається → стає resistance (і навпаки)
- Чим більше разів рівень тестувався → тим сильніший він
- Volume на пробитті підтверджує: високий volume = справжній пробій, низький = фейк

### Trend Lines
- Uptrend: з'єднай 2+ higher lows → лінія підтримки тренду
- Downtrend: з'єднай 2+ lower highs → лінія опору тренду
- Пробиття trend line = потенційний розворот
- Retest після пробиття = найкращий момент для входу

### Market Structure
- Higher Highs + Higher Lows = UPTREND → тільки LONG
- Lower Highs + Lower Lows = DOWNTREND → тільки SHORT
- Break of Structure (BOS) = перший lower low в uptrend = розворот
- Change of Character (CHoCH) = aggressive BOS з volume

---

## 2. CHART PATTERNS

### Reversal Patterns (розвороти)
**Head & Shoulders (H&S):**
- Три піки: лівий (менший), голова (найбільший), правий (менший)
- Neckline = лінія через два мінімуми між піками
- Пробиття neckline = SHORT
- Target = відстань від голови до neckline, відкладена вниз
- Volume: зменшується від лівого до правого плеча

**Double Top / Double Bottom:**
- Два піки/дна на одному рівні
- Пробиття рівня між ними = сигнал
- Target = висота формації

**Triple Top / Triple Bottom:**
- Три торкання одного рівня → дуже сильний сигнал

### Continuation Patterns (продовження)
**Bull/Bear Flag:**
- Сильний рух (pole) → коротка консолідація (flag) → продовження
- Flag нахилений ПРОТИ тренду
- Пробиття flag = entry

**Triangle (ascending, descending, symmetrical):**
- Ascending: горизонтальний resistance + rising support → BULLISH
- Descending: горизонтальний support + falling resistance → BEARISH  
- Symmetrical: converging lines → breakout в напрямку тренду

**Wedge (rising, falling):**
- Rising wedge = BEARISH (навіть в uptrend)
- Falling wedge = BULLISH (навіть в downtrend)

---

## 3. CANDLESTICK PATTERNS

### Bullish (single candle)
- **Hammer**: маленьке тіло зверху, довга нижня тінь (2x+ тіла). Після downtrend = розворот
- **Inverted Hammer**: маленьке тіло знизу, довга верхня тінь. Після downtrend = потенційний розворот
- **Dragonfly Doji**: open=close зверху, довга нижня тінь. Сильний bullish signal

### Bearish (single candle)
- **Shooting Star**: маленьке тіло знизу, довга верхня тінь. Після uptrend = розворот
- **Hanging Man**: як hammer, але ПІСЛЯ uptrend = bearish
- **Gravestone Doji**: open=close знизу, довга верхня тінь

### Multi-candle Patterns
- **Engulfing**: друга свічка повністю поглинає першу. Bullish або bearish
- **Morning Star**: down candle → doji → up candle = bullish reversal
- **Evening Star**: up candle → doji → down candle = bearish reversal
- **Three White Soldiers**: три послідовні великі зелені = strong bullish
- **Three Black Crows**: три великі червоні = strong bearish

---

## 4. VOLUME ANALYSIS

- **Volume confirms trend**: рух з volume = справжній, без volume = фейковий
- **Volume divergence**: ціна росте але volume падає = тренд слабшає
- **Volume climax**: екстремальний volume на піку/дні = можливий розворот
- **Volume breakout**: пробиття рівня з 2x+ avg volume = справжній пробій
- **Dry volume**: дуже низький volume = ринок чекає → може бути різкий рух

---

## 5. INDICATORS

### RSI (Relative Strength Index)
- >70 = overbought (може розвернутись вниз)
- <30 = oversold (може розвернутись вгору)
- **Divergence** = найсильніший сигнал: ціна робить новий high, RSI НЕ робить → розворот
- Hidden divergence: ціна higher low, RSI lower low → trend continuation

### Bollinger Bands
- Price above upper band = overbought
- Price below lower band = oversold
- **Squeeze** (bands стискаються) = volatility drop → очікуй breakout
- Walk the bands = strong trend (ціна тримається на band)

### MACD
- MACD crosses signal line UP = bullish
- MACD crosses signal line DOWN = bearish
- Histogram growing = momentum increasing
- **Divergence** з ціною = сильний reversal signal

### Funding Rate (крипто-специфічний)
- Positive funding = longs платять shorts → ринок overleveraged long → ризик dump
- Negative funding = shorts платять longs → ринок overleveraged short → ризик squeeze
- Extreme funding (>0.1%) = висока ймовірність розвороту

---

## 6. MULTI-TIMEFRAME ANALYSIS

### Framework:
1. **Weekly**: визначає ТРЕНД (up/down/sideways) — НЕ торгуємо проти weekly trend
2. **Daily**: визначає НАПРЯМОК — де ми в тренді (початок, середина, кінець)
3. **4h**: визначає ENTRY — конкретний момент входу
4. **1h**: визначає ТОЧКУ — оптимальна ціна входу

### Правило: ВИЩИЙ timeframe ЗАВЖДИ головніший
- Weekly says DOWN + Daily says UP = НЕ ТОРГУЄМО LONG (daily проти weekly)
- Weekly says UP + Daily says UP + 4h says pullback = LONG entry opportunity

---

## 7. CRYPTO-SPECIFIC

### Liquidation Cascades
- Великі ліквідації ПРИСКОРЮЮТЬ рух (каскад)
- Ціна тягнеться до зон з великими ліквідаціями ("liquidity magnet")
- Після каскаду = часто різкий reversal (всі слабкі руки вибиті)

### Whale Activity
- Великий переказ на біржу = потенційний продаж
- Великий переказ З біржі = потенційне накопичення
- Whale orders в orderbook = підтримка/опір

### DeFi / On-Chain
- TVL зростає = bullish для DeFi токенів
- Exchange outflows = accumulation (bullish)
- Exchange inflows = distribution (bearish)
- Stablecoin mint = new money entering crypto (bullish)

### Market Cycles
- Accumulation → Markup → Distribution → Markdown
- Fear & Greed extreme (<20 або >80) = потенційний розворот
- Bitcoin Halving cycle: ~6 months after halving = bull run

---

## 8. RISK MANAGEMENT

### Position Sizing
- MAX 10% капіталу на позицію
- MAX 3-4 позиції одночасно
- Correlation risk: не більше 2 монет з одного сектору

### Risk/Reward
- МІНІМУМ 1:1 (TP = SL)
- Ідеально 1.5:1 або 2:1
- НІКОЛИ не торгувати якщо R:R < 1:1

### Entry Rules
- ЗАВЖДИ чекай підтвердження (не ловити падаючий ніж)
- Entry на retest = найкращий R:R
- Не входити на top/bottom — входити на pullback

### Exit Rules
- TP на рівнях support/resistance
- Trailing stop після 50%+ від TP
- НІКОЛИ не переміщувати SL далі від entry (тільки ближче)

---

## 9. КОЛИ НЕ ТОРГУВАТИ

- Свята (Різдво, Пасха, Китайський Новий рік) — низька ліквідність
- Перед великими новинами (FOMC, CPI, NFP) — непередбачуваність
- Коли Fear & Greed = 45-55 (нерішучість ринку)
- Коли weekly і daily дають протилежні сигнали
- Коли volume нижче 50% від середнього
- Коли більше 3 стопів поспіль — стоп на день (можливо ринок змінився)

---

## 10. PER-COIN PROFILES

### BTC — Лідер ринку
- Рухається першим, альти слідують
- Менш волатильний ніж альти (ATR ~1.5%/day)
- Домінує в bear market (dominance росте)
- НЕ ТОРГУЄМО BTC — він наш індикатор

### ETH — Другий лідер
- Слідує за BTC з кореляцією 0.9
- ETH/BTC ratio = індикатор alt season
- ATR ~2%/day

### SOL, AVAX, ADA, DOT — L1 (Layer 1)
- Високо корелюють між собою
- Не більше 1 L1 в портфелі одночасно
- ATR 2-4%/day

### AAVE, UNI, LDO, CRV, PENDLE — DeFi
- Чутливі до TVL changes і hack news
- Не більше 1 DeFi одночасно
- ATR 3-5%/day

### DOGE, WIF — Meme
- Надвисока волатильність (ATR 4-8%)
- Driven by social media, не fundamentals
- Швидкий вхід/вихід, не тримати довго

### TAO, RENDER, FET — AI/Infra
- Narrative-driven (AI hype)
- Висока волатильність коли narrative сильний
- ATR 3-6%/day

---

## 11. ADVANCED TRADING STRATEGIES

### Breakout Trading
- Ціна в range (consolidation) → volume spike → breakout above/below range
- ВХІД: на retest рівня після пробиття (не на самому пробитті!)
- Фейковий breakout (fake-out): ціна пробиває рівень але повертається → trap
- Як відрізнити: volume. Справжній breakout = 2x+ avg volume. Fake = normal volume
- Breakout після squeeze (BB width мінімальний) = найсильніший рух

### Trend Continuation
- Pullback в тренді = найкращий вхід
- Fibonacci retracement: 38.2%, 50%, 61.8% — шукай реакцію на цих рівнях
- Pullback до EMA20 в strong trend = entry signal
- Pullback до EMA50 в medium trend = entry signal
- Pullback НИЖЧЕ EMA50 = можливо тренд зламався

### Mean Reversion
- Ціна відхилилась >2 стандартних відхилення від середнього → повернеться
- RSI extreme (<20 або >80) + volume climax = mean reversion setup
- НЕ торгуй mean reversion ПРОТИ strong trend (only in ranges)
- Time-based: mean reversion працює краще на вищих timeframes (daily, weekly)

### Scalping (short-term)
- Entry: momentum shift на 1h/15m
- Hold: 15-60 minutes
- TP: 0.3-0.7% (smaller but frequent)
- Key: spread and fees повинні бути < 0.1% загальних
- Працює тільки на ДУЖЕ ліквідних парах (BTC, ETH, SOL)

### Swing Trading (medium-term)
- Entry: daily support/resistance + 4h confirmation
- Hold: 1-7 days
- TP: 3-10%
- Requires: clear trend on weekly + daily setup
- Best R:R ratio possible (3:1 or more)

---

## 12. ORDER FLOW AND MARKET MICROSTRUCTURE

### Order Book Analysis
- Bid wall (великий buy order) = support (але може бути fake — "spoofing")
- Ask wall = resistance (може бути fake)
- Thin orderbook = volatile (малий ордер рухає ціну)
- Imbalance: bid > ask = покупці домінують = ціна росте

### Liquidation Mechanics
- Leveraged traders ліквідуються при русі проти них
- Cascading liquidations: одна ліквідація → ціна рухається → наступна → каскад
- Liquidation heatmap: де стоять стопи → ціна "магнітом" тягнеться до цих зон
- Після великого каскаду = часто різкий reversal (overcorrection)

### Market Maker Behavior
- Market makers provide liquidity (buy on bid, sell on ask)
- Вони ЗАВЖДИ заробляють на spread
- Коли MM знімає liquidity (pulls orders) = ринок стає volatile
- "Stop hunt": ціна швидко рухається до зони де стоять стопи → збирає їх → повертається

---

## 13. MACRO AND FUNDAMENTAL ANALYSIS

### Bitcoin Dominance
- BTC.D росте = гроші тікають з альтів в BTC (risk-off) = BEARISH для альтів
- BTC.D падає = "alt season" = гроші переливаються в альти = BULLISH для альтів
- BTC.D flat + BTC UP = bull market ранній stage
- BTC.D DOWN + BTC UP = alt season (найприбутковіший період для альтів)

### Correlation with Traditional Markets
- BTC корелює з NASDAQ (~0.7 correlation)
- Сильний USD (DXY росте) = BEARISH для крипто
- Слабкий USD = BULLISH
- Процентні ставки FED: підвищення = bearish, зниження = bullish
- Рецесія = спочатку bearish, потім QE = дуже bullish

### On-Chain Fundamentals
- MVRV ratio >3.7 = overvalued = BEARISH → sell
- MVRV ratio <1.0 = undervalued = BULLISH → buy
- NUPL >0.75 = euphoria = sell
- NUPL <0 = capitulation = buy (historically best entry)
- SOPR <1 = holders selling at loss = near bottom
- Exchange netflow positive = selling pressure
- Exchange netflow negative = accumulation

### Stablecoin Flows
- Stablecoins minted + flowing to exchanges = new buying power = BULLISH
- Stablecoins burned + flowing off exchanges = money leaving crypto = BEARISH

---

## 14. PSYCHOLOGY AND DISCIPLINE

### Emotional Traps
- FOMO (Fear Of Missing Out): "ціна вже +20% я маю купити!" → НІ, чекай pullback
- Revenge trading: після збитку хочеться "відіграти" → НІ, стоп на день
- Overconfidence: 5 виграшів поспіль → "я геній" → збільшив розмір → великий збиток
- Hope trading: позиція в мінусі → "може повернеться" → НІ, тримайся SL

### Rules That Save Money
1. ЗАВЖДИ ставити SL ПЕРЕД entry (план виходу до входу)
2. НІКОЛИ не пересувати SL далі від entry
3. Після 3 стопів поспіль — СТОП торгівля на 4-8 годин
4. Не торгувати в перші 30 хвилин після великої новини (хаос)
5. Не торгувати коли "відчуваєш" (торгуй по системі, не емоціями)
6. Записувати КОЖЕН трейд: вхід, вихід, причина, результат, що б зробив інакше

---

## 15. CRYPTO-SPECIFIC ADVANCED

### Token Unlock Events
- Великий unlock (>5% supply) = bearish pressure на токен
- Торгуй SHORT ЗА ТИЖДЕНЬ до unlock (anticipation)
- Після unlock ціна часто відновлюється (buy the news після sell the rumor)

### DeFi Protocol Risks
- TVL dropping = users leaving = bearish для governance token
- Hack/exploit = IMMEDIATE sell (часто -50%+ за годину)
- New major integration = bullish (Aave on new chain, etc)

### Meme Coin Dynamics
- Social media driven (Twitter/X, Reddit)
- Pump and dump cycles: 3-7 days pump → crash
- НІКОЛИ не тримати meme coins > 24h
- Volume is KEY: якщо volume falling = dump imminent

### Altcoin Rotation
- Early bull: BTC rallies first → ETH follows → top altcoins → mid caps → memes
- Late bull: everything pumps indiscriminately (sign of top)
- Bear: reverse order — memes die first → mid caps → altcoins → ETH → BTC last standing

---

## 16. НАШІ УРОКИ (з реальної торгівлі)

### Що працювало:
- Market breadth як primary regime indicator (не тільки BTC)
- Score threshold: не торгувати коли |score| < 0.002
- Adaptive TP per coin (volatile = вищий TP)
- Sector diversification (не 3 DeFi одночасно)
- WebSocket tick-by-tick для виходів

### Що НЕ працювало:
- Circuit breaker -3% → закривав позиції які потім відновлювались (-$1,178)
- Breakeven на +0.4% → обрізав прибуток до $40 але loss залишався $631
- Фіксований TP для всіх монет → не враховує волатильність
- Торгівля в FLAT режимі → рандомний результат
- Market orders замість limit → 0.06% зайвих витрат на кожен трейд

### Характеристики монет (з бектесту):
- TAO: висока волатильність, часто хибні сигнали, працює тільки з strong trend
- CRV: стабільніший, хороший для SHORT в bear market
- AAVE: 77% WR але великі збитки коли помиляється
- DOT: повільний, часто не досягає TP
- POL: середня волатильність, непередбачуваний
- WIF: meme = хаос, тільки з very high confidence

### Ключове спостереження:
11 квітня 2026 (Пасха) — ринок хаотичний, кожні 4h розвертався на 180°.
Volume на 45% нижче норми. BTC коливався ±1% але альти падали.
ВИСНОВОК: святкові дні = НЕ ТОРГУВАТИ (низька ліквідність = непередбачуваність)

---

## 17. ТОРГОВІ РЕЖИМИ — ПОВНИЙ ГАЙД

### Філософія: Ринок → Режим → Стратегія
Ринок ЗАВЖДИ в одному з режимів. Завдання — визначити поточний і адаптувати стратегію.
НІКОЛИ не нав'язувати ринку свою думку. Ринок правий завжди.

### Режим 1: STRONG TREND (Тренд)
**Ідентифікація:**
- ADX > 30 (сильний тренд)
- EMA20 > EMA50 > EMA100 (bull) або навпаки (bear)
- Price consistently above/below EMA20
- Higher highs + higher lows (bull) або lower highs + lower lows (bear)
- Volume підтверджує напрямок (зростає з трендом)

**Стратегія: TREND FOLLOWING**
- Вхід: pullback до EMA20 або EMA50
- SL: нижче попереднього swing low (bull) / вище swing high (bear)
- TP: розширення тренду або наступний рівень S/R
- Trailing stop: 1.5 ATR від поточної ціни
- Position size: МАКСИМАЛЬНИЙ (тренд = найвища ймовірність)
- НІКОЛИ не торгуй проти тренду ("trend is your friend")
- Фібоначчі 38.2% і 50% = найкращі рівні входу при pullback

**Підводні камені:**
- Тренд в кінцевій фазі: divergence на RSI, volume falling = тренд вмирає
- "Extended trend": ціна далеко від EMA50 = pullback неминучий, не входити
- Parabolic move: вертикальний ріст = blow-off top наближається

### Режим 2: RANGE / SIDEWAYS (Бокова торгівля)
**Ідентифікація:**
- ADX < 20 (тренд відсутній)
- Ціна bounce між чіткими support і resistance
- EMA20 і EMA50 переплетені (flat)
- Volume нижче середнього
- RSI осцилює 40-60

**Стратегія: MEAN REVERSION / RANGE TRADING**
- LONG на support з SL під support
- SHORT на resistance з SL над resistance
- TP = протилежний край range
- Position size: СЕРЕДНІЙ (ризик false breakout)
- Bollinger Bands: buy lower band, sell upper band
- RSI: buy <35, sell >65

**Підводні камені:**
- Range ЗАВЖДИ закінчується breakout — будь готовий вийти
- Чим довше range → тим сильніший breakout буде
- "Tightening range" (converging S/R) = breakout ось-ось

### Режим 3: BREAKOUT (Пробій)
**Ідентифікація:**
- Ціна в tight range (BB squeeze, declining ATR)
- Volume dry up → потім spike
- Чіткий рівень який ціна тестувала 2-3+ рази
- Converging trendlines (triangle)

**Стратегія: BREAKOUT TRADING**
- Чекай закриття свічки ЗА рівнем (не тінь, а тіло!)
- Volume на breakout повинен бути 2x+ середнього
- НАЙКРАЩИЙ entry: retest рівня після пробиття
- SL: за протилежний край range або всередину range
- TP: розмір range відкладений в бік пробиття
- Position size: СЕРЕДНІЙ-ВЕЛИКИЙ (після підтвердження volume)

**Fake Breakout (фейковий пробій):**
- Ціна пробиває рівень але повертається в range
- Ознаки: низький volume на пробитті, дуже довга тінь свічки
- Після fake breakout часто рухається в ПРОТИЛЕЖНОМУ напрямку (trap)
- Як захиститись: не входити на першій свічці — чекай retest або другу свічку підтвердження

### Режим 4: HIGH VOLATILITY (Висока волатильність)
**Ідентифікація:**
- ATR > 2x середнього
- BB дуже розширені
- Великі свічки (body > 2x normal)
- Gaps in orderbook
- Liquidation cascades active
- Fear & Greed в extreme (<15 або >85)

**Стратегія: REDUCED SIZE + MOMENTUM**
- Position size: МІНІМАЛЬНИЙ (50% від нормального)
- Wider stops (2x ATR замість 1.5x)
- TP в 2 етапи: 50% на 1x ATR, 50% trail
- Торгуй ТІЛЬКИ в напрямку momentum (не counter-trend!)
- Після liquidation cascade = найкращий mean reversion setup
- WAIT якщо напрямок незрозумілий

**Підводні камені:**
- Slippage значно більший (orderbook thin)
- Stop-loss може прослизнути (gap through stop)
- Емоції на максимумі — найчастіша причина великих збитків

### Режим 5: LOW VOLATILITY (Низька волатильність)
**Ідентифікація:**
- ATR < 50% від середнього
- BB дуже вузькі (squeeze)
- Маленькі свічки, doji, spinning top
- Volume нижче 50% від average
- Ринок "спить" (вихідні, свята, перед новинами)

**Стратегія: WAIT або ТІСНІ RANGE-TRADES**
- Переважно WAIT (малий R:R при малих рухах)
- Якщо торгуєш: дуже tight SL/TP, зменшений position size
- Готуйся до breakout — постав алерти на ключових рівнях
- BB squeeze = пружина: чим довше стискається → тим сильніший breakout

### Режим 6: CAPITULATION (Капітуляція / Паніка)
**Ідентифікація:**
- Extreme sell-off (-10%+ за день на BTC, -20%+ на альтах)
- Volume 5x+ середнього
- Fear & Greed < 10
- Funding strongly negative
- Масові ліквідації (>$1B за добу)
- Кожна година = новий мінімум

**Стратегія: WAIT → COUNTER-TREND (обережно)**
- Перші години капітуляції: ТІЛЬКИ WAIT (не ловити ніж)
- Чекай ознаки виснаження: volume dry up, doji/hammer candle, RSI divergence
- Вхід: після ПЕРШОГО зеленого 4h закриття з volume
- SL: під мінімум капітуляції
- TP: 50% FIB від усього падіння
- Position size: НЕВЕЛИКИЙ (висока невизначеність)
- Ніколи не full position на розвороті — додавай якщо підтверджується

### Режим 7: EUPHORIA (Ейфорія)
**Ідентифікація:**
- Extreme rally (+15%+ на BTC за тиждень)
- Fear & Greed > 90
- Funding strongly positive (>0.1%)
- "Everyone is a genius" — social media гудить
- Meme coins pump 100%+
- Low-quality projects pump

**Стратегія: TAKE PROFIT → SHORT PREPARATION**
- Фіксуй прибуток на LONG позиціях
- НЕ відкривай нові LONG (запізно)
- Готуй SHORT: шукай divergence, overbought signals
- WAIT на SHORT поки немає підтвердження розвороту (CHoCH)
- Коли перша red daily candle з великим volume → SHORT setup
- Position size: НЕВЕЛИКИЙ → збільшуй з підтвердженням

---

## 18. ADAPTIVE ENTRY TECHNIQUES

### Техніка 1: Momentum Entry
- Коли: strong trend або breakout
- Як: entry на close свічки яка підтверджує momentum (великий body, volume 1.5x+)
- SL: за low/high цієї свічки
- Pros: швидкий entry, не пропускаєш рух
- Cons: може бути late entry (заплатиш за підтвердження)

### Техніка 2: Pullback Entry
- Коли: uptrend або downtrend (не range!)
- Як: чекаєш pullback до EMA20/50 або Fib 38.2-61.8%
- Entry: коли pullback зупиняється + reversal candle (hammer, engulfing)
- SL: за swing low/high pullback
- Pros: найкращий R:R, найменший ризик
- Cons: можеш пропустити якщо pullback не прийде

### Техніка 3: Retest Entry
- Коли: після breakout рівня
- Як: ціна пробиває рівень → повертається до нього → відскакує
- Entry: на відскоку від рівня (колишній resistance тепер support)
- SL: за рівнем (якщо пробив назад → breakout був fake)
- Pros: найменший ризик, найбільше підтвердження
- Cons: часто ціна не повертається для retest (пропускаєш)

### Техніка 4: Limit Entry (Queue)
- Коли: знаєш точний рівень де хочеш входити
- Як: ставиш limit order на рівні заздалегідь
- SL: одразу з ордером
- Pros: найкраща ціна, maker fee замість taker
- Cons: може не активуватись; може активуватись і продовжити проти тебе

### Техніка 5: Scaled Entry (DCA вхід)
- Коли: не впевнений в точному рівні, але впевнений в напрямку
- Як: розбий позицію на 2-3 частини, входь на різних рівнях
- Наприклад: 40% на поточній ціні, 30% на -1%, 30% на -2%
- SL: єдиний для всієї позиції
- Pros: краща середня ціна, менший ризик
- Cons: складніше управління, можливо не всі entry активуються

---

## 19. ADVANCED EXIT STRATEGIES

### Exit 1: Fixed TP/SL
- TP і SL визначені при вході
- TP = f(ATR): для volatile монет ATR*2.0, для stable ATR*1.5
- SL = ATR*1.5 (стандарт)
- R:R мінімум 1:1, ідеально 1.5:1

### Exit 2: Partial Take Profit
- 50% позиції закрити на TP1 (1.5 ATR)
- Решту trail зі стопом 1 ATR від peak
- Фіксуєш прибуток + даєш решті "бігти"
- НАЙКРАЩА стратегія для trending markets

### Exit 3: Time-Based Exit
- Якщо позиція не рухається протягом N годин → закрити
- Стандарт: 6-12 годин для 4h trades, 1-2 години для 1h trades
- "Stagnant position" = opportunity cost + funding rate витрати
- Якщо ціна на breakeven після 8 годин → закрити (не чекати)

### Exit 4: Signal-Based Exit
- Закрити коли з'являється протилежний сигнал
- RSI divergence проти позиції → закрити
- Volume climax → закрити (можливий розворот)
- Зміна режиму (ADX різко впав, range → trend) → переоцінити

### Exit 5: Trailing Stop
- Після досягнення TP1: trail на 1 ATR від highest/lowest price
- Dynamic: trail звужується по мірі росту прибутку
- Починаємо trail після +1.5 ATR від entry
- НІКОЛИ не розширюємо trail (тільки звужуємо)

### Exit 6: Emergency Exit
- News impact score >= 8 і ПРОТИ позиції → НЕГАЙНО закрити
- Liquidation cascade починається → закрити якщо в протилежному напрямку
- Exchange issue (API error, withdraw halt) → закрити все

---

## 20. CROSS-MARKET ANALYSIS

### Кореляції між монетами
- BTC → ETH → Top alts → Mid caps → Memes (waterfall effect)
- Коли BTC рухається різко, альти ЗАВЖДИ слідують з lag 10-60 хвилин
- Але: якщо alt НЕ слідує за BTC = може бути coin-specific фактор (news, unlock)
- УВАГА: не торгуй 3 монети які рухаються ідентично (3x ризик на 1 idea)

### Sector Rotation
- Capital переливається між секторами: L1 → DeFi → Gaming → Meme → AI → назад
- Sector leading = перший виросте, перший впаде
- Sector lagging = можливість для entry якщо sector rotation почалась

### BTC Dominance як Compass
- BTC.D росте + BTC росте = risk-off, тримай тільки BTC (або SHORT alts)
- BTC.D падає + BTC росте = ALT SEASON, торгуй альти LONG
- BTC.D росте + BTC падає = panic, всі SELL, тримай кеш
- BTC.D падає + BTC падає = altcoins dying first, AVOID ALL

### Funding Rate як Sentiment
- Funding >+0.05% = market overleveraged LONG → ризик short squeeze → зменш LONG або SHORT
- Funding <-0.03% = market overleveraged SHORT → ризик long squeeze → зменш SHORT або LONG
- Funding near 0 = neutral, trade freely
- EXTREME funding (>0.1% або <-0.1%) = ДУЖЕ високий шанс розвороту

---

## 21. POSITION MANAGEMENT В РЕАЛЬНОМУ ЧАСІ

### Коли ДОДАВАТИ до позиції (Pyramiding):
- Тільки якщо позиція в прибутку (+1 ATR minimum)
- Тільки в напрямку тренду
- Кожне додавання = менший розмір (50% від попереднього)
- SL переносимо на breakeven для першого entry
- Максимум 3 додавання

### Коли ЗМЕНШУВАТИ позицію:
- При наближенні до strong resistance/support
- При зменшенні volume (momentum вичерпується)
- При divergence на RSI
- Перед major news (FOMC, CPI)
- При зміні режиму (trend → range)

### Hedging:
- Якщо маєш LONG позицію і ринок починає слабшати:
  - Відкрий невелику SHORT позицію на іншій корельованій монеті
  - Або зменш LONG на 50%
  - НЕ відкривай LONG і SHORT на ОДНІЙ монеті (це безглуздо, просто зменш)

---

## 22. MARKET MICROSTRUCTURE ADVANCED

### Bid-Ask Spread Analysis
- Tight spread (<0.01%) = здоровий ринок, можна торгувати
- Wide spread (>0.05%) = низька ліквідність, будь обережний
- Spread розширюється перед великим рухом (MM знають щось)
- Після великого руху spread тимчасово розширюється → не входи одразу

### Order Book Imbalance
- Bid depth >> Ask depth = bullish pressure
- Ask depth >> Bid depth = bearish pressure
- АЛЕ: великі ордери можуть бути spoofing (зникають перед виконанням)
- Кращий індикатор: TRADE flow (реальні угоди) ніж order book (наміри)

### Whale Detection
- Ордер > $500K на альткоїні = whale
- Ордер > $5M на BTC = institutional
- Whale купує на ASK (агресивно) = very bullish
- Whale продає на BID (агресивно) = very bearish
- Whale ставить limit на support = може бути trap (sell wall зверху)

---

## 23. TIME-BASED PATTERNS

### Внутріденна волатильність:
- Азійська сесія (00:00-08:00 UTC): низька волатильність, range
- Європейська сесія (08:00-16:00 UTC): середня, початок рухів
- Американська сесія (14:00-22:00 UTC): НАЙВИЩА волатильність
- Overlap EUR/US (14:00-16:00 UTC) = peak activity

### Тижневі патерни:
- Понеділок: часто продовжує рух вихідних
- Вівторок-Четвер: найбільш "чисті" сигнали
- П'ятниця: часто profit-taking, обережно з новими позиціями
- Вихідні: низька ліквідність, непередбачувані рухи

### Місячні патерни:
- Початок місяця: часто bullish (нові allocation, зарплати)
- Кінець кварталу: часто rebalancing, може бути sell-off
- Options expiry (останній четвер місяця): підвищена волатильність навколо max pain

---

## 24. ІНТЕГРАЦІЯ ЗНАНЬ — ЯК АНАЛІЗУВАТИ

### Чеклист перед кожним трейдом:
1. **РЕЖИМ**: Який зараз режим? (trend/range/breakout/volatile/calm)
2. **TIMEFRAME**: Всі TF в одному напрямку? (weekly→daily→4h→1h)
3. **РІВНІ**: Де найближчий S/R? Чи є місце для руху до TP?
4. **VOLUME**: Чи підтверджує volume мою thesis?
5. **R:R**: Чи >= 1:1? (краще 1.5:1+)
6. **КОРЕЛЯЦІЯ**: Чи не дублюю ризик з іншими позиціями?
7. **NEWS**: Чи є upcoming events що можуть зруйнувати setup?
8. **FUNDING**: Чи не йду проти екстремального funding?
9. **BREADTH**: Чи ринок в цілому підтримує мій напрямок?
10. **CONFIDENCE**: Якщо не впевнений — WAIT. Кращий трейд прийде

### Приоритет сигналів (від найважливішого):
1. Режим ринку (якщо capitulation → не торгуй LONG)
2. Weekly/Daily trend alignment
3. Market breadth (>60% монет в одному напрямку)
4. Volume confirmation
5. Pattern + candlestick confirmation
6. RSI/BB/MACD confirmation
7. News impact
8. Funding rate
9. ML model score
10. Per-coin historical behavior

### Формула рішення:
```
IF regime == CAPITULATION or EUPHORIA:
    WAIT (або trade тільки counter після підтвердження)
ELIF regime == STRONG_TREND:
    Trade pullbacks в напрямку trend (великий size)
ELIF regime == RANGE:
    Mean reversion на S/R (середній size)
ELIF regime == BREAKOUT:
    Entry на retest після пробиття (великий size після confirmation)
ELIF regime == HIGH_VOLATILITY:
    Momentum тільки з підтвердженням (маленький size)
ELIF regime == LOW_VOLATILITY:
    WAIT або мікро-trades (мінімальний size)
```

### Що робити коли НІЧОГО не зрозуміло:
- **WAIT** — це теж позиція
- Кращий трейд = ніякого трейду ніж поганий трейд
- Ринок ЗАВЖДИ дасть нову можливість
- Капітал який зберіг сьогодні = прибуток завтра

---

## 25. MACRO EVENTS IMPACT ON CRYPTO

### FOMC (Federal Reserve meetings)
- Day before: volatility LOW, market waits
- Announcement (14:00 EST / 18:00-19:00 UTC): EXTREME volatility for 30-60 min
- NEVER have open positions during FOMC announcement
- After: trend usually establishes within 2-4 hours
- Rate HIKE → bearish crypto (but if expected → "buy the news")
- Rate CUT → bullish crypto

### CPI (Consumer Price Index)
- Released 8:30 AM EST / 13:30 UTC, monthly
- High CPI → bearish (inflation → higher rates expected)
- Low CPI → bullish (rates may be cut)
- Impact lasts 4-12 hours, then fades

### NFP (Non-Farm Payrolls)
- First Friday of month, 8:30 AM EST
- Strong jobs → bearish crypto (rates stay high)
- Weak jobs → bullish crypto (rate cuts expected)
- Less impact than FOMC/CPI but still creates volatility

### Options Expiry
- Last Friday of month = max pain gravitational pull
- Price tends to move TOWARD max pain before expiry
- After expiry: free to move in either direction
- Thursday before expiry → position for post-expiry move

### GOLDEN RULE: Don't trade 30 minutes before/after major events.
Wait for dust to settle, then trade the established direction.

## 26. MARKET SESSION KNOWLEDGE

### Session Performance (FROM OUR 2-YEAR DATA — VERIFIED):
- **00:00-04:00 UTC:** avg +0.008%, vol 0.98% — quiet, neutral
- **04:00-08:00 UTC:** avg +0.003%, vol 0.81% — lowest volatility
- **08:00-12:00 UTC (EU open):** avg +0.052%, WR 54% — BEST session for entries
- **12:00-16:00 UTC (US open):** avg -0.014%, vol 1.30% — HIGHEST volatility but negative avg! Dangerous
- **16:00-20:00 UTC (US active):** avg -0.010%, vol 1.23% — still dangerous
- **20:00-00:00 UTC (Late US):** avg +0.055%, WR 53% — second best, profit-taking bounces

### Day of Week:
- **Weekday:** avg +0.020%/candle, vol 1.14%, WR 51%
- **Weekend:** avg +0.004%/candle, vol 0.69% (39% LESS volatile), WR 53% (slightly HIGHER!)
- Weekend = less volatile but MORE predictable. Good for scalps with tighter targets
- Crypto = 24/7, always trade. Weekend is opportunity, not risk

### Holidays:
- Christmas (Dec 24-26): volume -60%, DON'T TRADE
- New Year (Dec 31 - Jan 2): volume -50%, DON'T TRADE
- Chinese New Year: Asian volume drops, alts may dump
- US public holidays: reduced volume, less predictable
