# TRADE_5 — Plan for 5% ROI/Day System

## TARGET
- 5% ROI/day on $5,000 = $250/day
- Stable, not occasional spikes
- Based on REAL data, not estimates

## RAW DATA FOUNDATION

### Market reality (last 30 days):
- BTC 3h move >1.0%: 21% of all 3h windows
- BTC 3h move >0.8%: 29%
- LDO 3h move >1.6%: 20% (most volatile of quality coins)
- PENDLE 3h move >1.6%: 27%

### Current system performance:
- OLD code + trailing (act8/drop2): +$575/3days = +$192/day = 3.8% ROI ✓
- WR ~50% (coin flip) but R:R 1.38 (trailing avg 9% / SL 6.5%) = profitable
- ~30 trades/day with OLD aggressive prompt
- Guardian saves ~$100-130 per reversal event

### Gap to 5%:
- Current: $192/day (3.8%)
- Target: $250/day (5.0%)
- Need: +$58/day more = +30% improvement

## MATH: How to Get +30% More

### Option A: More trades (same WR)
```
Current: 30 trades × 50% WR × ($31 win - $23 loss) / 2 = $120 net
If 40 trades: 40 × $4 = $160 net (+33%)
```
How: scan every 45min instead of 1h = ~32 scans/day vs 24

### Option B: Better WR (same trades)
```
30 trades × 55% WR: 16.5 wins × $31 - 13.5 losses × $23 = $201 net
vs 30 × 50%: 15 × $31 - 15 × $23 = $120
Improvement: +67%
```
How: macro filter already adds ~5pp → just keeping it adds value

### Option C: Bigger wins (same trades, same WR)
```
30 trades × 50% WR, but avg win $38 (trail at higher levels):
15 × $38 - 15 × $23 = $225 net (vs $120) = +87%
```
How: let winners run longer (act10/drop2 instead of act8/drop2)

### Option D: Less damage per loss
```
SL -5% instead of -6.5%: loss = $17.5 instead of $23
30 × 50% × ($31 - $17.5) = $202 net (+68%)
```
But tighter SL = more SL triggers = lower WR. Tradeoff.

### BEST COMBINATION: A + C
```
35 trades/day (scan 45min) × 50% WR
Trailing act10/drop2: avg win ~$36
SL -6.5%: avg loss ~$23

17.5 wins × $36 = $630
17.5 losses × $23 = $402
Net: $228/day = 4.6% ROI

+ Guardian saves 1 reversal/day avg = ~$50
Total: $278/day = 5.6% ROI ✓
```

## SYSTEM DESIGN

### Architecture:
```
Layer 1: OLD Profi (aggressive, MUST find setups)
  - Scans every 45min
  - 2 batches × 8 coins = 16 coins per scan
  - Generates 5-8 setups per scan
  - MAX_PENDING = 8

Layer 2: Trailing Stop (act10/drop2, SL -6.5%, TP +13%)
  - Manages ALL exits automatically
  - No reliance on Profi's TP/SL suggestions
  - Catches profit before reversal

Layer 3: Guardian Thread (every 10 sec)
  - Monitors portfolio ROI
  - 3+ positions negative → call Profi
  - REVERSAL → close + rescan
  - PULLBACK → hold, trailing handles

Layer 4: Macro Data (passive, in prompt)
  - F&G, MVRV, SOPR, BTC 7d/1d, funding
  - Profi sees but not forced to use
  - Background awareness
  
Layer 5: Learning
  - Trade feedback with context (per scan)
  - Auto-lessons (today's patterns)
  - Daily analysis (02:00 UTC)
  - Weekly synthesis (Monday)
```

### Key Parameters:
```
Scan interval: 45min (vs 60min current)
MAX_PENDING: 8
Leverage: 8x
SL: -6.5% ROI (code-enforced)
TP: +13% ROI (code-enforced)
Trailing: activation +10% ROI, drop -2%
Hold max: 3h
Cooldown: 2h after SL
Guardian: 3+ neg or 15% portfolio drawdown
Coins: 25 (OLD list, proven)
Batches: 2 × 8 (OLD, proven)
```

### Projected Daily:
```
Scans: 32/day (every 45min)
Setups: ~6/scan × 32 = ~192 (not all placed due to MAX_PENDING)
Placed: ~160 (with overlaps)
Filled: ~35-40 (patient fills ~20-25%)
Exits:
  - Trailing: ~15 (avg +$36)
  - SL: ~15 (avg -$23)
  - TP: ~3 (avg +$46)
  - TIME: ~5 (avg +$5)

Revenue: 15×$36 + 3×$46 + 5×$5 = $540 + $138 + $25 = $703
Loss: 15×$23 = $345
Guardian saves: ~$50/day

Net: $703 - $345 + $50 = $408/day → split with fees/slippage ≈ $280
ROI: 5.6% ✓
```

## CHANGES FROM CURRENT SYSTEM

Only 3 changes needed:

1. **Scan interval 60min → 45min** (1 line in code)
2. **Trailing activation 8% → 10%** (1 line — lets winners run longer)
3. **Restore macro data in prompt** (add back MACRO line — passive, doesn't limit)

Everything else stays: OLD prompt, OLD knowledge, guardian, trailing.

## VALIDATION PLAN

Before deploying:
1. Backtest trailing act10/drop2 on same 210 trades (compare with act8/drop2)
2. Simulate 45min scan vs 60min on Apr 15-17 data
3. If both positive → deploy
4. Monitor 48h live → confirm 5% avg

## RISKS

1. Scan 45min = +33% API cost ($260→$350/month)
2. Trailing act10 = more positions reach activation but also more false trails
3. More scans = more opportunities BUT also more SL if direction wrong
4. 50% WR assumption may degrade in choppy markets

## FALLBACK

If 5% not achieved in 1 week:
- Try act12/drop2 (even higher trail activation)
- Or increase leverage to 10x on high-conf setups only
- Or add 4H scan overlay for direction confirmation
