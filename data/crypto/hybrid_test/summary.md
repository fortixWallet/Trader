# Hybrid 1D Direction + 4H Entry Backtest Results

**Test period**: 2024-01-01 to 2026-04-17
**Total days**: 838
**Active coins**: 24 (ADA, ALGO, APT, AVAX, BCH, BNB, BTC, CRV, ETH, FIL, HBAR, JUP, LDO, LINK, NEAR, PENDLE, POL, PYTH, SOL, SUI, TON, UNI, XLM, XRP)
**Start equity**: $5,000 | **Leverage**: 8x | **Fees**: 0.08% roundtrip

## Scenario Comparison

| Metric | A) Conservative | B) Moderate | C) Aggressive | D) No Filter |
|--------|----------------|-------------|---------------|---------------|
| Total Trades | 7438 | 10417 | 11948 | 18378 |
| Win Rate | 46.1% | 45.1% | 44.6% | 46.0% |
| Trades/Day | 8.9 | 12.4 | 14.3 | 21.9 |
| SL Rate | 28.2% | 31.1% | 32.2% | 28.2% |
| TP Rate | 17.6% | 17.7% | 17.0% | 18.3% |
| Avg Daily ROI | -8.529% | -5.675% | -0.867% | -6.575% |
| Median Daily ROI | 0.0% | 0.0% | 0.0% | -1.624% |
| Min Daily ROI | -1361.57% | -1859.41% | -40.98% | -974.98% |
| Max Daily ROI | 562.73% | 1364.05% | 59.48% | 893.68% |
| Days Profitable | 23.2% | 23.2% | 22.0% | 45.7% |
| Days >= 10% ROI | 15.0% | 15.4% | 9.2% | 26.0% |
| Max Drawdown | 571.1% | 829.8% | 112.9% | 456.3% |
| Final Equity | $3,534 | $2,584 | $126 | $107 |
| Total Return | -29.3% | -48.3% | -97.5% | -97.9% |
| Avg Trade PnL | -0.76% | -0.89% | -0.99% | -0.48% |

## Daily Direction Distribution & Win Rate (Scenario B)

| Direction | Days | Trades | Wins | WR |
|-----------|------|--------|------|----|
| STRONG_LONG | 241 | 5537 | 2388 | 43.1% |
| LONG | 0 | 0 | 0 | 0% |
| NEUTRAL | 382 | 0 | 0 | 0% |
| SHORT | 0 | 0 | 0 | 0% |
| STRONG_SHORT | 215 | 4880 | 2313 | 47.4% |

## Per-Coin Performance (Scenario B)

| Coin | Trades | Wins | WR | Total PnL |
|------|--------|------|-----|----------|
| XLM | 608 | 298 | 49.0% | $+3,538.91 |
| BTC | 208 | 94 | 45.2% | $+334.25 |
| NEAR | 45 | 22 | 48.9% | $+112.28 |
| XRP | 9 | 3 | 33.3% | $+45.85 |
| HBAR | 73 | 32 | 43.8% | $-217.83 |
| ALGO | 384 | 173 | 45.1% | $-328.58 |
| LINK | 36 | 9 | 25.0% | $-334.54 |
| PENDLE | 21 | 7 | 33.3% | $-544.32 |
| TON | 40 | 16 | 40.0% | $-624.88 |
| SUI | 43 | 17 | 39.5% | $-712.75 |
| AVAX | 164 | 62 | 37.8% | $-749.29 |
| APT | 308 | 134 | 43.5% | $-1,546.36 |
| ETH | 1951 | 942 | 48.3% | $-1,839.79 |
| BNB | 247 | 65 | 26.3% | $-1,903.61 |
| BCH | 813 | 378 | 46.5% | $-2,909.84 |
| UNI | 669 | 293 | 43.8% | $-2,927.30 |
| ADA | 592 | 245 | 41.4% | $-3,346.72 |
| CRV | 1951 | 942 | 48.3% | $-3,809.78 |
| SOL | 677 | 259 | 38.3% | $-6,696.05 |
| FIL | 902 | 419 | 46.5% | $-7,108.56 |
| LDO | 676 | 291 | 43.0% | $-9,177.27 |

## Daily ROI Distribution (Scenario B)

```
        < -20% | ################################################################################ (143, 17.1%)
   -20 to -10% | ########################################################## (58, 6.9%)
    -10 to -5% | ################################### (35, 4.2%)
     -5 to -2% | ############# (13, 1.6%)
      -2 to 0% | ######### (9, 1.1%)
   = 0% (idle) | ################################################################################ (384, 45.8%)
       0 to 2% | ############## (14, 1.7%)
       2 to 5% | ####################### (23, 2.7%)
      5 to 10% | ############################ (28, 3.3%)
     10 to 20% | ################################# (33, 3.9%)
         > 20% | ################################################################################ (94, 11.2%)
```

## Answers to Key Questions

### 1. Hybrid WR vs No-Filter Baseline?
- Hybrid (1D+4H): **45.1%** WR
- No daily filter: **46.0%** WR
- Delta: **-0.9%** -- daily direction does not improve WR

### 2. Realistic Trades Per Day?
- A_Conservative: **8.9**/day
- B_Moderate: **12.4**/day
- C_Aggressive: **14.3**/day

### 3. What % of Days Achieve 10%+ ROI?
- A_Conservative: **15.0%** of days
- B_Moderate: **15.4%** of days
- C_Aggressive: **9.2%** of days

### 4. Realistic Daily ROI (Median)?
- A_Conservative: **0.0%** median daily ROI
- B_Moderate: **0.0%** median daily ROI
- C_Aggressive: **0.0%** median daily ROI

### 5. Max Drawdown?
- A_Conservative: **571.1%**
- B_Moderate: **829.8%**
- C_Aggressive: **112.9%**

### 6. Best Coins in Hybrid System?
Top 5 by total PnL (Scenario B):
  1. **XLM** -- 608 trades, 49.0% WR, $+3,538.91
  2. **BTC** -- 208 trades, 45.2% WR, $+334.25
  3. **NEAR** -- 45 trades, 48.9% WR, $+112.28
  4. **XRP** -- 9 trades, 33.3% WR, $+45.85
  5. **HBAR** -- 73 trades, 43.8% WR, $-217.83

Worst 3:
  - **SOL** -- 677 trades, 38.3% WR, $-6,696.05
  - **FIL** -- 902 trades, 46.5% WR, $-7,108.56
  - **LDO** -- 676 trades, 43.0% WR, $-9,177.27

### 7. Does Daily Direction Improve WR vs Random?
Direction-specific WR (Scenario B):
  - STRONG_LONG: 43.1% (5537 trades)
  - LONG: 0% (0 trades)
  - NEUTRAL: 0% (0 trades)
  - SHORT: 0% (0 trades)
  - STRONG_SHORT: 47.4% (4880 trades)

Overall hybrid: 45.1% vs no-filter: 46.0%
Daily direction adds **-0.9%** to WR.

## Final Recommendation

10% daily ROI is NOT consistently achievable. Median daily ROI = 0.0% (Moderate), 0.0% (Aggressive).
Avg daily ROI = -5.675% (Moderate). Only 15.4% of days reach 10%+ (Moderate), 9.2% (Aggressive).

Max drawdown: 829.8% -- DANGEROUS for $5,000 account.
Recommended: Scenario B (Moderate) as baseline.
