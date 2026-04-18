# TP13 / SL6.5 / Trail(6→-2) Simulation Results
**Period:** April 12-17, 2026 (6 days)

**Config:** TP=+13% ROI, SL=-6.5% ROI, Trailing activation=+6% ROI, Trail drop=-2% from peak
**Simulation leverage:** 8x | **Margin per trade:** $350.0
**Max hold:** 12h | **Candle resolution:** 1h

## 1. Grand Total

| Metric | Actual | TP13/SL6.5/Trail6-2 |
|--------|--------|---------------------|
| Total Trades | 181 | 181 |
| Wins | 91 | 93 |
| Win Rate | 50.3% | 51.4% |
| Total PnL | $-147.17 | $742.96 |
| Avg PnL/trade | $-0.81 | $4.10 |
| Gross Profit | $1,503.65 | $2,636.40 |
| Gross Loss | -$1,650.81 | -$1,893.44 |
| Profit Factor | 0.91 | 1.39 |
| **Delta PnL** | | **$+890.12** |

## 2. Per-Day Breakdown

| Day | Trades | Actual PnL | Sim PnL | Delta |
|-----|--------|------------|---------|-------|
| 2026-04-12 | 16 | $-91.62 | $203.63 | $+295.25 |
| 2026-04-13 | 51 | $-34.34 | $-106.61 | $-72.27 |
| 2026-04-14 | 41 | $61.13 | $448.12 | $+386.99 |
| 2026-04-15 | 17 | $-84.67 | $82.47 | $+167.15 |
| 2026-04-16 | 47 | $-24.52 | $-16.57 | $+7.95 |
| 2026-04-17 | 9 | $26.86 | $131.91 | $+105.05 |

## 3. Exit Type Distribution (Simulated)

| Exit Type | Count | Avg ROI % | Total PnL |
|-----------|-------|-----------|----------|
| SL | 79 | -6.50% | $-1,797.25 |
| TRAIL | 82 | +8.97% | $2,574.71 |
| TIME | 3 | -0.30% | $-3.14 |
| NO_DATA | 17 | +3.13% | $-31.36 |

*NO_DATA = coins without 1h candle data (BTC, COAI, ENJ, JUP, LTC, MYX, NEAR, RAVE, ZAMA); uses actual PnL as fallback.*

**Note:** TP never triggers because trailing (activate=6%, drop=2%) catches all trades before they reach 13% ROI. This is by design -- the trailing stop is tight enough to exit profitably well before TP.

## 4. Key Stats

- **Trailing activated:** 82 trades (45.3%)
  - Avg peak ROI when trailing activated: 10.97%
- **Trailing exits:** 82 trades
  - Avg trail exit ROI: 8.97%
- **TP hits:** 0 trades
- **Would have hit TP but got trailed first:** 20 trades

- **Best trade (sim):** AAVE SHORT on 2026-04-12 → $125.77 (TRAIL, 35.93% ROI)
- **Worst trade (sim):** BTC LONG on 2026-04-17 → $-27.17 (NO_DATA, -0.83% ROI)

## 5. Per-Coin Breakdown

### Top 5 Coins (Sim PnL)

| Coin | Trades | Actual PnL | Sim PnL | Delta |
|------|--------|------------|---------|-------|
| PENDLE | 17 | $-12.44 | $231.86 | $+244.30 |
| AVAX | 11 | $189.05 | $198.28 | $+9.22 |
| TAO | 3 | $44.14 | $135.28 | $+91.15 |
| AAVE | 3 | $-532.54 | $117.49 | $+650.03 |
| LDO | 12 | $-15.36 | $88.58 | $+103.94 |

### Bottom 5 Coins (Sim PnL)

| Coin | Trades | Actual PnL | Sim PnL | Delta |
|------|--------|------------|---------|-------|
| CRV | 8 | $247.10 | $-27.40 | $-274.51 |
| BNB | 9 | $-25.82 | $-37.15 | $-11.33 |
| ADA | 16 | $-19.71 | $-48.76 | $-29.05 |
| XRP | 7 | $13.47 | $-59.43 | $-72.90 |
| SOL | 6 | $-22.62 | $-98.65 | $-76.03 |

### Simulated-only (excluding NO_DATA coins)

| Metric | Actual (164 trades) | Simulated |
|--------|--------|----------|
| Wins | 81 | 83 |
| Win Rate | 49.4% | 50.6% |
| PnL | $-115.80 | $774.32 |
| Profit Factor | - | 1.43 |
| Delta | | $+890.12 |


## 6. Honest Assessment

The TP13/SL6.5/Trail(6→-2) config would have been **BETTER** than actual by **$+890.12**.

### Analysis

- Profit factor improved: 0.91 → 1.39
- Win rate change: 50.3% → 51.4% (+1.1pp)
- Avg PnL per trade: $-0.81 → $4.10
- SL hit rate: 79/181 (43.6%)
- The 2:1 R:R (13% TP vs 6.5% SL) requires >33% win rate to be profitable
- Trailing stop protected gains on 82 trades, avg exit ROI: 8.97%
