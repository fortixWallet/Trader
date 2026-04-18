# Trailing Stop Simulation Results

Total unique trades analyzed: 307
Data sources: postmortem (98), fortix (103), okx (162)
After dedup with price data: 314

## 1. Config Comparison (Activation x Trail Drop -> Net PnL Diff)

| Act \ Drop | 2% | 3% | 4% | 5% |
|---|---|---|---|---|
| **3%** | $+495 | $+494 | $+513 | $+595 |
| **5%** | $-313 | $-319 | $-343 | $-192 |
| **6%** | $+82 | $+76 | $+74 | $+63 |
| **8%** | $+784 | $+784 | $+784 | $+766 |
| **10%** | $+817 **BEST** | $+811 | $+811 | $+793 |

## 2. Best Config: Activation=10%, Trail Drop=2%
- Actual total PnL: **$-3389.86**
- Trailing total PnL: **$-2572.46**
- Net improvement: **$+817.40** (+24.1%)
- Trades where trailing activated: 117/307 (38%)

## 3. Winners Hurt by Trailing
- Count: 24 trades
- Total PnL lost: $666.46

## 4. Losers Saved by Trailing
- Count: 20 trades
- Total PnL saved: $1377.01
- Net (saved - lost): $+710.54

## 5. False Activations (left money on table)
- Count: 12 trades (TP would have hit but trailing exited early)
- Money left: $449.74

## 6. Per-Day Breakdown
| Date | Trades | Actual PnL | Trailing PnL | Diff |
|------|--------|------------|--------------|------|
| 2026-04-11 | 50 | $-3027 | $-2725 | $+302 |
| 2026-04-12 | 14 | $-88 | $-88 | $+0 |
| 2026-04-13 | 48 | $-37 | $-43 | $-6 |
| 2026-04-14 | 45 | $150 | $115 | $-35 |
| 2026-04-15 | 36 | $50 | $93 | $+43 |
| 2026-04-16 | 103 | $-498 | $-63 | $+435 |
| 2026-04-17 | 11 | $60 | $138 | $+78 |

## 7. Per-Coin Analysis (best config)
| Coin | Trades | Actual PnL | Trailing PnL | Diff | Saved |
|------|--------|------------|--------------|------|-------|
| TAO | 16 | $-427 | $-103 | $+324 | 4 |
| PENDLE | 27 | $-41 | $255 | $+296 | 9 |
| CRV | 18 | $-302 | $-217 | $+85 | 3 |
| AVAX | 20 | $224 | $305 | $+81 | 4 |
| SOL | 12 | $-107 | $-32 | $+75 | 2 |
| POL | 19 | $-98 | $-29 | $+69 | 1 |
| LDO | 19 | $-104 | $-85 | $+19 | 5 |
| LINK | 28 | $-43 | $-26 | $+18 | 1 |
| BNB | 17 | $-61 | $-54 | $+7 | 1 |
| RENDER | 3 | $-387 | $-381 | $+5 | 1 |
| ADA | 24 | $-36 | $-30 | $+5 | 2 |
| DOGE | 4 | $-22 | $-22 | $+0 | 0 |
| WIF | 1 | $-9 | $-9 | $+0 | 0 |
| ARB | 3 | $94 | $94 | $+0 | 0 |
| AAVE | 7 | $-1562 | $-1562 | $+0 | 0 |
| DOT | 10 | $-715 | $-715 | $+0 | 0 |
| BTC | 20 | $-123 | $-130 | $-7 | 0 |
| OP | 4 | $83 | $70 | $-14 | 0 |
| XRP | 10 | $48 | $9 | $-39 | 0 |
| ETH | 36 | $104 | $64 | $-39 | 1 |
| UNI | 9 | $93 | $25 | $-69 | 0 |

## 8. Recent (Apr 15-17) vs Historical
- **Apr 15-17**: 150 trades, actual $-389, trailing $168, diff $+556
- **Historical**: 157 trades, actual $-3001, trailing $-2740, diff $+261

## 9. HONEST Assessment

- Configs with positive net: 16/20
- Configs with negative net: 4/20
- Best config improvement: $+817.40
- Worst config damage: $-342.73
- Spread: $1160 between best and worst

**CONCERN 1: Trailing HURTS more trades (24) than it SAVES (20).** Net positive only because saved amounts are larger per trade. A few big reversals (TAO circuit breakers: $347 saved, PENDLE reversals: $137+$75 saved) drive most of the benefit.

**CONCERN 2: The 5% activation row is NEGATIVE across all trail drops (-$192 to -$343).** This means the benefit is NOT monotonic — specific activation thresholds hurt. This is a sign of instability.

**CONCERN 3: Apr 16 dominates.** $435 of the $817 total improvement comes from ONE DAY (Apr 16, 103 trades). Remove that day and the benefit drops to $382 across 204 trades ($1.87/trade).

**CONCERN 4: Historical period is weak.** Only $+261 improvement on 157 historical trades ($1.66/trade) vs $+556 on 150 recent trades ($3.71/trade). The benefit is not consistently large.

**CONCERN 5: 12 false activations** left $450 on the table (trades that activated trailing, exited early, but TP would have hit). This partially offsets the savings.

**CONCERN 6: 1h candle limitation.** We cannot determine intra-candle price ordering. A trailing stop that activates and triggers within the same hour may be unrealistic with actual tick data.

**GOOD SIGN:** 16/20 configs are positive. The activation threshold matters more than trail drop (act 8-10% consistently best). This suggests the signal is real but small.

### Bottom Line
Trailing stop shows a MODEST, FRAGILE benefit. The +$817 best config is real but:
- Driven by ~5 big reversal saves, not systematic improvement
- $2.66/trade average improvement is small vs typical trade sizes ($350 margin)
- The 5% activation anomaly suggests non-robust behavior
- Conservative recommendation: act=8%, drop=3% (more stable across periods, $+784)
- IMPLEMENT WITH CAUTION: paper-trade first, track false activation rate, and compare to random walk to rule out curve-fitting