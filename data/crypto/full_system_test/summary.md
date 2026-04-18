# Full System Test: Trailing Stop + Tighter TP

**Date range:** 2026-04-11 to 2026-04-17
**Total trades:** 265 (104 fortix, 161 okx)
**Simulated (has SL/TP + candles):** 93
**Not simulated (no SL/TP):** 172

## 1. Grand Total Comparison

| Metric | S1: Actual | S2: +Trailing | S3: +Trail+TightTP |
|--------|-----------|--------------|--------------------|
| Trades | 265 | 265 | 265 |
| Wins | 138 | 139 | 148 |
| Win Pct | 52.10 | 52.50 | 55.80 |
| Total Pnl | -2990.71 | -2971.65 | -2661.67 |
| Roi Pct | -59.81 | -59.43 | -53.23 |
| Avg Win | 24.60 | 24.22 | 24.36 |
| Avg Loss | -50.28 | -50.30 | -53.56 |
| Profit Factor | 0.53 | 0.53 | 0.58 |
| Max Dd Pct | 71.80 | 71.80 | 71.80 |
| Final Equity | 2009.29 | 2028.35 | 2338.33 |
| Winning Days | 3/7 | 3/7 | 4/7 |

## 2. Per-Day Comparison

| Date | S1 PnL | S1 Win% | S2 PnL | S2 Win% | S3 PnL | S3 Win% |
|------|--------|---------|--------|---------|--------|--------|
| 2026-04-11 | $-2972.96 | 48.1% | $-2972.96 | 48.1% | $-2972.96 | 48.1% |
| 2026-04-12 | $-91.62 | 56.2% | $-91.62 | 56.2% | $-91.62 | 56.2% |
| 2026-04-13 | $-34.34 | 49.0% | $-34.34 | 49.0% | $-34.34 | 49.0% |
| 2026-04-14 | $60.53 | 60.4% | $79.12 | 60.4% | $126.98 | 72.9% |
| 2026-04-15 | $-18.61 | 51.9% | $47.00 | 55.6% | $84.07 | 55.6% |
| 2026-04-16 | $20.46 | 49.2% | $-40.53 | 49.2% | $102.44 | 52.5% |
| 2026-04-17 | $45.83 | 60.0% | $41.68 | 60.0% | $123.77 | 70.0% |

## 3. Win Rate Comparison

- S1 Actual: 52.1% (138/265)
- S2 Trailing: 52.5% (139/265)
- S3 Trail+Tight: 55.8% (148/265)

## 4. Exit Category Shifts

### S2 (Trailing) shifts from actual:
- TARGET_HIT -> TRAIL_EXIT: 10 trades
- STOP_LOSS -> TRAIL_EXIT: 5 trades
- NEWS_REACTION -> TRAIL_EXIT: 5 trades
- NEWS_REACTION -> TARGET_HIT: 4 trades
- TIME_EXIT -> TRAIL_EXIT: 4 trades
- STOP_LOSS -> TARGET_HIT: 3 trades

### S3 (Trail+TightTP) shifts from actual:
- TARGET_HIT -> TIGHT_TP: 15 trades
- STOP_LOSS -> TIGHT_TP: 8 trades
- NEWS_REACTION -> TIGHT_TP: 8 trades
- TIME_EXIT -> TIGHT_TP: 5 trades
- NEWS_REACTION -> TRAIL_EXIT: 3 trades
- STOP_LOSS -> TRAIL_EXIT: 2 trades
- TIME_EXIT -> TRAIL_EXIT: 1 trades

## 5. Coins That BENEFIT Most from Trailing

| Coin | Trades | Actual PnL | Trail PnL | Diff | Trail+Tight PnL | Diff |
|------|--------|-----------|-----------|------|----------------|------|
| ADA | 20 | $29.45 | $58.57 | $+29.11 | $59.07 | $+29.62 |
| AVAX | 14 | $198.77 | $226.19 | $+27.42 | $245.80 | $+47.02 |
| BTC | 14 | $-129.31 | $-102.78 | $+26.53 | $-93.00 | $+36.32 |
| OP | 5 | $97.84 | $111.57 | $+13.73 | $110.59 | $+12.75 |
| POL | 15 | $-59.49 | $-51.43 | $+8.06 | $-38.53 | $+20.96 |
| TAO | 18 | $-387.93 | $-382.28 | $+5.65 | $-384.21 | $+3.72 |
| RENDER | 4 | $-386.32 | $-380.79 | $+5.53 | $-381.99 | $+4.32 |
| SOL | 9 | $-60.20 | $-54.81 | $+5.39 | $-47.06 | $+13.14 |
| ETH | 32 | $85.95 | $88.72 | $+2.77 | $134.75 | $+48.81 |
| RAVE | 3 | $-0.34 | $-0.34 | $+0.00 | $-0.34 | $+0.00 |

## 6. Coins HURT by Trailing

| Coin | Trades | Actual PnL | Trail PnL | Diff | Trail+Tight PnL | Diff |
|------|--------|-----------|-----------|------|----------------|------|
| DOGE | 4 | $-21.61 | $-26.11 | $-4.51 | $-21.67 | $-0.06 |
| PENDLE | 20 | $-8.04 | $-18.23 | $-10.19 | $79.86 | $+87.89 |
| CRV | 11 | $-213.83 | $-224.87 | $-11.04 | $-189.21 | $+24.62 |
| UNI | 5 | $47.80 | $31.92 | $-15.88 | $39.61 | $-8.19 |
| LDO | 13 | $-14.27 | $-77.80 | $-63.53 | $-36.39 | $-22.12 |

## 7. Days Where Trailing SAVED vs HURT

- **2026-04-11**: Trail SAME ($+0.00), Trail+Tight SAME ($+0.00)
- **2026-04-12**: Trail SAME ($+0.00), Trail+Tight SAME ($+0.00)
- **2026-04-13**: Trail SAME ($+0.00), Trail+Tight SAME ($+0.00)
- **2026-04-14**: Trail SAVED ($+18.59), Trail+Tight SAVED ($+66.45)
- **2026-04-15**: Trail SAVED ($+65.61), Trail+Tight SAVED ($+102.68)
- **2026-04-16**: Trail HURT ($-60.99), Trail+Tight SAVED ($+81.98)
- **2026-04-17**: Trail HURT ($-4.15), Trail+Tight SAVED ($+77.94)

## 8. Robustness Assessment

**Simulated trades:** 93 of 265 total

**S2 Trailing impact on simulated trades:**
- Trades improved: 26
- Trades worsened: 12
- Trades unchanged: 55
- Total improvement: $240.30
- Largest single improvement: $32.51
- Total worsening: $-221.24
- Largest single worsening: $-50.35
- **Net impact: $+19.06**

Top 5 biggest changes (trailing):
- PENDLE LONG (2026-04-16T12:13): $-50.35 (actual=TARGET_HIT, trail=TRAIL_EXIT)
- LDO LONG (2026-04-16T06:47): $-44.95 (actual=TARGET_HIT, trail=TRAIL_EXIT)
- BTC LONG (2026-04-15T21:16): $+32.51 (actual=STOP_LOSS, trail=TARGET_HIT)
- PENDLE SHORT (2026-04-16T08:47): $+28.46 (actual=STOP_LOSS, trail=TRAIL_EXIT)
- ADA SHORT (2026-04-16T12:06): $+25.67 (actual=STOP_LOSS, trail=TRAIL_EXIT)

Outlier check: top 2 improvements = $60.96 = 25% of total improvement
Improvement reasonably distributed across trades.

**S3 Trailing+TightTP impact on simulated trades:**
- Trades improved: 26
- Trades worsened: 18
- Trades unchanged: 49
- **Net impact: $+329.04**

## 9. Projected Daily ROI if Deployed

- **S1 Actual**: $-427.24/day = -8.54%/day on $5000
- **S2 Trailing**: $-424.52/day = -8.49%/day on $5000
- **S3 Trail+Tight**: $-380.24/day = -7.60%/day on $5000

*Based on 7 trading days, 265 total trades.*
*172 trades could not be simulated (no SL/TP data) — scenarios 2 & 3 use actual PnL for those.*
