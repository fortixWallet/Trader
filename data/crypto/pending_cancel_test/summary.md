# Pending Order Cancel Test — Apr 15-16, 2026

**Hypothesis:** Cancel LONG pending orders when BTC drops ≥X% from placement price; cancel SHORT when BTC rises ≥X%.

## Baseline

- **Total trades:** 74
- **Wins:** 31 (42%)
- **Actual PnL:** $-163.68

## Results by Threshold

| Threshold | Cancelled | Kept | Kept Wins | Simulated PnL | Improvement | Saved (SL avoided) | Lost (TP missed) |
|-----------|-----------|------|-----------|---------------|-------------|--------------------|-----------------|
| 0.2% | 8 | 66 | 26 (39%) | $-298.85 | $-135.17 | $+32.32 | $-167.49 |
| 0.3% | 4 | 70 | 29 (41%) | $-163.96 | $-0.28 | $+25.11 | $-25.39 |
| 0.5% | 3 | 71 | 29 (41%) | $-173.14 | $-9.46 | $+15.93 | $-25.39 |
| 0.7% | 0 | 74 | 31 (42%) | $-163.68 | $+0.00 | $+0.00 | $+0.00 |

## Cancelled Trades Breakdown

### Threshold 0.2%

| Time | Side | Coin | BTC Change | PnL | Exit |
|------|------|------|------------|-----|------|
| 04-15 02:12 | SHORT | XRP | +0.59% | $+13.51 | TARGET_HIT |
| 04-15 02:12 | SHORT | ADA | +0.59% | $+11.88 | TARGET_HIT |
| 04-15 02:12 | SHORT | BNB | +0.59% | $-15.93 | STOP_LOSS |
| 04-16 01:15 | LONG | LINK | -0.21% | $-7.21 | STOP_LOSS |
| 04-16 03:14 | LONG | UNI | -0.21% | $+40.75 | TARGET_HIT |
| 04-16 03:14 | LONG | AVAX | -0.21% | $+42.43 | TARGET_HIT |
| 04-16 03:14 | LONG | PENDLE | -0.21% | $+58.92 | TARGET_HIT |
| 04-16 12:13 | LONG | ETH | -0.38% | $-9.18 | STOP_LOSS |

### Threshold 0.3%

| Time | Side | Coin | BTC Change | PnL | Exit |
|------|------|------|------------|-----|------|
| 04-15 02:12 | SHORT | XRP | +0.59% | $+13.51 | TARGET_HIT |
| 04-15 02:12 | SHORT | ADA | +0.59% | $+11.88 | TARGET_HIT |
| 04-15 02:12 | SHORT | BNB | +0.59% | $-15.93 | STOP_LOSS |
| 04-16 12:13 | LONG | ETH | -0.38% | $-9.18 | STOP_LOSS |

### Threshold 0.5%

| Time | Side | Coin | BTC Change | PnL | Exit |
|------|------|------|------------|-----|------|
| 04-15 02:12 | SHORT | XRP | +0.59% | $+13.51 | TARGET_HIT |
| 04-15 02:12 | SHORT | ADA | +0.59% | $+11.88 | TARGET_HIT |
| 04-15 02:12 | SHORT | BNB | +0.59% | $-15.93 | STOP_LOSS |

### Threshold 0.7%

No trades cancelled at this threshold.

## Conclusion

**The cancel rule does NOT help on this data.** Every threshold tested either hurts PnL or has zero effect.

- At 0.2%: cancels 8 trades but 5 of them were winners ($+167 lost opportunity) vs only $32 saved from SL. Net: -$135.
- At 0.3%: cancels 4 trades, nearly break-even ($-0.28). Catches 2 SL but also kills 2 TP trades.
- At 0.5%+: too few cancels to matter, and they still net negative.

**Key limitation:** Most orders fill within 2-5 minutes of placement. With only 1h BTC candles available, BTC price appears unchanged for 52/74 trades (70%). The rule only triggers on the few trades that waited 30-60min for fill, during which a full candle shift occurred. Of those, the winners outnumber the losers.

**Why it fails:** The PATIENT limit orders that wait longest and see BTC move against them are often contrarian entries that eventually profit (mean-reversion). Cancelling them removes good trades, not bad ones.
