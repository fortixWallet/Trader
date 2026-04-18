# Checklist Deep Validation Results

**Date**: 2026-04-17 11:22
**Total trades**: 263 (fortix: 101, okx: 162)
**Trades with >=4 valid factors**: 246
**Date range**: 2026-04-11 13:39:44.019756+00:00 to 2026-04-17 02:18:43.705094+00:00

## 1. Score vs Win Rate

| Score | Trades | WR% | Total PnL | Avg PnL |
|-------|--------|-----|-----------|--------|
| 2 | 40 | 45.0% | $-1953.10 | $-48.83 |
| 3 | 91 | 46.2% | $+47.75 | $+0.52 |
| 4 | 88 | 61.4% | $-591.12 | $-6.72 |
| 5 | 27 | 48.1% | $-466.03 | $-17.26 |

## 2. Optimal Threshold

| Threshold | Pass | WR% | PnL | Sharpe | /day | Fail WR% | Gap |
|-----------|------|-----|-----|--------|------|----------|-----|
| >=2 | 246 | 51.6% | $-2962.49 | -0.104 | 45.2 | 0% | +51.6pp |
| >=3 | 206 | 52.9% | $-1009.39 | -0.06 | 37.9 | 45.0% | +7.9pp |
| >=4 | 115 | 58.3% | $-1057.15 | -0.088 | 21.1 | 45.8% | +12.5pp |
| >=5 | 27 | 48.1% | $-466.03 | -0.138 | 5.0 | 52.1% | -3.9pp |

## 3. Time-Split Robustness

Split point: 2026-04-14 14:07:01.358882+00:00

**Threshold >=3**: H1 gap -2.3pp, H2 gap +16.9pp → INCONSISTENT

**Threshold >=4**: H1 gap +13.7pp, H2 gap +9.4pp → CONSISTENT

**Threshold >=5**: H1 gap +2.1pp, H2 gap -9.1pp → INCONSISTENT

## 4. Per-Factor Stability

| Factor | YES WR | NO WR | Gap | H1 Gap | H2 Gap | Stability |
|--------|--------|-------|-----|--------|--------|----------|
| macro | 57.3% | 45.9% | +11.4pp | +12.2pp | +10.3pp | STABLE |
| ema_4h | 52.9% | 50.0% | +2.9pp | -7.1pp | +12.3pp | UNSTABLE |
| coin_wr | 53.3% | 50.5% | +2.8pp | +14.1pp | -4.7pp | UNSTABLE |
| entry_dist | 51.6% | 0% | +51.6pp | +48.0pp | +55.3pp | STABLE |
| rsi_ok | 51.2% | 75.0% | -23.8pp | +48.0pp | -20.4pp | UNSTABLE |

## 5. Inverted Factors

**OB**: YES WR 53.8%, NO WR 40.8%, Gap +13.0pp → NORMAL

**Funding**: YES WR 40.0%, NO WR 53.6%, Gap -13.6pp → INVERTED

## 6. Best Factor Combinations

| Factors | Threshold | Trades | WR% | Gap | Sharpe |
|---------|-----------|--------|-----|-----|--------|
| macro+ema_4h+ob_inv+funding_inv | >=4 | 18 | 66.7% | +26.4pp | 0.412 |
| macro+entry_dist+ob_inv+funding_inv | >=4 | 19 | 63.2% | +19.3pp | 0.371 |
| macro+ob_inv+funding_inv | >=3 | 19 | 63.2% | +19.3pp | 0.371 |
| macro+ob_inv+funding_inv | >=3 | 19 | 63.2% | +19.3pp | 0.371 |
| macro+rsi_ok+ob_inv+funding_inv | >=4 | 17 | 64.7% | +23.6pp | 0.33 |

## 7. Final Recommendation

### Original 5-factor checklist: WEAK

The original 5 factors (macro, ema_4h, coin_wr, entry_dist, rsi_ok) are mostly noise:
- **entry_dist** and **rsi_ok** have near-zero variance (246/246 and 242/246 pass) -- they filter nothing
- **coin_wr** and **ema_4h** are UNSTABLE across time splits -- likely overfit
- **MACRO is the only stable, predictive factor** (+11.4pp gap, stable across both halves)
- Score >=5 actually has LOWER WR (48.1%) than >=4 (58.3%) -- threshold >=5 is too strict and noisy

### Inverted factors matter

- **Funding contrarian is genuinely INVERTED**: -13.6pp gap. When funding suggests contrarian trade, it actually hurts.
- **OB is NOT inverted** on full data (+13.0pp gap) -- the earlier finding was likely sample noise.

### Best combo: macro + ema_4h + ob_inv + funding_inv (all 4 must pass)

- 18 trades, 66.7% WR, +$171 PnL, Sharpe 0.412
- Only positive-Sharpe combo found
- BUT: only 18 trades in 5.4 days -- low sample size, high overfitting risk

### Practical recommendation

1. **Use macro (BTC 7d trend) as primary gate** -- it is the only factor that is both predictive and stable
2. **Add funding_inv as secondary filter** -- funding contrarian being inverted is a strong signal (macro + funding_inv >= 2 gives 31 trades at 58.1% WR)
3. **Drop entry_dist, rsi_ok, coin_wr** -- they add no filtering value
4. **Keep ema_4h cautiously** -- positive overall but unstable across halves
5. **Do NOT use score >=5 threshold** -- it fails on full data

### Overfitting warning

All data spans only 5.4 days (Apr 11-17). Even the "stable" macro factor could be reflecting a single BTC regime (uptrend during this window). True validation requires at least 30+ days of data across different market conditions.
