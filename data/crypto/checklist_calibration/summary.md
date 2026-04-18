# Checklist Calibration Report — Apr 11-17, 2026

**Trades analyzed**: 101 closed trades over 6 days
**Overall WR**: 47.5% | **Total PnL**: $-129.72

## Score Distribution

| Score | Trades | WR% | Avg PnL | Total PnL |
|------:|-------:|----:|--------:|----------:|
| 2 | 6 | 33.3% | $-2.36 | $-14.19 |
| 3 | 23 | 43.5% | $-4.21 | $-96.86 |
| 4 | 47 | 44.7% | $-4.79 | $-225.11 |
| 5 | 21 | 57.1% | $+6.81 | $+142.94 |
| 6 | 4 | 75.0% | $+15.87 | $+63.49 |

## Best Threshold

**Recommended minimum score: 5** (maximizes total PnL from filtered trades)

## Profi Confidence vs Checklist

- Pearson correlation: **0.019**
- Profi assigns narrow range (50%-78%), checklist gives wider spread

## Per-Factor Analysis (most predictive first)

| Factor | YES(n) | YES WR | NO(n) | NO WR | Delta WR |
|--------|-------:|-------:|------:|------:|---------:|
| coin_wr | 31 | 71.0% | 70 | 37.1% | +33.8pp |
| ob | 67 | 40.3% | 34 | 61.8% | -21.5pp |
| ema_4h | 48 | 54.2% | 44 | 36.4% | +17.8pp |
| funding | 45 | 40.0% | 56 | 53.6% | -13.6pp |
| macro | 44 | 54.5% | 57 | 42.1% | +12.4pp |
| rsi | 85 | 44.7% | 7 | 57.1% | -12.4pp |
| entry_dist | 63 | 47.6% | 29 | 41.4% | +6.2pp |

## Verdict

The checklist **improves** trade quality. Trades scoring >=5 had 60.0% WR vs 43.4% for lower scores (+16.6pp). Filtering would have saved $-336.16 in losses from low-quality setups.
