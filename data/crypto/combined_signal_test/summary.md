# Combined Signal Test Results
**Period:** Mar 27 - Apr 17, 2026 (21 days) | **Coins:** 22 | **Observations:** 10,530

## 1. Win Rate by Combined Score

| Score | Count | WR% | Avg 4h Return |
|------:|------:|----:|---------------:|
| -3 | 1,325 | 55.4% | -0.044% (SHORT pred) |
| -2 | 627 | 62.2% | -0.321% (SHORT pred) |
| -1 | 2,640 | 52.8% | -0.023% (SHORT pred) |
| +0 | 644 | 0.0% | -0.330% |
| +1 | 2,789 | 46.2% | +0.083% (LONG pred) |
| +2 | 397 | 45.8% | +0.239% (LONG pred) |
| +3 | 1,848 | 54.8% | +0.327% (LONG pred) |
| +4 | 226 | 47.8% | +0.034% (LONG pred) |
| +5 | 34 | 64.7% | +0.380% (LONG pred) |

## 2. Individual Signal Win Rates

| Signal | Fired | WR% |
|--------|------:|----:|
| momentum | 10,530 | 58.2% |
| ls_contrarian | 9,799 | 52.3% |
| macro | 9,650 | 49.4% |
| ema | 10,530 | 48.9% |
| funding_contrarian | 152 | 47.4% |
| fng | 10,530 | 46.9% |
| oi_change | 289 | 43.6% |
| sopr | 0 | 0.0% |

## 3. Combined Signal at Thresholds

| Threshold | Long (n) | Long WR | Short (n) | Short WR | Total (n) | Total WR | Avg Ret | Trades/Day |
|----------:|---------:|--------:|----------:|---------:|----------:|---------:|--------:|-----------:|
| >= 2 | 2,505 | 52.9% | 1,952 | 57.6% | 4,457 | 54.9% | +0.219% | 212.2 |
| >= 3 | 2,108 | 54.2% | 1,325 | 55.4% | 3,433 | 54.6% | +0.199% | 163.5 |
| >= 4 | 260 | 50.0% | 0 | 0.0% | 260 | 50.0% | +0.079% | 12.4 |
| >= 5 | 34 | 64.7% | 0 | 0.0% | 34 | 64.7% | +0.380% | 1.6 |
| >= 6 | 0 | 0.0% | 0 | 0.0% | 0 | 0.0% | +0.000% | 0.0 |

## 4. Optimal Signal Subsets (threshold >= 3)

| Size | Signals | WR% | Trades |
|-----:|---------|----:|-------:|
| 4 | ls_contrarian, funding_contrarian, oi_change, momentum | 58.5% | 82 |
| 5 | ls_contrarian, funding_contrarian, oi_change, sopr, momentum | 58.5% | 82 |
| 6 | macro, ema, ls_contrarian, funding_contrarian, sopr, momentum | 57.6% | 2,092 |
| 7 | macro, ema, ls_contrarian, funding_contrarian, oi_change, sopr, momentum | 56.7% | 2,212 |
| 8 | macro, ema, ls_contrarian, funding_contrarian, oi_change, fng, sopr, momentum | 54.6% | 3,433 |

## 5. Optimal Signal Subsets (threshold >= 2)

| Size | Signals | WR% | Trades |
|-----:|---------|----:|-------:|
| 4 | ls_contrarian, funding_contrarian, sopr, momentum | 60.4% | 4,902 |
| 5 | ls_contrarian, funding_contrarian, oi_change, sopr, momentum | 60.3% | 4,901 |
| 6 | ema, ls_contrarian, funding_contrarian, oi_change, sopr, momentum | 57.9% | 3,676 |
| 7 | macro, ls_contrarian, funding_contrarian, oi_change, fng, sopr, momentum | 56.5% | 4,957 |
| 8 | macro, ema, ls_contrarian, funding_contrarian, oi_change, fng, sopr, momentum | 54.9% | 4,457 |

## 6. Recommendation

**Best subset:** ls_contrarian, funding_contrarian, sopr, momentum (4 signals)
**Win rate:** 60.4% over 4,902 trades

**Best all-8 threshold:** >= 5 → WR 64.7% (34 trades, 1.6/day)

**Best individual signal:** momentum → WR 58.2% (10,530 fires)
**Combined advantage:** +6.5pp over best individual
