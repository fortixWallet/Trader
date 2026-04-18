# Backtest: April 17, 2026 — NEW Prompt System vs ACTUAL

## NEW vs ACTUAL Comparison

| Metric | NEW Prompt | ACTUAL (OKX) |
|--------|-----------|--------------|
| Total trades | 2 | 10 |
| Win rate | 100.0% | 30.0% |
| Net PnL | $+23.85 | $-121.48 |
| SL hits | 0 | 8 |
| Skipped scans | 5/8 | 0/8 |

## NEW Prompt Details
- **Start equity**: $5,000.00
- **End equity**: $5,023.85
- **Net PnL**: $+23.85 (+0.48%)
- **Trades**: 2 (2W / 0L)
- **Leverage**: 8x | SL: -6.5% ROI | TP: 13% ROI
- **Trailing**: activate +6%, drop -2%
- **Scans**: 8 (every 2h, 00:00-14:00 UTC) | API calls: 8
- **Tokens**: 7,989 in / 6,485 out
- **Est. cost**: $0.61
- **Banned coins** (2+ SL): None

## MACRO Citation Analysis
- Setups citing MACRO: 4/4 (100%)
- Skipped scans (empty []): 5/8

## Exit Type Breakdown (NEW)
| Exit | Count | PnL |
|------|-------|-----|
| EOD | 2 | $+23.85 |

## Per-Coin Comparison
| Coin | NEW trades | NEW PnL | ACTUAL trades | ACTUAL PnL |
|------|-----------|---------|---------------|------------|
| APT | 0 | $+0.00 | 1 | $-21.43 |
| AVAX | 0 | $+0.00 | 1 | $-22.35 |
| BTC | 1 | $+13.14 | 1 | $-27.17 |
| ETH | 1 | $+10.71 | 1 | $+28.88 |
| FIL | 0 | $+0.00 | 1 | $-23.41 |
| LDO | 0 | $+0.00 | 1 | $-24.27 |
| LINK | 0 | $+0.00 | 1 | $-23.07 |
| NEAR | 0 | $+0.00 | 1 | $+18.97 |
| PENDLE | 0 | $+0.00 | 1 | $-42.37 |
| SUI | 0 | $+0.00 | 1 | $+14.74 |

## Key Questions

### Did NEW skip when it should (FLAT regime)?
Skipped 5 out of 8 scans. The actual system NEVER skipped and had 8 SL hits.
YES - NEW correctly skipped uncertain conditions.

### Did NEW cite MACRO in reasons?
100% of setups cited at least one MACRO factor. Target: 100%.

### Was SL ban effective?
Banned coins: None.
No coin hit 2+ SL (good discipline or few trades).

## Recommendation
**DEPLOY** - NEW prompt outperformed ACTUAL by $+145.33.
Fewer trades (2 vs 10), better selectivity. MACRO-driven decisions work.
