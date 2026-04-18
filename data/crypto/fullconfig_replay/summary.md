# Full Config Replay: April 15-16 2026

## Config Applied

| Parameter | Value |
|---|---|
| MAX_PENDING | 8 |
| Active coins | 27 (excl LTC, TRX, JTO) |
| Per-coin R:R | From optimal_config.json |
| Per-coin SL mult | From optimal_config.json |
| Per-coin hold bars | From optimal_config.json (4h -> 1h) |
| Macro filter | BTC 7d>=+3% AND 1d>-3% blocks SHORT |
| News reactor | Impact 7-8: Profi HOLD, 9-10: auto-close |

### Apr 15

| Metric | Actual | Simulated | Delta |
|---|---|---|---|
| Trades | 20 | 11 | -9 |
| Wins | 12 | 8 | -4 |
| WR | 60% | 73% | |
| PnL | $-2.09 | $+248.38 | $+250.47 |
| Blocked | - | 9 | |

### Apr 16

| Metric | Actual | Simulated | Delta |
|---|---|---|---|
| Trades | 58 | 29 | -29 |
| Wins | 24 | 15 | -9 |
| WR | 41% | 52% | |
| PnL | $-67.85 | $+342.75 | $+410.60 |
| Blocked | - | 29 | |

### Combined

- **Actual**: $-69.94 (78 trades)
- **Simulated**: $+591.13 (40 trades)
- **Delta**: $+661.07
- **Blocked**: 38 trades (avoided $-272.60)

### Per-Coin Comparison

| Coin | Actual PnL | Sim PnL | Delta | R:R | SL_mult |
|---|---|---|---|---|---|
| ADA | $+2.41 | $+0.74 | $-1.67 | 2.5 | 0.7 |
| AVAX | $+75.20 | $+147.04 | $+71.84 | 2.5 | 0.7 |
| BNB | $-15.12 | $-5.20 | $+9.92 | 2.0 | 0.5 |
| BTC | $-28.76 | $-42.54 | $-13.78 | 1.5 | 1.0 |
| CRV | $-96.56 | $-73.18 | $+23.38 | 1.2 | 0.8 |
| ETH | $+37.98 | $+26.54 | $-11.44 | 1.2 | 0.6 |
| LDO | $-10.87 | $+210.39 | $+221.26 | 1.5 | 1.0 |
| LINK | $-8.78 | $-12.68 | $-3.90 | 2.5 | 0.5 |
| LTC | $-19.52 | $+0.00 | $+19.52 | 1.0 | 1.0 |
| PENDLE | $+37.32 | $+243.87 | $+206.55 | 2.5 | 0.8 |
| POL | $-35.82 | $+98.21 | $+134.03 | 1.2 | 1.2 |
| SOL | $-13.33 | $-23.64 | $-10.31 | 2.0 | 0.8 |
| UNI | $+10.31 | $+21.58 | $+11.27 | 1.2 | 0.8 |
| XRP | $-4.40 | $+0.00 | $+4.40 | 2.5 | 0.6 |
