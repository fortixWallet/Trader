# Trade Postmortem: Apr 15-17 2026

**Total trades: 117 | Net PnL: $-352.04**

## Category Distribution

| Cat | Description | Count | % | Total PnL |
|-----|------------|-------|---|-----------|
| A | Winners (TP hit) | 22 | 19% | $+600.68 |
| B | Could-have-won (trailing saves) | 25 | 21% | $-566.53 |
| C | Wrong direction (no fix) | 16 | 14% | $-363.35 |
| D | Too tight SL | 2 | 2% | $-43.51 |
| E | Too wide TP | 9 | 8% | $-281.61 |
| F | TIME_EXIT OK | 31 | 26% | $+418.91 |
| G | TIME_EXIT lost | 12 | 10% | $-116.63 |

## Top COULD-HAVE-WON Trades (Category B)

| Coin | Dir | Entry | Peak ROI | Peak Min | Actual ROI | PnL | Trail PnL | Saved |
|------|-----|-------|----------|----------|------------|-----|-----------|-------|
| POL | LONG | $0.0831 | +55.8% | 1855m | -4.8% | $-20.25 | $+8.46 | $+28.71 |
| AVAX | LONG | $9.71 | +14.8% | -22m | -10.8% | $-40.08 | $+25.92 | $+66.00 |
| SOL | SHORT | $85.27 | +13.8% | 56m | -4.0% | $-17.03 | $+24.11 | $+41.14 |
| ADA | SHORT | $0.2496 | +13.5% | 114m | -6.1% | $-23.42 | $+7.85 | $+31.27 |
| PENDLE | SHORT | $1.3305 | +13.4% | -9m | -12.3% | $-42.37 | $+21.71 | $+64.08 |
| BTC | SHORT | $74290.0 | +10.6% | 105m | -3.3% | $-13.81 | $+18.62 | $+32.43 |
| LDO | SHORT | $0.3968 | +10.4% | 1m | -2.5% | $-11.39 | $+16.12 | $+27.51 |
| PENDLE | SHORT | $1.1622 | +9.8% | 12m | -8.4% | $-31.51 | $+12.29 | $+43.80 |
| ETH | LONG | $2352.84 | +9.4% | 43m | -1.0% | $-6.48 | $+6.40 | $+12.88 |
| AVAX | SHORT | $9.42 | +9.3% | 89m | -5.9% | $-22.60 | $+5.94 | $+28.54 |

## Trailing Stop Simulation Results

- Activation: after +3% ROI
- Trail: close at 50% of peak ROI
- B trades with trailing sim: 25
- **Total saved by trailing: $+875.66**

## Money Left on Table

| Source | Amount | Fix |
|--------|--------|-----|
| B: Trailing stop would save | $+875.66 | Add trailing stop (activate +3%, trail 50%) |
| E: Tighter TP would capture | $+331.55 | Reduce TP distance or use partial TP |
| D: Wider SL would save | $-43.51 lost | Widen SL for high-conviction setups |
| C: Unfixable (wrong direction) | $-363.35 | Better signal filtering |
| G: TIME_EXIT losses | $-116.63 | Earlier exits / tighter TIME_EXIT |

## All Trades Detail

| # | Coin | Dir | Lev | Entry | Exit Reason | ROI | PnL | Peak ROI | Peak@min | Cat |
|---|------|-----|-----|-------|-------------|-----|-----|----------|----------|-----|
| 1 | BR | S | 5x | $0 | STOP_LOSS | -5.6% | $-23.41 | +0.0% | 0m | C |
| 2 | 4 | L | 5x | $0 | STOP_LOSS | -14.5% | $-54.37 | +0.0% | 0m | C |
| 3 | ZAMA | S | 5x | $0 | STOP_LOSS | -3.3% | $-12.75 | +0.0% | 0m | C |
| 4 | POL | S | 8x | $0 | TARGET_HIT | +3.8% | $+1.98 | +0.0% | 0m | A |
| 5 | WIF | S | 8x | $0 | TARGET_HIT | +3.2% | $+1.64 | +0.0% | 0m | A |
| 6 | PENDLE | S | 8x | $1.046 | STOP_LOSS | -7.0% | $-26.87 | +3.1% | -18m | E |
| 7 | POL | S | 8x | $0 | STOP_LOSS | -5.3% | $-21.20 | +0.0% | 0m | C |
| 8 | BNB | S | 8x | $615.4 | STOP_LOSS | -3.9% | $-15.93 | +2.1% | -7m | E |
| 9 | AVAX | L | 8x | $9.311 | TIME_EXIT | +4.3% | $+15.05 | +6.8% | -32m | F |
| 10 | LINK | L | 8x | $8.9878 | TIME_EXIT | +8.3% | $+28.92 | +11.8% | 150m | F |
| 11 | ETH | L | 8x | $2313.29 | TIME_EXIT | +7.0% | $+24.43 | +10.2% | 90m | F |
| 12 | LDO | L | 8x | $0.3504 | TIME_EXIT | +3.0% | $+10.40 | +8.9% | 100m | F |
| 13 | BTC | L | 10x | $74074.1 | TIME_EXIT | +6.3% | $+21.83 | +9.6% | 160m | F |
| 14 | SOL | S | 8x | $83.61 | TIME_EXIT | -1.3% | $-4.69 | +4.6% | 164m | G |
| 15 | XRP | S | 8x | $1.3624 | TIME_EXIT | +4.5% | $+15.62 | +4.2% | 140m | F |
| 16 | ADA | S | 8x | $0.2405 | TIME_EXIT | +4.0% | $+13.97 | +5.3% | 113m | F |
| 17 | ADA | S | 8x | $0.2399 | STOP_LOSS | -6.0% | $-23.34 | +5.7% | 147m | B |
| 18 | PENDLE | S | 8x | $1.0502 | TIME_EXIT | +0.7% | $+2.41 | +9.3% | 227m | F |
| 19 | BTC | L | 8x | $74351.0 | TIME_EXIT | -3.5% | $-12.44 | +3.7% | -27m | G |
| 20 | AVAX | L | 8x | $9.311 | TIME_EXIT | +4.0% | $+14.19 | +11.9% | 88m | F |
| 21 | CRV | L | 8x | $0.2182 | STOP_LOSS | -5.9% | $-23.82 | +2.2% | -13m | D |
| 22 | LINK | S | 8x | $9.0638 | TIME_EXIT | +3.1% | $+10.82 | +6.5% | 183m | F |
| 23 | ETH | S | 8x | $2329.93 | TIME_EXIT | +4.6% | $+15.92 | +7.4% | 178m | F |
| 24 | CAKE | S | 8x | $0 | TARGET_HIT | +8.2% | $+25.55 | +0.0% | 0m | A |
| 25 | AVAX | L | 8x | $0 | STOP_LOSS | -7.7% | $-30.53 | +0.0% | 0m | C |
| 26 | SUI | L | 8x | $0 | STOP_LOSS | -8.6% | $-33.47 | +0.0% | 0m | C |
| 27 | BNB | L | 8x | $613.95 | TARGET_HIT | +0.8% | $+0.60 | +17.8% | 817m | A |
| 28 | WLD | L | 8x | $0 | STOP_LOSS | -0.3% | $-4.05 | +0.0% | 0m | C |
| 29 | ETH | L | 8x | $2352.84 | STOP_LOSS | -1.0% | $-6.48 | +9.4% | 43m | B |
| 30 | BTC | L | 8x | $74632.1 | STOP_LOSS | -0.4% | $-4.43 | +8.5% | 43m | B |
| 31 | ENA | L | 8x | $0 | STOP_LOSS | -10.2% | $-39.34 | +0.0% | 0m | C |
| 32 | CRV | L | 8x | $0.2235 | STOP_LOSS | -7.5% | $-28.39 | +5.4% | -26m | B |
| 33 | LDO | S | 7x | $0.3748 | STOP_LOSS | -13.3% | $-48.35 | +3.0% | -15m | E |
| 34 | LINK | L | 8x | $9.26 | TIME_EXIT | -1.4% | $-4.83 | +6.9% | 179m | G |
| 35 | ETH | L | 8x | $2355.93 | TIME_EXIT | -1.7% | $-5.93 | +4.6% | 164m | G |
| 36 | BNB | L | 8x | $622.05 | TIME_EXIT | +0.3% | $+1.12 | +5.3% | 164m | F |
| 37 | PENDLE | L | 8x | $1.0866 | TARGET_HIT | +17.4% | $+58.92 | +31.9% | 113m | A |
| 38 | ADA | L | 8x | $0.2453 | TIME_EXIT | +8.8% | $+30.82 | +15.3% | 204m | F |
| 39 | UNI | L | 8x | $3.1993 | TARGET_HIT | +12.3% | $+40.75 | +13.2% | 53m | A |
| 40 | AVAX | L | 8x | $9.3506 | TARGET_HIT | +12.7% | $+42.43 | +17.1% | 173m | A |
| 41 | BTC | L | 8x | $74572.0 | TIME_EXIT | +4.0% | $+14.06 | +7.5% | -16m | F |
| 42 | BTC | L | 8x | $0 | TARGET_HIT | +3.7% | $+11.11 | +0.0% | 0m | A |
| 43 | PENDLE | S | 7x | $1.1405 | STOP_LOSS | -6.9% | $-25.91 | +4.0% | -36m | E |
| 44 | CRV | L | 8x | $0.2253 | STOP_LOSS | -3.2% | $-14.31 | +1.8% | -36m | C |
| 45 | LINK | L | 8x | $9.312 | STOP_LOSS | -4.7% | $-19.69 | +5.0% | -36m | D |
| 46 | BTC | L | 8x | $74998.5 | STOP_LOSS | -3.8% | $-16.50 | +1.5% | -36m | C |
| 47 | BNB | S | 8x | $624.36 | TIME_EXIT | +3.5% | $+12.42 | +4.3% | 144m | F |
| 48 | ETH | S | 8x | $2353.01 | TIME_EXIT | +4.4% | $+15.37 | +6.2% | 144m | F |
| 49 | ADA | L | 8x | $0.2497 | TIME_EXIT | -4.5% | $-15.84 | +6.7% | 84m | G |
| 50 | AVAX | L | 8x | $9.487 | TIME_EXIT | -2.7% | $-9.50 | +11.2% | 84m | G |
| 51 | SOL | L | 8x | $0 | STOP_LOSS | -1.4% | $-7.18 | +0.0% | 0m | C |
| 52 | LDO | L | 7x | $0.3481 | TARGET_HIT | +25.1% | $+84.58 | +104.4% | 1624m | A |
| 53 | PENDLE | S | 8x | $1.1622 | STOP_LOSS | -8.4% | $-31.51 | +9.8% | 12m | B |
| 54 | POL | S | 8x | $0.0869 | STOP_LOSS | -5.1% | $-20.81 | +0.0% | -44m | C |
| 55 | LDO | S | 7x | $0.3884 | STOP_LOSS | -9.9% | $-37.43 | +6.5% | -14m | B |
| 56 | CRV | S | 8x | $0.222 | STOP_LOSS | -4.3% | $-18.16 | +2.9% | -13m | E |
| 57 | ETH | L | 8x | $2335.0 | PROFI_EXIT | -2.0% | $-7.00 | +5.0% | 116m | G |
| 58 | BTC | S | 8x | $74290.0 | STOP_LOSS | -3.3% | $-13.81 | +10.6% | 105m | B |
| 59 | LINK | S | 8x | $9.229 | STOP_LOSS | -5.1% | $-20.96 | +6.8% | 166m | B |
| 60 | AVAX | S | 8x | $9.42 | STOP_LOSS | -5.9% | $-22.60 | +9.3% | 89m | B |
| 61 | XRP | S | 8x | $1.4098 | STOP_LOSS | -4.8% | $-20.02 | +6.0% | 167m | B |
| 62 | BNB | S | 8x | $619.4 | STOP_LOSS | -2.7% | $-12.52 | +5.8% | 167m | B |
| 63 | UNI | S | 7x | $3.264 | STOP_LOSS | -3.9% | $-16.20 | +5.6% | 106m | B |
| 64 | PENDLE | L | 8x | $1.1669 | TARGET_HIT | +16.9% | $+56.73 | +28.9% | 46m | A |
| 65 | ADA | S | 8x | $0.2496 | STOP_LOSS | -6.1% | $-23.42 | +13.5% | 114m | B |
| 66 | POL | L | 8x | $0.0831 | STOP_LOSS | -4.8% | $-20.25 | +55.8% | 1855m | B |
| 67 | POL | L | 8x | $0.0879 | NEWS_REACTION | +0.6% | $+2.26 | +9.1% | 46m | F |
| 68 | CRV | S | 8x | $0.2253 | NEWS_REACTION | +1.8% | $+6.20 | +11.7% | 1m | F |
| 69 | LDO | L | 7x | $0.3861 | NEWS_REACTION | -2.5% | $-8.68 | +62.0% | 481m | G |
| 70 | PENDLE | S | 7x | $1.1664 | NEWS_REACTION | +0.2% | $+0.63 | +11.0% | 1m | F |
| 71 | ETH | S | 8x | $2303.53 | NEWS_REACTION | -0.2% | $-0.65 | +6.4% | 1m | G |
| 72 | LDO | S | 7x | $0.3968 | STOP_LOSS | -2.5% | $-11.39 | +10.4% | 1m | B |
| 73 | CRV | S | 8x | $0 | TARGET_HIT | +1.8% | $+3.13 | +0.0% | 0m | A |
| 74 | POL | S | 7x | $0.0875 | TARGET_HIT | +0.8% | $+0.11 | +9.6% | 1m | A |
| 75 | SOL | S | 8x | $85.27 | STOP_LOSS | -4.0% | $-17.03 | +13.8% | 56m | B |
| 76 | BNB | S | 8x | $617.5 | STOP_LOSS | -2.7% | $-12.51 | +2.9% | -36m | E |
| 77 | UNI | S | 8x | $3.264 | STOP_LOSS | -3.2% | $-14.24 | +6.6% | -36m | B |
| 78 | AVAX | S | 8x | $9.35 | STOP_LOSS | -7.4% | $-28.12 | +7.7% | -38m | B |
| 79 | CRV | S | 8x | $0.2242 | STOP_LOSS | -4.3% | $-18.08 | +7.8% | -36m | B |
| 80 | BTC | S | 8x | $73921.9 | STOP_LOSS | -4.1% | $-17.47 | +5.4% | -36m | B |
| 81 | LINK | S | 8x | $9.277 | PROFI_EXIT | -3.6% | $-12.64 | +4.9% | -9m | G |
| 82 | ADA | L | 8x | $0.2529 | NEWS_REACTION | +5.7% | $+20.22 | +7.9% | 7m | F |
| 83 | SOL | S | 8x | $84.4 | NEWS_REACTION | +1.7% | $+5.87 | +5.7% | -59m | F |
| 84 | AVAX | L | 8x | $9.586 | NEWS_REACTION | +1.4% | $+5.03 | +5.3% | 7m | F |
| 85 | LINK | L | 8x | $9.418 | NEWS_REACTION | +2.7% | $+9.60 | +6.1% | 7m | F |
| 86 | BNB | L | 8x | $624.4 | NEWS_REACTION | +3.3% | $+11.70 | +4.6% | 7m | F |
| 87 | PENDLE | L | 7x | $1.2129 | NEWS_REACTION | +0.8% | $+2.92 | +9.9% | 66m | F |
| 88 | POL | L | 8x | $0.0885 | NEWS_REACTION | +0.8% | $+2.87 | +7.2% | 66m | F |
| 89 | ETH | L | 8x | $2338.32 | NEWS_REACTION | +0.7% | $+2.32 | +3.9% | -54m | F |
| 90 | SOL | L | 8x | $86.62 | TARGET_HIT | +1.7% | $+2.52 | +4.7% | -53m | A |
| 91 | AVAX | L | 8x | $9.53 | TARGET_HIT | +17.7% | $+58.72 | +28.5% | 74m | A |
| 92 | LTC | L | 8x | $56.24 | STOP_LOSS | -5.0% | $-19.52 | +0.0% | 0m | C |
| 93 | TON | L | 8x | $1.4143 | TARGET_HIT | +13.0% | $+42.20 | +0.0% | 0m | A |
| 94 | ETH | L | 8x | $2349.21 | STOP_LOSS | -6.5% | $-26.26 | +5.3% | -33m | B |
| 95 | BNB | L | 8x | $634.4 | STOP_LOSS | -4.5% | $-19.62 | +2.8% | -13m | E |
| 96 | LDO | L | 7x | $0.4441 | STOP_LOSS | -21.1% | $-77.75 | +2.7% | -13m | E |
| 97 | ADA | L | 8x | $0.2587 | STOP_LOSS | -9.3% | $-36.51 | +5.0% | -13m | E |
| 98 | SOL | L | 8x | $89.0 | STOP_LOSS | -8.6% | $-32.55 | +6.9% | -22m | B |
| 99 | AVAX | L | 8x | $9.71 | STOP_LOSS | -10.8% | $-40.08 | +14.8% | -22m | B |
| 100 | LINK | L | 8x | $9.513 | STOP_LOSS | -9.8% | $-37.58 | +9.0% | 47m | B |
| 101 | PENDLE | L | 7x | $1.2737 | TIME_EXIT | -6.5% | $-23.78 | +15.0% | 47m | G |
| 102 | CRV | L | 8x | $0.2315 | TIME_EXIT | +2.4% | $+8.47 | +11.1% | 219m | F |
| 103 | UNI | S | 7x | $3.4019 | TIME_EXIT | +10.0% | $+35.28 | +10.7% | 315m | F |
| 104 | XRP | S | 8x | $1.449 | TIME_EXIT | +11.2% | $+39.22 | +14.6% | 195m | F |
| 105 | BTC | L | 10x | $74886.0 | TIME_EXIT | -3.0% | $-10.65 | +3.9% | 8m | G |
| 106 | BTC | L | 10x | $0 | STOP_LOSS | -3.0% | $-13.35 | +0.0% | 0m | C |
| 107 | XRP | S | 8x | $0 | TARGET_HIT | +11.2% | $+36.33 | +0.0% | 0m | A |
| 108 | UNI | S | 7x | $0 | TARGET_HIT | +10.0% | $+32.84 | +0.0% | 0m | A |
| 109 | CRV | L | 8x | $0 | TARGET_HIT | +2.4% | $+6.08 | +0.0% | 0m | A |
| 110 | PENDLE | L | 7x | $0 | STOP_LOSS | -6.5% | $-25.95 | +0.0% | 0m | C |
| 111 | NEAR | S | 8x | $1.4081 | TIME_EXIT | +5.9% | $+18.97 | +0.0% | 0m | F |
| 112 | TON | L | 8x | $1.424 | TARGET_HIT | +9.0% | $+27.23 | +0.0% | 0m | A |
| 113 | ADA | L | 8x | $0.2517 | TARGET_HIT | +6.0% | $+18.30 | +26.1% | 95m | A |
| 114 | BTC | L | 8x | $74588.0 | TARGET_HIT | +6.6% | $+20.05 | +19.1% | 303m | A |
| 115 | XLM | S | 8x | $0.167 | STOP_LOSS | -7.7% | $-26.61 | +0.0% | 0m | C |
| 116 | ETH | L | 8x | $2347.27 | TARGET_HIT | +9.1% | $+28.88 | +10.1% | -9m | A |
| 117 | PENDLE | S | 7x | $1.3305 | STOP_LOSS | -12.3% | $-42.37 | +13.4% | -9m | B |

## Recommendations

1. **Implement trailing stop** — Biggest single improvement. Activate after +3% ROI, trail at 50% of peak.
2. **Tighter TP for weak setups** — Trades reaching 50-80% of TP often reverse. Consider partial exits.
3. **Filter wrong-direction trades** — C-category trades lost with no recovery. Stricter entry criteria needed.
4. **TIME_EXIT improvements** — Review G-category trades for earlier exit signals.
