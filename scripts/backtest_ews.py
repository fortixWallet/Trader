"""
Backtest Early Warning System on historical fortix_trades.
For each SL trade: what was the EWS score AT THE TIME of entry?
Would blocking that trade have saved money?
"""
import os, sys, sqlite3, time, json
import numpy as np
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DB_PATH = 'data/crypto/market.db'


def calc_rsi(closes, period=14):
    if len(closes) < period + 1:
        return 50.0
    prices = list(reversed(closes))
    deltas = np.diff(prices)
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)
    avg_gain = np.mean(gains[:period])
    avg_loss = np.mean(losses[:period])
    if avg_loss == 0:
        return 100.0
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def get_ews_at_time(conn, ts, coin='BTC'):
    """Calculate EWS components at a specific historical timestamp."""
    components = {}

    # 1. RSI at entry time
    prices = conn.execute(
        "SELECT close FROM prices WHERE coin=? AND timeframe='1h' "
        "AND timestamp <= ? ORDER BY timestamp DESC LIMIT 20", (coin, ts)
    ).fetchall()
    rsi = calc_rsi([p[0] for p in prices], 14) if len(prices) >= 15 else 50.0
    components['rsi'] = round(rsi, 1)

    rsi_score = 0
    rsi_blocked = None
    if rsi > 75:
        rsi_score = min(20, int((rsi - 70) * 2))
        rsi_blocked = 'LONG'
    elif rsi > 70:
        rsi_score = min(15, int((rsi - 65) * 1.5))
        rsi_blocked = 'LONG'
    elif rsi < 30:
        rsi_score = min(20, int((35 - rsi) * 2))
        rsi_blocked = 'SHORT'
    elif rsi < 35:
        rsi_score = min(15, int((40 - rsi) * 1.5))
        rsi_blocked = 'SHORT'
    components['rsi_score'] = rsi_score
    components['rsi_blocked'] = rsi_blocked

    # 2. OI divergence (4h before entry)
    lookback = ts - 4 * 3600
    oi = conn.execute(
        "SELECT oi_usdt FROM open_interest WHERE coin=? "
        "AND timestamp > ? AND timestamp <= ? ORDER BY timestamp ASC",
        (coin, lookback, ts)
    ).fetchall()
    p_range = conn.execute(
        "SELECT close FROM prices WHERE coin=? AND timeframe='1h' "
        "AND timestamp > ? AND timestamp <= ? ORDER BY timestamp ASC",
        (coin, lookback, ts)
    ).fetchall()

    oi_score = 0
    oi_dir = 'neutral'
    if len(oi) >= 2 and len(p_range) >= 2:
        price_chg = (p_range[-1][0] - p_range[0][0]) / p_range[0][0] * 100
        oi_chg = (oi[-1][0] - oi[0][0]) / oi[0][0] * 100
        if price_chg > 0.3 and oi_chg < -1.0:
            oi_score = min(20, int(abs(oi_chg) * 4))
            oi_dir = 'bearish_exhaustion'
        elif price_chg < -0.3 and oi_chg < -2.0:
            oi_score = min(20, int(abs(oi_chg) * 3))
            oi_dir = 'bullish_capitulation'
        elif abs(price_chg) < 0.3 and oi_chg > 3.0:
            oi_score = min(15, int(oi_chg * 2))
            oi_dir = 'pressure_building'
        components['oi_price_chg'] = round(price_chg, 2)
        components['oi_chg'] = round(oi_chg, 2)
    components['oi_score'] = oi_score
    components['oi_dir'] = oi_dir

    # 3. CVD divergence
    taker = conn.execute(
        "SELECT buy_volume, sell_volume FROM taker_volume "
        "WHERE coin=? AND period='1h' AND timestamp > ? AND timestamp <= ? ORDER BY timestamp ASC",
        (coin, lookback, ts)
    ).fetchall()

    cvd_score = 0
    cvd_dir = 'neutral'
    if len(taker) >= 2 and len(p_range) >= 2:
        price_chg = (p_range[-1][0] - p_range[0][0]) / p_range[0][0] * 100
        deltas = [t[0] - t[1] for t in taker]
        cvd = np.cumsum(deltas)
        cvd_trend = cvd[-1] - cvd[0]
        total_vol = sum(t[0] + t[1] for t in taker)
        cvd_pct = (cvd_trend / total_vol * 100) if total_vol > 0 else 0
        if price_chg > 0.3 and cvd_pct < -5:
            cvd_score = min(20, int(abs(cvd_pct) * 1.5))
            cvd_dir = 'bearish'
        elif price_chg < -0.3 and cvd_pct > 5:
            cvd_score = min(20, int(cvd_pct * 1.5))
            cvd_dir = 'bullish'
        components['cvd_pct'] = round(cvd_pct, 2)
    components['cvd_score'] = cvd_score

    # 4. Funding velocity (24h)
    fund_lookback = ts - 24 * 3600
    rates = conn.execute(
        "SELECT rate FROM funding_rates WHERE coin=? "
        "AND timestamp > ? AND timestamp <= ? ORDER BY timestamp ASC",
        (coin, fund_lookback, ts)
    ).fetchall()

    fund_score = 0
    if len(rates) >= 2:
        current = rates[-1][0]
        velocity = current - rates[0][0]
        if current > 0.0005 and velocity > 0.0002:
            fund_score = min(15, int(abs(velocity) * 100 * 50))
        elif current < -0.0005 and velocity < -0.0002:
            fund_score = min(15, int(abs(velocity) * 100 * 50))
        elif abs(current) > 0.001:
            fund_score = min(10, int(abs(current) * 100 * 30))
        components['funding'] = round(current * 100, 4)
        components['funding_vel'] = round(velocity * 100, 4)
    components['fund_score'] = fund_score

    # 5. Long/short crowding
    ls = conn.execute(
        "SELECT long_short_ratio FROM long_short_ratio "
        "WHERE coin=? AND period='1h' AND timestamp <= ? ORDER BY timestamp DESC LIMIT 1",
        (coin, ts)
    ).fetchone()

    crowd_score = 0
    if ls and ls[0]:
        ratio = ls[0]
        if ratio > 3.0: crowd_score = 15
        elif ratio > 2.5: crowd_score = 10
        elif ratio > 2.0: crowd_score = 5
        elif ratio < 0.33: crowd_score = 15
        elif ratio < 0.4: crowd_score = 10
        elif ratio < 0.5: crowd_score = 5
        components['ls_ratio'] = round(ratio, 2)
    components['crowd_score'] = crowd_score

    # 4H trend
    prices_4h = conn.execute(
        "SELECT close FROM prices WHERE coin=? AND timeframe='4h' "
        "AND timestamp <= ? ORDER BY timestamp DESC LIMIT 15", (coin, ts)
    ).fetchall()
    trend_4h = 'UNKNOWN'
    if len(prices_4h) >= 13:
        closes_4h = list(reversed([p[0] for p in prices_4h]))
        mult = 2.0 / 13
        ema = closes_4h[0]
        for c in closes_4h[1:]:
            ema = c * mult + ema * (1 - mult)
        trend_4h = 'UP' if closes_4h[-1] > ema else 'DOWN'
    components['trend_4h'] = trend_4h

    total = rsi_score + oi_score + cvd_score + fund_score + crowd_score
    components['total_score'] = min(100, total)

    return components


def main():
    conn = sqlite3.connect(DB_PATH)

    # Get all closed trades with timestamps
    trades = conn.execute("""
        SELECT coin, direction, pnl_usd, pnl_pct, exit_reason,
               created_at, filled_at, closed_at, confidence, reason
        FROM fortix_trades
        WHERE status = 'CLOSED' AND filled_at IS NOT NULL
        ORDER BY filled_at DESC
    """).fetchall()

    print(f"Total closed trades: {len(trades)}")

    # Convert filled_at to timestamp
    sl_trades = []
    tp_trades = []
    for t in trades:
        coin, direction, pnl_usd, pnl_pct, exit_reason = t[:5]
        created_at, filled_at, closed_at, conf, reason = t[5:]

        if not filled_at or not pnl_usd:
            continue

        try:
            import datetime
            clean = filled_at[:19].replace('T', ' ')
            ts = int(datetime.datetime.strptime(clean, '%Y-%m-%d %H:%M:%S').timestamp())
        except:
            continue

        trade = {
            'coin': coin, 'direction': direction, 'pnl_usd': pnl_usd,
            'pnl_pct': pnl_pct or 0, 'exit_reason': exit_reason or '',
            'ts': ts, 'filled_at': filled_at, 'conf': conf or 0, 'reason': reason or ''
        }

        if 'STOP_LOSS' in (exit_reason or ''):
            sl_trades.append(trade)
        elif pnl_usd and pnl_usd > 0:
            tp_trades.append(trade)

    print(f"SL trades: {len(sl_trades)} | Profitable trades: {len(tp_trades)}")
    print()

    # Analyze each SL trade
    sl_would_block = 0
    sl_saved = 0.0
    tp_would_block = 0
    tp_missed = 0.0

    rsi_block_sl = 0
    rsi_block_sl_usd = 0.0
    rsi_block_tp = 0
    rsi_block_tp_usd = 0.0

    oi_block_sl = 0
    cvd_block_sl = 0

    print("=" * 80)
    print("SL TRADES — EWS ANALYSIS")
    print("=" * 80)

    for t in sl_trades[:100]:  # last 100
        ews = get_ews_at_time(conn, t['ts'], 'BTC')
        score = ews['total_score']
        rsi = ews['rsi']
        blocked = ews.get('rsi_blocked')
        trend = ews.get('trend_4h', '?')

        would_block = False
        reasons = []

        # RSI block
        if blocked == t['direction']:
            would_block = True
            reasons.append(f"RSI={rsi} blocks {t['direction']}")
            rsi_block_sl += 1
            rsi_block_sl_usd += abs(t['pnl_usd'])

        # High score block (>60)
        if score >= 60:
            would_block = True
            reasons.append(f"score={score}>=60")

        # 4H trend conflict
        if trend == 'UP' and t['direction'] == 'SHORT':
            reasons.append("4H=UP vs SHORT")
        elif trend == 'DOWN' and t['direction'] == 'LONG':
            reasons.append("4H=DOWN vs LONG")

        if ews['oi_score'] > 0:
            oi_block_sl += 1
        if ews['cvd_score'] > 0:
            cvd_block_sl += 1

        if would_block:
            sl_would_block += 1
            sl_saved += abs(t['pnl_usd'])

        flag = "BLOCKED" if would_block else "      "
        print(f"  {flag} {t['direction']:5s} {t['coin']:8s} ${t['pnl_usd']:>7.2f} | "
              f"RSI={rsi:>5.1f} OI={ews['oi_score']:>2} CVD={ews['cvd_score']:>2} "
              f"Fund={ews['fund_score']:>2} Crowd={ews['crowd_score']:>2} "
              f"Score={score:>3} 4H={trend} | {' + '.join(reasons) if reasons else '-'}")

    # Check TP trades too — how many would we MISS?
    print()
    print("=" * 80)
    print("TP TRADES — WOULD WE MISS THEM?")
    print("=" * 80)

    for t in tp_trades[:80]:
        ews = get_ews_at_time(conn, t['ts'], 'BTC')
        score = ews['total_score']
        blocked = ews.get('rsi_blocked')

        would_block = False
        if blocked == t['direction']:
            would_block = True
            rsi_block_tp += 1
            rsi_block_tp_usd += t['pnl_usd']
        if score >= 60:
            would_block = True

        if would_block:
            tp_would_block += 1
            tp_missed += t['pnl_usd']
            print(f"  MISSED {t['direction']:5s} {t['coin']:8s} +${t['pnl_usd']:>7.2f} | "
                  f"RSI={ews['rsi']:>5.1f} Score={score:>3}")

    # Summary
    print()
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"SL trades analyzed: {len(sl_trades[:100])}")
    print(f"SL would block:     {sl_would_block} trades, saving ${sl_saved:.2f}")
    print(f"TP would miss:      {tp_would_block} trades, losing ${tp_missed:.2f}")
    print(f"NET IMPROVEMENT:    ${sl_saved - tp_missed:.2f}")
    print()
    print("BY COMPONENT:")
    print(f"  RSI blocks SL:    {rsi_block_sl} trades, ${rsi_block_sl_usd:.2f} saved")
    print(f"  RSI blocks TP:    {rsi_block_tp} trades, ${rsi_block_tp_usd:.2f} missed")
    print(f"  RSI NET:          ${rsi_block_sl_usd - rsi_block_tp_usd:.2f}")
    print(f"  OI divergence:    {oi_block_sl} SL trades had OI signal")
    print(f"  CVD divergence:   {cvd_block_sl} SL trades had CVD signal")

    # RSI distribution for SL trades
    print()
    print("RSI DISTRIBUTION (SL trades):")
    rsi_bins = {'<30': 0, '30-40': 0, '40-50': 0, '50-60': 0, '60-70': 0, '70-80': 0, '>80': 0}
    for t in sl_trades[:100]:
        ews = get_ews_at_time(conn, t['ts'], 'BTC')
        rsi = ews['rsi']
        if rsi < 30: rsi_bins['<30'] += 1
        elif rsi < 40: rsi_bins['30-40'] += 1
        elif rsi < 50: rsi_bins['40-50'] += 1
        elif rsi < 60: rsi_bins['50-60'] += 1
        elif rsi < 70: rsi_bins['60-70'] += 1
        elif rsi < 80: rsi_bins['70-80'] += 1
        else: rsi_bins['>80'] += 1
    for k, v in rsi_bins.items():
        bar = '█' * v
        print(f"  RSI {k:>5s}: {v:>3} {bar}")

    conn.close()


if __name__ == '__main__':
    main()
