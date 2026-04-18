"""
Test Guardian v3: consults derivatives data before cancelling.
v2: BTC -0.5% → cancel all LONGs (blind)
v3: BTC -0.5% → check OI, CVD, taker → is this reversal or noise? → then decide

Test on Apr 14-18 2026: when would v3 cancel vs v2, and who's right?
"""
import os, sys, sqlite3, numpy as np
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = 'data/crypto/market.db'


def nearest(conn, table, col, coin, ts, w=7200):
    r = conn.execute(
        f'SELECT {col} FROM {table} WHERE coin=? AND timestamp BETWEEN ? AND ? ORDER BY ABS(timestamp-?) LIMIT 1',
        (coin, ts-w, ts+w, ts)
    ).fetchone()
    return float(r[0]) if r and r[0] is not None else None


def guardian_v2_decision(btc_change):
    """V2: blind — just BTC move."""
    if btc_change < -0.5:
        return 'CANCEL_LONGS', f'BTC {btc_change:+.2f}%'
    if btc_change > 0.5:
        return 'CANCEL_SHORTS', f'BTC {btc_change:+.2f}%'
    return 'HOLD', ''


def guardian_v3_decision(conn, ts, btc_change):
    """V3: consult derivatives before deciding."""

    # Get derivatives context
    oi_now = nearest(conn, 'pred_oi_history', 'c', 'BTC', ts)
    oi_4h = nearest(conn, 'pred_oi_history', 'c', 'BTC', ts - 14400)
    oi_chg = (oi_now / oi_4h - 1) * 100 if oi_now and oi_4h and oi_4h > 0 else 0

    cvd_now = nearest(conn, 'pred_cvd_futures', 'cvd', 'BTC', ts)
    cvd_1h = nearest(conn, 'pred_cvd_futures', 'cvd', 'BTC', ts - 3600)
    cvd_trend = 'UP' if cvd_now and cvd_1h and cvd_now > cvd_1h else 'DOWN'

    tk = nearest(conn, 'pred_taker_volume', 'ratio', 'BTC', ts) or 1.0

    lq = conn.execute(
        'SELECT long_liq_usd, short_liq_usd FROM pred_liq_history WHERE coin=? AND timestamp BETWEEN ? AND ? ORDER BY ABS(timestamp-?) LIMIT 1',
        ('BTC', ts-7200, ts+7200, ts)
    ).fetchone()
    liq_ratio = (float(lq[0]) - float(lq[1])) / (float(lq[0]) + float(lq[1])) if lq and (float(lq[0]) + float(lq[1])) > 0 else 0

    reasons = []
    reversal_score = 0

    if btc_change < -0.5:
        # BTC dropping — should we cancel LONGs?

        # OI dropping too = deleveraging = reversal likely
        if oi_chg < -1:
            reversal_score += 3
            reasons.append(f'OI dropping {oi_chg:+.1f}% = deleveraging')

        # CVD also falling = real selling
        if cvd_trend == 'DOWN':
            reversal_score += 2
            reasons.append('CVD falling = real selling')

        # Taker selling dominant
        if tk < 0.9:
            reversal_score += 2
            reasons.append(f'taker SELL dominant {tk:.2f}')

        # Longs getting liquidated = cascade risk
        if liq_ratio > 0.5:
            reversal_score += 2
            reasons.append('longs getting liquidated')

        # COUNTER: OI rising during drop = just a shakeout
        if oi_chg > 1:
            reversal_score -= 3
            reasons.append(f'OI RISING {oi_chg:+.1f}% = accumulation, just shakeout')

        # COUNTER: CVD rising = buyers absorbing
        if cvd_trend == 'UP':
            reversal_score -= 2
            reasons.append('CVD rising = buyers absorbing dip')

        # COUNTER: Taker buying = dip buyers
        if tk > 1.1:
            reversal_score -= 2
            reasons.append(f'taker BUY {tk:.2f} = dip buying')

        if reversal_score >= 3:
            return 'CANCEL_LONGS', f'REVERSAL (score={reversal_score}): {"; ".join(reasons)}'
        else:
            return 'HOLD', f'NOISE (score={reversal_score}): {"; ".join(reasons)}'

    elif btc_change > 0.5:
        # BTC rising — should we cancel SHORTs?

        if oi_chg < -1:
            reversal_score += 3
            reasons.append(f'OI dropping despite rally = exhaustion')

        if cvd_trend == 'UP':
            reversal_score += 2
            reasons.append('CVD rising = real buying')

        if tk > 1.1:
            reversal_score += 2
            reasons.append(f'taker BUY {tk:.2f}')

        if liq_ratio < -0.5:
            reversal_score += 2
            reasons.append('shorts getting liquidated')

        # COUNTER
        if oi_chg > 1 and cvd_trend == 'DOWN':
            reversal_score -= 3
            reasons.append('OI rising but CVD falling = trap')

        if reversal_score >= 3:
            return 'CANCEL_SHORTS', f'REVERSAL (score={reversal_score}): {"; ".join(reasons)}'
        else:
            return 'HOLD', f'NOISE (score={reversal_score}): {"; ".join(reasons)}'

    return 'HOLD', ''


def main():
    conn = sqlite3.connect(DB_PATH)

    ts_start = 1776124800  # Apr 14 2026
    ts_end = 1776470400    # Apr 18 2026

    # Simulate BTC 15-min windows every hour
    btc_prices = conn.execute(
        "SELECT timestamp, close FROM prices WHERE coin='BTC' AND timeframe='1h' "
        "AND timestamp >= ? AND timestamp <= ? ORDER BY timestamp",
        (ts_start, ts_end)
    ).fetchall()

    print("=" * 90)
    print("  GUARDIAN v2 vs v3 — Apr 14-18 2026")
    print("  v2: blind (BTC ±0.5% → cancel)")
    print("  v3: consult derivatives (OI, CVD, taker, liq) before deciding")
    print("=" * 90)

    v2_actions = []
    v3_actions = []

    for i in range(3, len(btc_prices)):
        ts = btc_prices[i][0]
        btc_now = btc_prices[i][1]
        btc_15m_ago = btc_prices[i-1][1]  # ~1h ago (approximation for 15m)

        btc_change = (btc_now / btc_15m_ago - 1) * 100

        # What happened NEXT hour (to judge if cancel was correct)
        next_price = btc_prices[i+1][1] if i+1 < len(btc_prices) else btc_now
        next_change = (next_price / btc_now - 1) * 100

        v2_dec, v2_reason = guardian_v2_decision(btc_change)
        v3_dec, v3_reason = guardian_v3_decision(conn, ts, btc_change)

        if v2_dec != 'HOLD' or v3_dec != 'HOLD':
            dt = datetime.utcfromtimestamp(ts)

            # Was cancelling correct?
            if v2_dec == 'CANCEL_LONGS':
                v2_correct = next_change < 0  # price continued down = correct to cancel longs
            elif v2_dec == 'CANCEL_SHORTS':
                v2_correct = next_change > 0
            else:
                v2_correct = None

            if v3_dec == 'CANCEL_LONGS':
                v3_correct = next_change < 0
            elif v3_dec == 'CANCEL_SHORTS':
                v3_correct = next_change > 0
            elif v3_dec == 'HOLD' and v2_dec != 'HOLD':
                # v3 held when v2 cancelled — was HOLDING correct?
                if v2_dec == 'CANCEL_LONGS':
                    v3_correct = next_change > 0  # held longs, price went up = correct
                else:
                    v3_correct = next_change < 0
            else:
                v3_correct = None

            v2_icon = '✅' if v2_correct else ('❌' if v2_correct is not None else '  ')
            v3_icon = '✅' if v3_correct else ('❌' if v3_correct is not None else '  ')

            v2_actions.append(v2_correct)
            v3_actions.append(v3_correct)

            print(f"\n[{dt.strftime('%m-%d %H:%M')}] BTC {btc_change:+.2f}% → next hour {next_change:+.2f}%")
            print(f"  v2: {v2_icon} {v2_dec:15s} | {v2_reason}")
            print(f"  v3: {v3_icon} {v3_dec:15s} | {v3_reason[:80]}")

    # Summary
    print(f"\n{'='*90}")
    print(f"  SUMMARY")
    print(f"{'='*90}")

    v2_triggered = [x for x in v2_actions if x is not None]
    v3_triggered = [x for x in v3_actions if x is not None]

    v2_correct_count = sum(1 for x in v2_triggered if x)
    v3_correct_count = sum(1 for x in v3_triggered if x)

    print(f"  v2: {len(v2_triggered)} actions, {v2_correct_count} correct ({v2_correct_count/len(v2_triggered)*100:.0f}% accuracy)" if v2_triggered else "  v2: no actions")
    print(f"  v3: {len(v3_triggered)} actions, {v3_correct_count} correct ({v3_correct_count/len(v3_triggered)*100:.0f}% accuracy)" if v3_triggered else "  v3: no actions")

    conn.close()


if __name__ == '__main__':
    main()
