import sqlite3, sys
from datetime import datetime, timezone, timedelta
sys.path.insert(0, 'C:/YT/Factory')

conn = sqlite3.connect('data/crypto/market.db', timeout=15)
now = datetime.now(timezone.utc)

print('=== ETH DATA FOR WEEKLY ANALYSIS ===\n')

# 1. Prices
print('1. PRICE (7d):')
for r in conn.execute("SELECT date(timestamp,'unixepoch'), close FROM prices WHERE coin='ETH' AND timeframe='1d' ORDER BY timestamp DESC LIMIT 7").fetchall():
    print(f'   {r[0]}: ${r[1]:,.2f}')

# 2. Funding
print('\n2. FUNDING RATES:')
fr = conn.execute("SELECT rate, datetime(timestamp,'unixepoch') FROM funding_rates WHERE coin='ETH' ORDER BY timestamp DESC LIMIT 1").fetchone()
if fr: print(f'   Latest: {fr[1]} rate={fr[0]:.6f}%')
cnt = conn.execute("SELECT COUNT(*) FROM funding_rates WHERE coin='ETH'").fetchone()
print(f'   Total entries: {cnt[0]}')

# 3. L/S
print('\n3. L/S RATIO:')
ls = conn.execute("SELECT long_ratio, datetime(timestamp,'unixepoch') FROM long_short_ratio WHERE coin='ETH' AND ratio_type='global' ORDER BY timestamp DESC LIMIT 1").fetchone()
if ls: print(f'   {ls[1]}: {float(ls[0])*100:.0f}% long')

# 4. On-chain
print('\n4. ON-CHAIN:')
for t in ['cq_exchange_flows', 'cq_active_addresses']:
    try:
        row = conn.execute(f"SELECT MAX(date), COUNT(*) FROM {t} WHERE coin='ETH'").fetchone()
        print(f'   {t}: {row[0]}, {row[1]} entries')
    except: print(f'   {t}: N/A')

# 5. ETF
print('\n5. ETH ETF:')
try:
    for e in conn.execute("SELECT date, flow_usd FROM cg_etf_flows WHERE asset='ETH' ORDER BY date DESC LIMIT 5").fetchall():
        print(f'   {e[0]}: ${e[1]/1e6:+.0f}M' if e[1] else f'   {e[0]}: $0')
except: print('   N/A')

# 6. TVL
print('\n6. DEFI TVL:')
try:
    for t in conn.execute("SELECT date, tvl FROM defi_tvl_chain WHERE chain='Ethereum' ORDER BY date DESC LIMIT 3").fetchall():
        print(f'   {t[0]}: ${t[1]/1e9:.1f}B')
except: print('   No chain TVL')
try:
    for t in conn.execute("SELECT date, total_tvl FROM defi_tvl_history ORDER BY date DESC LIMIT 3").fetchall():
        print(f'   Total: {t[0]}: ${t[1]/1e9:.1f}B')
except: print('   No total TVL')

# 7. Whales
print('\n7. ETH WHALES (7d):')
w = conn.execute("SELECT COUNT(*), SUM(amount_usd) FROM whale_transactions WHERE coin='ETH' AND timestamp > ?",
    (int((now - timedelta(days=7)).timestamp()),)).fetchone()
print(f'   {w[0]} transactions, ${w[1]/1e6:.0f}M total' if w[1] else f'   {w[0]} transactions')

# 8. Overview
print('\n8. CURRENT:')
mo = conn.execute("SELECT price_usd, change_24h, change_7d, change_30d, rank FROM market_overview WHERE coin='ETH' ORDER BY timestamp DESC LIMIT 1").fetchone()
if mo:
    print(f'   ${mo[0]:,.2f} | 24h:{mo[1]:+.1f}% | 7d:{mo[2]:+.1f}% | 30d:{mo[3]:+.1f}% | #{mo[4]}')

conn.close()

# 9. Forecast
print('\n9. FORECAST:')
from src.crypto.forecast_engine import forecast_coin
fc = forecast_coin('ETH')
if fc:
    print(f'   Prediction: {fc.get("prediction")} | Score: {fc.get("composite_score",0):.3f} | Confidence: {fc.get("confidence",0)}/10')
    th = fc.get('target_high', 0)
    tl = fc.get('target_low', 0)
    print(f'   Target: ${tl:,.0f} - ${th:,.0f}')
    sigs = fc.get('v3_signals', [])
    print(f'   Signals: {len(sigs)}' + (f' - {sigs[0]["type"]}' if sigs else ' - none'))

# 10. Opportunity
print('\n10. OPPORTUNITY INDICATORS:')
from src.crypto.opportunity_detector import detect_opportunities
for o in detect_opportunities():
    if 'ETH' in o.get('coins', []) or not o['coins']:
        print(f'   {o["type"]}: {o["description"][:80]}')
