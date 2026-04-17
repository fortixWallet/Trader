"""
Test new prompts vs old prompts on today's data (Apr 17).
Calls real Opus with both prompt versions on same market data.
"""
import os, sys, json, sqlite3, re, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import anthropic

DB_PATH = 'data/crypto/market.db'
client = anthropic.Anthropic(api_key=os.environ['ANTHROPIC_API_KEY'])

# Load knowledge
knowledge = ""
for f in ['src/crypto/profi_knowledge.md', 'src/crypto/profi_coin_knowledge.md',
          'src/crypto/profi_advanced_knowledge.md', 'src/crypto/profi_lessons.md']:
    try:
        knowledge += open(f).read() + "\n\n"
    except: pass

# Get macro data
conn = sqlite3.connect(DB_PATH)
parts = []
r = conn.execute("SELECT close FROM prices WHERE coin='BTC' AND timeframe='4h' ORDER BY timestamp DESC LIMIT 42").fetchall()
if len(r) >= 42: parts.append(f"BTC_7d={((r[0][0]/r[-1][0])-1)*100:+.1f}%")
r = conn.execute("SELECT close FROM prices WHERE coin='BTC' AND timeframe='4h' ORDER BY timestamp DESC LIMIT 6").fetchall()
if len(r) >= 6: parts.append(f"BTC_1d={((r[0][0]/r[-1][0])-1)*100:+.1f}%")
r = conn.execute("SELECT value FROM fear_greed ORDER BY date DESC LIMIT 1").fetchone()
if r: parts.append(f"F&G={r[0]}")
r = conn.execute("SELECT value FROM cq_btc_onchain WHERE metric='mvrv' ORDER BY date DESC LIMIT 1").fetchone()
if r and r[0]: parts.append(f"MVRV={r[0]:.2f}")
r = conn.execute("SELECT rate FROM funding_rates WHERE coin='BTC' ORDER BY timestamp DESC LIMIT 1").fetchone()
if r and r[0] is not None: parts.append(f"BTC_funding={r[0]*100:+.3f}%")
macro = " | ".join(parts)

# Get coin data for test
coins_data = []
for coin in ['BTC', 'ETH', 'SOL', 'LDO', 'AVAX', 'LINK', 'PENDLE', 'ADA']:
    row = conn.execute("SELECT close FROM prices WHERE coin=? AND timeframe='1h' ORDER BY timestamp DESC LIMIT 1", (coin,)).fetchone()
    if row:
        price = row[0]
        atr_rows = conn.execute("SELECT high,low,close FROM prices WHERE coin=? AND timeframe='1h' ORDER BY timestamp DESC LIMIT 14", (coin,)).fetchall()
        import numpy as np
        atr = np.mean([(r[0]-r[1])/r[2] for r in atr_rows if r[2] > 0]) if len(atr_rows) >= 5 else 0.01
        fr = conn.execute("SELECT rate FROM funding_rates WHERE coin=? ORDER BY timestamp DESC LIMIT 1", (coin,)).fetchone()
        fund = fr[0]*100 if fr and fr[0] else 0
        coins_data.append(f"[{coin}: LIVE=${price:.4f} | ATR_1h={atr*100:.2f}% | funding={fund:+.3f}%]")

coins_str = "\n".join(coins_data)
conn.close()

# OLD system prompt
OLD_SYSTEM = f"""You are PROFI — a professional crypto futures trader who MAKES MONEY.
You have 20 years experience and $1,000 capital on Bybit Demo.

YOUR CORE BELIEF: There is ALWAYS a way to profit. Always.
You are NOT a risk-averse analyst who says WAIT. You are a TRADER who finds the edge.
Every scan, you MUST find at least 1 tradeable setup.

YOUR TRADING KNOWLEDGE:
{knowledge[:5000]}"""

OLD_SCAN = f"""MACRO: {macro}

*** BTC MOMENTUM: BTC RISING: +0.5% (1h) ***
RULE: When BTC RISING → LONG BTC and alts. Verified on 1000+ data points: 85% accuracy.

{coins_str}

REGIME: BULL
HOW TO DECIDE DIRECTION:
- OB buy pressure >20% + 15m momentum positive → bullish signal, BUT check RSI and recent rally size before entering
- LIVE order book: buy/sell pressure and walls — THIS PREDICTS the next move

Reply JSON array (3-5 setups):
[{{"coin": "BTC", "direction": "LONG", "entry": 75000, "sl": 74400, "tp": 76000, "confidence": 0.72, "reason": "..."}}]"""

# NEW system prompt
NEW_SYSTEM = f"""You are PROFI — a professional crypto futures trader.
You make money by being RIGHT about direction, not by trading often.

CORE PRINCIPLES:
- SKIP is your best trade when uncertain. Empty array [] is a valid response.
- Quality over quantity. 3 perfect setups > 8 mediocre ones.
- MACRO drives direction. Per-coin data confirms entry.
- OB shows WHERE walls are (for entry placement), NOT direction. Walls can be spoofed.
- Past SL on a coin TODAY = avoid that coin and level.

YOUR TRADING KNOWLEDGE:
{knowledge[:5000]}"""

NEW_SCAN = f"""MACRO: {macro}

BTC context: +0.5% (1h). Note: this is CURRENT momentum, not prediction.

{coins_str}

REGIME: BULL

DECISION PROCESS (follow this order):
1. READ MACRO first. BTC_7d tells you the TREND. F&G tells you SENTIMENT. Funding tells you CROWD.
2. DECIDE direction from MACRO. If BTC already moved >2% today → exhaustion risk, be cautious.
3. If uncertain about direction → SKIP. Return empty []. No forced trades.
4. THEN find 3-5 coins that match your direction at S/R within 0.5% of price.

RULES:
- OB pressure is for ENTRY PLACEMENT only, not direction. "OB buy +50%" does NOT mean LONG.
- If price approached S/R level fast (>0.5% drop in 1h) → likely breakdown, not bounce. SKIP.
- In your reason, CITE at least one MACRO factor (BTC_7d, F&G, funding, MVRV).
- Risk management is code-enforced: SL -6.5% ROI, TP +13% ROI, trailing +6%/-2%.

Reply JSON array (0-5 setups). EMPTY [] is valid if no strong setups:
[{{"coin": "BTC", "direction": "LONG", "entry": 75000, "sl": 74400, "tp": 76000, "confidence": 0.72, "reason": "BTC_7d=+4.5% uptrend, F&G=23 fear=contrarian buy..."}}]"""

print("="*60)
print("  TESTING OLD vs NEW PROMPTS ON SAME DATA")
print("="*60)

# Call OLD
print("\n=== OLD PROMPT ===\n")
resp_old = client.messages.create(
    model="claude-opus-4-6",
    max_tokens=800,
    system=[{"type": "text", "text": OLD_SYSTEM}],
    messages=[{"role": "user", "content": OLD_SCAN}]
)
old_text = resp_old.content[0].text
print(old_text[:1000])

# Call NEW
print("\n=== NEW PROMPT ===\n")
resp_new = client.messages.create(
    model="claude-opus-4-6",
    max_tokens=800,
    system=[{"type": "text", "text": NEW_SYSTEM}],
    messages=[{"role": "user", "content": NEW_SCAN}]
)
new_text = resp_new.content[0].text
print(new_text[:1000])

# Compare
print("\n=== COMPARISON ===\n")
def count_setups(text):
    try:
        start = text.find('[')
        end = text.rfind(']') + 1
        if start >= 0 and end > start:
            setups = json.loads(text[start:end])
            return setups
    except:
        pass
    return []

old_setups = count_setups(old_text)
new_setups = count_setups(new_text)

print(f"OLD: {len(old_setups)} setups")
for s in old_setups:
    print(f"  {s.get('direction','?'):5} {s.get('coin','?'):8} conf={s.get('confidence',0):.0%} | {s.get('reason','')[:80]}")

print(f"\nNEW: {len(new_setups)} setups")
for s in new_setups:
    print(f"  {s.get('direction','?'):5} {s.get('coin','?'):8} conf={s.get('confidence',0):.0%} | {s.get('reason','')[:80]}")

# Check: does NEW cite macro?
new_reasons = " ".join(s.get('reason','') for s in new_setups)
macro_cited = any(k in new_reasons for k in ['BTC_7d', 'F&G', 'MVRV', 'funding', 'macro', '7d'])
print(f"\nNEW cites MACRO in reasoning: {'YES' if macro_cited else 'NO'}")
print(f"NEW returns empty (SKIP): {'YES' if len(new_setups) == 0 else 'NO'}")
