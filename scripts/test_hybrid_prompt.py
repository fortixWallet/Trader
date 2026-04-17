"""Test hybrid prompt: OLD aggression + NEW safety on today's data with real Opus."""
import os, sys, json, sqlite3, re, time, numpy as np
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import anthropic

DB_PATH = 'data/crypto/market.db'
api_key = open('.env').read().split('ANTHROPIC_API_KEY=')[1].split('\n')[0]
client = anthropic.Anthropic(api_key=api_key)

# Load knowledge (first 5000 chars)
knowledge = ""
for f in ['src/crypto/profi_knowledge.md', 'src/crypto/profi_coin_knowledge.md',
          'src/crypto/profi_lessons.md']:
    try: knowledge += open(f).read()[:3000] + "\n"
    except: pass

# HYBRID system prompt: OLD aggression + NEW awareness
HYBRID_SYSTEM = f"""You are PROFI — a professional crypto futures trader who MAKES MONEY.
You have 20 years experience. You ALWAYS find opportunity in any market.

CORE RULES:
- ALWAYS return 3-5 setups per scan. Market always has edge — find it.
- MACRO awareness: cite BTC_7d, F&G, funding in your reasoning. If macro conflicts with your direction — lower confidence, don't skip.
- OB shows entry placement, not direction. Don't trust walls blindly.
- Both LONG and SHORT are valid. Follow the trend.
- If coin had SL today — avoid it.
- Risk management is automated: SL -6.5%, TP +13%, trailing +8%/-2%.

YOUR ROLE: pick DIRECTION and ENTRY. 3-5 setups minimum. Quality matters but so does participation.

{knowledge[:8000]}"""

conn = sqlite3.connect(DB_PATH)

# Get macro data
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

# Get per-coin data
coins_data = []
for coin in ['BTC','ETH','SOL','BNB','XRP','ADA','AVAX','LINK','LDO','PENDLE','CRV','POL','UNI']:
    row = conn.execute("SELECT close FROM prices WHERE coin=? AND timeframe='1h' ORDER BY timestamp DESC LIMIT 1", (coin,)).fetchone()
    if not row: continue
    price = row[0]
    atr_rows = conn.execute("SELECT high,low,close FROM prices WHERE coin=? AND timeframe='1h' ORDER BY timestamp DESC LIMIT 14", (coin,)).fetchall()
    atr = np.mean([(r[0]-r[1])/r[2] for r in atr_rows if r[2] > 0]) if len(atr_rows) >= 5 else 0.01
    fr = conn.execute("SELECT rate FROM funding_rates WHERE coin=? ORDER BY timestamp DESC LIMIT 1", (coin,)).fetchone()
    fund = fr[0]*100 if fr and fr[0] else 0
    coins_data.append(f"[{coin}: ${price:.4f} | ATR={atr*100:.2f}% | fund={fund:+.3f}%]")

coins_str = "\n".join(coins_data)
conn.close()

# 3 prompts to compare
prompts = {
    "OLD (MUST find)": f"""MACRO: {macro}

*** BTC MOMENTUM: BTC context ***

{coins_str}

REGIME: BULL

HOW TO DECIDE: OB pressure + momentum → direction. Find 5-8 setups.
Reply JSON array (5-8 setups):
[{{"coin":"BTC","direction":"LONG","entry":75000,"sl":74400,"tp":76000,"confidence":0.72,"reason":"..."}}]""",

    "NEW (SKIP ok)": f"""MACRO: {macro}

BTC context: current momentum.

{coins_str}

REGIME: BULL

DECISION PROCESS:
1. READ MACRO first.
2. If uncertain → SKIP. Return empty [].
3. Find 3-5 coins matching direction.
MUST cite MACRO in reason.
Reply JSON (0-5 setups):""",

    "HYBRID (always trade + macro aware)": f"""MACRO: {macro}

BTC context: current state, not prediction.

{coins_str}

REGIME: BULL

RULES:
- ALWAYS return 3-5 setups. Market has opportunity — find it.
- Cite MACRO in every reason (BTC_7d, F&G, funding).
- If macro conflicts with your direction → still trade but LOWER confidence.
- Entry within 0.5% of current price.
- If BTC moved >2% today → cautious, prefer pullback entries.
Reply JSON array (3-5 setups):
[{{"coin":"BTC","direction":"LONG","entry":75000,"sl":74400,"tp":76000,"confidence":0.72,"reason":"BTC_7d=+4.5%..."}}]"""
}

for name, scan_prompt in prompts.items():
    print(f"\n{'='*60}")
    print(f"  {name}")
    print(f"{'='*60}\n")
    
    resp = client.messages.create(
        model="claude-opus-4-6", max_tokens=600,
        system=[{"type": "text", "text": HYBRID_SYSTEM if 'HYBRID' in name else HYBRID_SYSTEM.replace("ALWAYS return 3-5", "Find setups if confident")}],
        messages=[{"role": "user", "content": scan_prompt}]
    )
    text = resp.content[0].text
    
    try:
        start = text.find('['); end = text.rfind(']') + 1
        setups = json.loads(text[start:end]) if start >= 0 else []
    except: setups = []
    
    print(f"Setups: {len(setups)}")
    for s in setups:
        macro_cited = any(k in s.get('reason','') for k in ['BTC_7d','F&G','MVRV','funding','7d','fear'])
        print(f"  {s.get('direction','?'):5} {s.get('coin','?'):8} conf={s.get('confidence',0):.0%} macro={'✅' if macro_cited else '❌'} | {s.get('reason','')[:70]}")
