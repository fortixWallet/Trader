"""
FORTIX — Phase A: Build Historical Crypto Event Database
=========================================================
1. Create crypto_events table
2. Populate with 200+ major events (2020-2026)
3. Backfill BTC/ETH daily prices from CoinGecko (2020-01 to 2021-03)
4. Calculate price impact (1h, 24h, 7d) for each event
5. Classify severity and build event_type → impact distributions

Usage:
    python scripts/build_event_database.py              # Full build
    python scripts/build_event_database.py --events     # Only insert events
    python scripts/build_event_database.py --backfill   # Only backfill prices
    python scripts/build_event_database.py --impacts    # Only calculate impacts
"""

import sqlite3
import json
import time
import sys
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path

DB_PATH = Path('data/crypto/market.db')


# ═══════════════════════════════════════════════════════
# STEP 1: CREATE TABLE
# ═══════════════════════════════════════════════════════

def create_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crypto_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            event_type TEXT NOT NULL,
            description TEXT NOT NULL,
            coins_affected TEXT,
            impact_1h REAL,
            impact_24h REAL,
            impact_7d REAL,
            btc_price_at_event REAL,
            market_cap_change REAL,
            severity INTEGER,
            source TEXT,
            sentiment TEXT,
            UNIQUE(date, description)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_events_date ON crypto_events(date)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_events_type ON crypto_events(event_type)
    """)
    conn.commit()
    print("[OK] crypto_events table ready")


# ═══════════════════════════════════════════════════════
# STEP 2: HISTORICAL EVENTS DATABASE (200+)
# ═══════════════════════════════════════════════════════

EVENTS = [
    # ══════ 2020 ══════
    # COVID & Recovery
    ("2020-01-03", "black_swan", "US drone strike kills Iranian General Soleimani — geopolitical shock", '["BTC"]', 6, "negative"),
    ("2020-02-13", "macro", "Global COVID fears start affecting markets, BTC drops from $10.5K", '["BTC"]', 5, "negative"),
    ("2020-03-12", "black_swan", "COVID Black Thursday — BTC crashes 40% in 24h to $3,800, global markets collapse", '["BTC","ETH"]', 10, "negative"),
    ("2020-03-13", "macro", "Fed emergency rate cut to 0%, massive QE announced", '["BTC"]', 8, "positive"),
    ("2020-03-15", "macro", "Fed cuts rates to zero, $700B QE program — infinite money era begins", '["BTC","ETH"]', 9, "positive"),
    ("2020-04-09", "macro", "Fed announces $2.3T in lending programs, risk assets rally", '["BTC"]', 7, "positive"),
    ("2020-05-11", "halving", "Bitcoin 3rd halving — block reward 12.5→6.25 BTC, supply shock begins", '["BTC"]', 9, "positive"),
    ("2020-06-16", "macro", "Fed begins corporate bond buying — unprecedented stimulus", '["BTC"]', 6, "positive"),
    ("2020-07-27", "institutional", "Grayscale BTC Trust premium hits 20%, institutional demand surging", '["BTC"]', 6, "positive"),
    ("2020-08-02", "defi", "DeFi Summer peaks — Yearn, Compound, Aave TVL explodes 10x", '["ETH"]', 7, "positive"),
    ("2020-08-11", "institutional", "MicroStrategy announces $250M BTC purchase — first major corporate buyer", '["BTC"]', 8, "positive"),
    ("2020-09-14", "defi", "Uniswap UNI token airdrop — $1,200 per wallet, DeFi mania", '["ETH","UNI"]', 6, "positive"),
    ("2020-10-08", "regulatory", "Square (Jack Dorsey) buys $50M BTC, follows MicroStrategy playbook", '["BTC"]', 6, "positive"),
    ("2020-10-21", "institutional", "PayPal announces crypto buying/selling for 350M users", '["BTC","ETH"]', 9, "positive"),
    ("2020-11-05", "macro", "Biden wins US election — stimulus expectations boost risk assets", '["BTC"]', 6, "positive"),
    ("2020-11-18", "institutional", "MicroStrategy announces another $400M BTC purchase", '["BTC"]', 7, "positive"),
    ("2020-12-01", "institutional", "Guggenheim Partners files to invest up to $500M in GBTC", '["BTC"]', 6, "positive"),
    ("2020-12-11", "institutional", "MassMutual ($235B insurer) buys $100M BTC", '["BTC"]', 7, "positive"),
    ("2020-12-16", "market_milestone", "BTC breaks $20K ATH for first time since 2017", '["BTC"]', 8, "positive"),
    ("2020-12-27", "regulatory", "US Treasury proposes self-hosted wallet KYC rules — crypto community pushback", '["BTC"]', 5, "negative"),

    # ══════ 2021 Q1 ══════
    ("2021-01-02", "market_milestone", "BTC breaks $30K — 50% gain in 2 weeks", '["BTC"]', 7, "positive"),
    ("2021-01-08", "market_milestone", "BTC hits $42K — parabolic move", '["BTC"]', 7, "positive"),
    ("2021-01-11", "market_milestone", "BTC drops 26% from $42K to $30K in 3 days — first major correction", '["BTC"]', 7, "negative"),
    ("2021-01-29", "social", "Elon Musk adds #Bitcoin to Twitter bio — BTC pumps 20%", '["BTC"]', 8, "positive"),
    ("2021-02-08", "institutional", "Tesla buys $1.5B BTC, plans to accept BTC payments", '["BTC"]', 10, "positive"),
    ("2021-02-09", "institutional", "BNY Mellon ($2T custodian) announces crypto custody services", '["BTC"]', 7, "positive"),
    ("2021-02-12", "institutional", "MicroStrategy buys another $1B BTC", '["BTC"]', 7, "positive"),
    ("2021-02-19", "market_milestone", "BTC market cap hits $1 trillion for first time", '["BTC"]', 8, "positive"),
    ("2021-02-21", "market_milestone", "BTC hits $58K ATH", '["BTC"]', 7, "positive"),
    ("2021-02-22", "market_milestone", "BTC drops 17% in 24h from $58K — Yellen warns about crypto", '["BTC"]', 6, "negative"),
    ("2021-03-11", "market_milestone", "BTC hits $60K ATH amid NFT mania (Beeple $69M sale)", '["BTC","ETH"]', 7, "positive"),
    ("2021-03-13", "macro", "Biden signs $1.9T stimulus — risk assets rally", '["BTC"]', 7, "positive"),
    ("2021-03-24", "institutional", "Tesla starts accepting BTC for car purchases", '["BTC"]', 7, "positive"),

    # ══════ 2021 Q2 ══════
    ("2021-04-14", "institutional", "Coinbase IPO on NASDAQ at $328/share ($85B valuation)", '["BTC","ETH"]', 8, "positive"),
    ("2021-04-14", "market_milestone", "BTC hits $64.8K ATH on Coinbase IPO day", '["BTC"]', 8, "positive"),
    ("2021-04-18", "market_event", "BTC flash crash 15% — $10B liquidations in 1 hour, Turkey bans crypto payments", '["BTC"]', 7, "negative"),
    ("2021-04-23", "political", "Biden proposes doubling capital gains tax to 39.6% — crypto sells off 10%", '["BTC","ETH"]', 7, "negative"),
    ("2021-05-12", "political", "Elon Musk tweets Tesla stops accepting BTC (environment concerns) — BTC -15%", '["BTC"]', 9, "negative"),
    ("2021-05-13", "market_event", "BTC drops below $50K for first time since March", '["BTC"]', 6, "negative"),
    ("2021-05-19", "regulatory", "China bans financial institutions from crypto — BTC crashes to $30K (-30%)", '["BTC","ETH"]', 10, "negative"),
    ("2021-05-21", "regulatory", "China State Council cracks down on BTC mining — mass exodus begins", '["BTC"]', 9, "negative"),
    ("2021-06-09", "political", "El Salvador passes BTC legal tender law — first country ever", '["BTC"]', 8, "positive"),
    ("2021-06-21", "regulatory", "China orders BTC miners to shut down (Sichuan, Xinjiang, Inner Mongolia)", '["BTC"]', 8, "negative"),
    ("2021-06-22", "market_event", "BTC hits local bottom at $28.8K — 55% drop from ATH", '["BTC"]', 7, "negative"),
    ("2021-06-26", "macro", "FATF publishes updated crypto guidance — stricter travel rule", '["BTC"]', 5, "negative"),

    # ══════ 2021 Q3 ══════
    ("2021-07-21", "social", "Elon Musk, Jack Dorsey, Cathie Wood at 'B Word' conference — bullish", '["BTC"]', 5, "positive"),
    ("2021-07-26", "social", "Amazon job posting for 'Digital Currency Lead' — BTC pumps 15%", '["BTC"]', 7, "positive"),
    ("2021-08-01", "upgrade", "Ethereum EIP-1559 (London upgrade) — fee burn mechanism, deflationary ETH", '["ETH"]', 8, "positive"),
    ("2021-08-05", "upgrade", "EIP-1559 goes live — 700 ETH burned in first hour", '["ETH"]', 7, "positive"),
    ("2021-08-10", "hack", "Poly Network hack — $611M stolen (largest DeFi hack at the time), later returned", '["ETH"]', 7, "negative"),
    ("2021-08-23", "institutional", "MicroStrategy buys 3,907 BTC for $177M — total now 108,992 BTC", '["BTC"]', 5, "positive"),
    ("2021-09-07", "political", "El Salvador officially adopts BTC as legal tender — app crashes, protests", '["BTC"]', 7, "mixed"),
    ("2021-09-07", "market_event", "BTC crashes 17% on El Salvador launch day — buy the rumor sell the news", '["BTC"]', 7, "negative"),
    ("2021-09-24", "regulatory", "China declares ALL crypto transactions illegal — final ban", '["BTC","ETH"]', 8, "negative"),

    # ══════ 2021 Q4 ══════
    ("2021-10-01", "macro", "US debt ceiling crisis — risk-off across all markets", '["BTC"]', 5, "negative"),
    ("2021-10-15", "regulatory", "SEC approves ProShares Bitcoin Futures ETF (BITO) — first US BTC ETF", '["BTC"]', 9, "positive"),
    ("2021-10-19", "institutional", "BITO ETF launches — $1B volume on day 1 (fastest ETF ever)", '["BTC"]', 8, "positive"),
    ("2021-10-20", "market_milestone", "BTC hits $67K new ATH", '["BTC"]', 7, "positive"),
    ("2021-11-03", "macro", "Fed announces tapering of bond purchases — beginning of tightening", '["BTC"]', 6, "negative"),
    ("2021-11-09", "defi", "Total crypto market cap hits $3T ATH", '["BTC","ETH"]', 8, "positive"),
    ("2021-11-10", "market_milestone", "BTC hits $69K ATH — cycle top (not known at the time)", '["BTC"]', 9, "positive"),
    ("2021-11-10", "market_milestone", "ETH hits $4,891 ATH", '["ETH"]', 8, "positive"),
    ("2021-11-12", "market_event", "BTC starts declining from $69K — start of bear market", '["BTC"]', 7, "negative"),
    ("2021-12-04", "market_event", "BTC flash crash to $42K — $2.4B liquidations, Omicron fears", '["BTC","ETH"]', 8, "negative"),
    ("2021-12-14", "regulatory", "India crypto tax bill rumors — 30% tax + no loss offset", '["BTC"]', 5, "negative"),
    ("2021-12-15", "macro", "Fed signals 3 rate hikes in 2022 — hawkish pivot begins", '["BTC"]', 7, "negative"),

    # ══════ 2022 Q1 ══════
    ("2022-01-05", "macro", "Fed minutes reveal aggressive tightening plans — crypto drops 10%", '["BTC","ETH"]', 7, "negative"),
    ("2022-01-21", "market_event", "BTC drops below $35K — 50% from ATH, crypto winter fears", '["BTC"]', 7, "negative"),
    ("2022-01-24", "macro", "Russia-Ukraine tensions escalate — risk-off globally", '["BTC"]', 6, "negative"),
    ("2022-02-09", "regulatory", "US DOJ seizes $3.6B in stolen BTC from 2016 Bitfinex hack", '["BTC"]', 5, "mixed"),
    ("2022-02-24", "black_swan", "Russia invades Ukraine — BTC drops 10%, then recovers as 'digital gold' narrative", '["BTC","ETH"]', 9, "negative"),
    ("2022-03-09", "political", "Biden signs executive order on crypto regulation — market rallies (not as bad as feared)", '["BTC","ETH"]', 7, "positive"),
    ("2022-03-16", "macro", "Fed raises rates 25bps (first hike since 2018) — crypto already priced it in", '["BTC"]', 6, "mixed"),

    # ══════ 2022 Q2 ══════
    ("2022-04-06", "macro", "Fed signals 50bps hikes and QT — aggressive tightening", '["BTC"]', 6, "negative"),
    ("2022-04-27", "regulatory", "Terraform Labs founder Do Kwon subpoenaed by SEC", '["LUNA","UST"]', 5, "negative"),
    ("2022-05-04", "macro", "Fed raises rates 50bps — largest hike since 2000", '["BTC"]', 7, "negative"),
    ("2022-05-09", "black_swan", "UST depeg begins — Luna death spiral starts, $40B wiped in days", '["LUNA","UST","BTC"]', 10, "negative"),
    ("2022-05-12", "black_swan", "Luna crashes 99%, UST collapses to $0.10 — contagion fears spread", '["LUNA","BTC","ETH"]', 10, "negative"),
    ("2022-05-13", "black_swan", "BTC drops to $26K amid Luna contagion — total market cap -35%", '["BTC","ETH"]', 9, "negative"),
    ("2022-06-13", "black_swan", "Celsius Network halts withdrawals — $11.7B frozen, insolvency fears", '["BTC","ETH"]', 8, "negative"),
    ("2022-06-15", "macro", "Fed raises rates 75bps — most aggressive in 28 years", '["BTC"]', 7, "negative"),
    ("2022-06-18", "market_event", "BTC drops below $20K for first time since Dec 2020 — capitulation", '["BTC"]', 9, "negative"),
    ("2022-06-27", "black_swan", "Three Arrows Capital (3AC) files for bankruptcy — $3B hedge fund", '["BTC","ETH"]', 8, "negative"),

    # ══════ 2022 Q3 ══════
    ("2022-07-06", "black_swan", "Voyager Digital files for bankruptcy — $5.9B in assets frozen", '["BTC"]', 6, "negative"),
    ("2022-07-13", "black_swan", "Celsius files for Chapter 11 bankruptcy", '["BTC","ETH"]', 6, "negative"),
    ("2022-07-20", "institutional", "BlackRock partners with Coinbase for institutional crypto", '["BTC"]', 7, "positive"),
    ("2022-07-27", "macro", "Fed raises rates another 75bps — markets rally on 'peak hawkishness' hope", '["BTC"]', 6, "mixed"),
    ("2022-08-08", "regulatory", "US Treasury sanctions Tornado Cash — first DeFi protocol sanctioned", '["ETH"]', 7, "negative"),
    ("2022-08-10", "regulatory", "Tornado Cash developer arrested in Netherlands", '["ETH"]', 6, "negative"),
    ("2022-09-15", "upgrade", "Ethereum Merge — PoW→PoS transition, ETH issuance drops 90%", '["ETH"]', 9, "positive"),
    ("2022-09-21", "macro", "Fed raises rates 75bps — 3rd consecutive mega hike", '["BTC"]', 6, "negative"),

    # ══════ 2022 Q4 ══════
    ("2022-10-07", "hack", "BNB Chain bridge hack — $569M stolen, chain halted", '["BNB"]', 7, "negative"),
    ("2022-10-14", "macro", "CPI comes in hot at 8.2% — but stocks/crypto rally on 'peak inflation' thesis", '["BTC"]', 6, "mixed"),
    ("2022-11-02", "black_swan", "CoinDesk reveals Alameda Research balance sheet — mostly FTT tokens", '["FTT","BTC"]', 8, "negative"),
    ("2022-11-06", "black_swan", "Binance CZ announces selling all FTT — triggers bank run on FTX", '["FTT","BTC","SOL"]', 9, "negative"),
    ("2022-11-08", "black_swan", "FTX halts withdrawals — $6B withdrawal requests in 72h", '["FTT","BTC","SOL"]', 10, "negative"),
    ("2022-11-09", "black_swan", "Binance backs out of FTX acquisition — FTX effectively dead", '["FTT","BTC","SOL"]', 10, "negative"),
    ("2022-11-11", "black_swan", "FTX files for bankruptcy — SBF resigns, $8B customer funds missing", '["FTT","BTC","SOL","ETH"]', 10, "negative"),
    ("2022-11-09", "market_event", "BTC crashes to $15.5K — 2-year low, fear extreme", '["BTC"]', 9, "negative"),
    ("2022-11-10", "market_event", "SOL crashes 60% in a week — major FTX/Alameda exposure", '["SOL"]', 9, "negative"),
    ("2022-11-14", "black_swan", "Genesis Trading halts withdrawals — DCG contagion fears", '["BTC"]', 7, "negative"),
    ("2022-11-21", "market_event", "BTC hits cycle low $15.4K — maximum fear", '["BTC"]', 8, "negative"),
    ("2022-12-14", "macro", "Fed raises rates 50bps — slowing pace, dovish signals", '["BTC"]', 6, "positive"),
    ("2022-12-19", "regulatory", "Binance proof-of-reserves audit questioned — Mazars pulls out", '["BNB"]', 6, "negative"),

    # ══════ 2023 Q1 ══════
    ("2023-01-03", "regulatory", "SEC charges Gemini and Genesis for unregistered securities", '["BTC"]', 5, "negative"),
    ("2023-01-13", "market_event", "BTC rallies above $19K — first big move of 2023", '["BTC"]', 6, "positive"),
    ("2023-01-19", "black_swan", "Genesis files for bankruptcy — $3.5B debt", '["BTC"]', 5, "negative"),
    ("2023-02-01", "macro", "Fed raises rates 25bps — smallest hike in the cycle, dovish shift", '["BTC"]', 6, "positive"),
    ("2023-02-09", "regulatory", "SEC charges Kraken for staking — $30M fine, US staking shut down", '["ETH"]', 6, "negative"),
    ("2023-02-13", "regulatory", "SEC proposes new crypto custody rules — industry pushback", '["BTC"]', 5, "negative"),
    ("2023-03-08", "black_swan", "Silvergate Bank announces voluntary liquidation — crypto banking crisis", '["BTC"]', 7, "negative"),
    ("2023-03-10", "black_swan", "Silicon Valley Bank (SVB) collapses — $209B bank, USDC depegs to $0.87", '["USDC","BTC"]', 9, "negative"),
    ("2023-03-12", "black_swan", "Signature Bank shut down by regulators — second crypto-friendly bank gone", '["BTC"]', 8, "negative"),
    ("2023-03-12", "macro", "Fed/FDIC/Treasury guarantee all SVB deposits — banking crisis contained", '["BTC"]', 8, "positive"),
    ("2023-03-13", "market_event", "BTC rallies from $20K to $24K as banking crisis boosts 'be your own bank' narrative", '["BTC"]', 7, "positive"),
    ("2023-03-22", "macro", "Fed raises rates 25bps — signals pause, dot plot shows cuts in 2024", '["BTC"]', 6, "positive"),
    ("2023-03-27", "regulatory", "CFTC sues Binance and CZ for operating illegal exchange", '["BNB"]', 7, "negative"),

    # ══════ 2023 Q2 ══════
    ("2023-04-14", "market_milestone", "BTC breaks $30K — first time since June 2022", '["BTC"]', 6, "positive"),
    ("2023-04-17", "defi", "Ethereum Shanghai upgrade — staked ETH withdrawals enabled", '["ETH"]', 7, "positive"),
    ("2023-04-26", "market_event", "First Republic Bank seized by regulators — banking crisis continues", '["BTC"]', 5, "positive"),
    ("2023-06-05", "regulatory", "SEC sues Binance for 13 charges including commingling customer funds", '["BNB","BTC"]', 8, "negative"),
    ("2023-06-06", "regulatory", "SEC sues Coinbase for operating as unregistered exchange", '["BTC","ETH"]', 8, "negative"),
    ("2023-06-06", "regulatory", "SEC declares SOL, ADA, MATIC as securities in Coinbase lawsuit", '["SOL","ADA","POL"]', 8, "negative"),
    ("2023-06-15", "institutional", "BlackRock files for spot Bitcoin ETF — game changer", '["BTC"]', 10, "positive"),
    ("2023-06-16", "institutional", "BTC pumps 7% on BlackRock ETF filing — institutional hope", '["BTC"]', 8, "positive"),
    ("2023-06-20", "institutional", "Fidelity, WisdomTree, Invesco file spot BTC ETFs — ETF race begins", '["BTC"]', 7, "positive"),
    ("2023-06-29", "institutional", "BlackRock refiles BTC ETF with Coinbase surveillance agreement", '["BTC"]', 6, "positive"),

    # ══════ 2023 Q3 ══════
    ("2023-07-13", "regulatory", "Ripple partially wins vs SEC — XRP not a security in secondary sales, XRP +73%", '["XRP"]', 9, "positive"),
    ("2023-07-26", "macro", "Fed raises rates 25bps to 5.5% — last hike of the cycle", '["BTC"]', 6, "mixed"),
    ("2023-08-17", "market_event", "BTC drops 7% in hours — SpaceX writedown rumors, Evergrande bankruptcy", '["BTC"]', 6, "negative"),
    ("2023-08-29", "regulatory", "Grayscale wins lawsuit against SEC — GBTC→ETF conversion path opens", '["BTC"]', 8, "positive"),
    ("2023-09-01", "market_event", "BTC pumps to $28K on Grayscale ruling, then fades", '["BTC"]', 6, "positive"),
    ("2023-09-11", "institutional", "Franklin Templeton, Hashdex file spot BTC ETFs", '["BTC"]', 5, "positive"),

    # ══════ 2023 Q4 ══════
    ("2023-10-07", "black_swan", "Hamas attacks Israel — geopolitical shock, brief crypto selloff", '["BTC"]', 6, "negative"),
    ("2023-10-16", "social", "False report of BlackRock BTC ETF approval — BTC pumps 10% then drops", '["BTC"]', 6, "mixed"),
    ("2023-10-24", "market_event", "BTC breaks $35K on ETF optimism — strongest rally since April", '["BTC"]', 7, "positive"),
    ("2023-11-01", "macro", "Fed pauses again — 2 meetings in a row, peak rates confirmed", '["BTC"]', 6, "positive"),
    ("2023-11-02", "regulatory", "Sam Bankman-Fried found guilty on all 7 criminal charges", '["BTC"]', 5, "positive"),
    ("2023-11-09", "market_event", "BTC breaks $37K — sustained rally, ETF anticipation", '["BTC"]', 6, "positive"),
    ("2023-11-15", "regulatory", "BlackRock files for spot Ethereum ETF — ETH pumps 10%", '["ETH"]', 8, "positive"),
    ("2023-11-21", "regulatory", "Binance pleads guilty, CZ resigns — $4.3B fine", '["BNB"]', 8, "negative"),
    ("2023-12-01", "market_event", "BTC breaks $40K — first time since April 2022", '["BTC"]', 7, "positive"),
    ("2023-12-04", "market_milestone", "BTC hits $42K, SOL hits $68 — massive altcoin rally", '["BTC","SOL"]', 7, "positive"),
    ("2023-12-13", "macro", "Fed dot plot signals 3 rate cuts in 2024 — massive risk-on rally", '["BTC","ETH"]', 8, "positive"),

    # ══════ 2024 Q1 ══════
    ("2024-01-10", "regulatory", "SEC approves 11 spot Bitcoin ETFs — historic moment", '["BTC"]', 10, "positive"),
    ("2024-01-11", "institutional", "BTC ETFs see $4.6B volume on day 1", '["BTC"]', 8, "positive"),
    ("2024-01-12", "market_event", "BTC sells off from $49K to $42K — 'sell the news'", '["BTC"]', 7, "negative"),
    ("2024-01-23", "institutional", "GBTC outflows dominate — $5B sold in 2 weeks, pressuring BTC", '["BTC"]', 7, "negative"),
    ("2024-02-06", "institutional", "BlackRock IBIT surpasses $3B AUM in less than a month", '["BTC"]', 6, "positive"),
    ("2024-02-15", "market_event", "BTC breaks $52K — ETF demand overwhelming GBTC selling", '["BTC"]', 7, "positive"),
    ("2024-02-28", "market_event", "BTC surpasses $60K — approaching ATH", '["BTC"]', 7, "positive"),
    ("2024-03-05", "market_milestone", "BTC breaks $69K ATH — new all-time high, ETF-driven demand", '["BTC"]', 9, "positive"),
    ("2024-03-08", "market_milestone", "BTC pumps past $70K", '["BTC"]', 8, "positive"),
    ("2024-03-13", "market_milestone", "ETH breaks $4,000 — ETH ETF speculation", '["ETH"]', 7, "positive"),
    ("2024-03-14", "market_milestone", "BTC hits $73.8K ATH — cycle high", '["BTC"]', 9, "positive"),
    ("2024-03-18", "market_event", "BTC corrects 10% from ATH to $66K — normal pullback", '["BTC"]', 5, "negative"),
    ("2024-03-20", "macro", "Fed holds rates, projects 3 cuts in 2024 — risk-on maintained", '["BTC"]', 6, "positive"),

    # ══════ 2024 Q2 ══════
    ("2024-04-13", "black_swan", "Iran launches drone/missile attack on Israel — geopolitical shock", '["BTC"]', 7, "negative"),
    ("2024-04-15", "market_event", "BTC drops 8% to $61K on Iran-Israel escalation", '["BTC"]', 6, "negative"),
    ("2024-04-20", "halving", "Bitcoin 4th halving — block reward 6.25→3.125 BTC", '["BTC"]', 9, "positive"),
    ("2024-05-01", "market_event", "BTC drops to $56K — post-halving consolidation, Mt. Gox concerns", '["BTC"]', 6, "negative"),
    ("2024-05-20", "regulatory", "US passes FIT21 crypto bill in House — bipartisan 279-136 vote", '["BTC","ETH"]', 7, "positive"),
    ("2024-05-23", "regulatory", "SEC approves spot Ethereum ETFs — surprise approval", '["ETH"]', 9, "positive"),
    ("2024-05-24", "market_event", "ETH pumps 20% on ETF approval news", '["ETH"]', 8, "positive"),
    ("2024-06-12", "macro", "Fed holds rates, projects only 1 cut in 2024 (was 3) — hawkish surprise", '["BTC"]', 6, "negative"),
    ("2024-06-18", "market_event", "Mt. Gox trustee moves $9B BTC to new wallets — distribution fears", '["BTC"]', 7, "negative"),
    ("2024-06-24", "market_event", "BTC drops to $58K — Mt. Gox + GBTC outflows pressure", '["BTC"]', 6, "negative"),

    # ══════ 2024 Q3 ══════
    ("2024-07-04", "whale", "German government starts selling 50K BTC ($3B) — heavy sell pressure", '["BTC"]', 8, "negative"),
    ("2024-07-05", "market_event", "BTC drops below $55K — German gov + Mt. Gox + miners selling", '["BTC"]', 7, "negative"),
    ("2024-07-12", "whale", "German government completes BTC sale — selling pressure removed", '["BTC"]', 7, "positive"),
    ("2024-07-13", "political", "Trump assassination attempt — BTC pumps 10% (pro-crypto candidate)", '["BTC"]', 8, "positive"),
    ("2024-07-16", "market_event", "Mt. Gox starts BTC distributions to creditors — $9B over months", '["BTC"]', 7, "negative"),
    ("2024-07-22", "institutional", "ETH spot ETFs begin trading on CBOE", '["ETH"]', 7, "positive"),
    ("2024-07-27", "political", "Trump speaks at Bitcoin Conference: 'Will make US crypto capital'", '["BTC"]', 8, "positive"),
    ("2024-07-31", "macro", "Fed signals September rate cut — risk-on rally", '["BTC","ETH"]', 7, "positive"),
    ("2024-08-05", "black_swan", "Japan carry trade unwind — Nikkei -12%, global markets crash, BTC to $49K", '["BTC","ETH"]', 9, "negative"),
    ("2024-08-05", "market_event", "BTC drops 15% to $49K in hours — $1.2B liquidations", '["BTC","ETH"]', 8, "negative"),
    ("2024-08-07", "market_event", "Markets recover — BTC back above $56K", '["BTC"]', 6, "positive"),
    ("2024-08-23", "macro", "Fed Chair Powell at Jackson Hole: 'Time has come' for rate cuts", '["BTC"]', 7, "positive"),
    ("2024-09-18", "macro", "Fed cuts rates 50bps — first cut since 2020, dovish surprise", '["BTC","ETH"]', 8, "positive"),

    # ══════ 2024 Q4 ══════
    ("2024-10-10", "market_event", "BTC breaks $65K — building momentum for election", '["BTC"]', 6, "positive"),
    ("2024-10-29", "market_event", "BTC hits $73K — matching ATH ahead of election", '["BTC"]', 7, "positive"),
    ("2024-11-05", "political", "Trump wins US presidential election — massive crypto rally", '["BTC","ETH","SOL"]', 10, "positive"),
    ("2024-11-06", "market_event", "BTC surges past $75K on Trump victory — new ATH", '["BTC"]', 9, "positive"),
    ("2024-11-11", "market_event", "BTC breaks $80K — unstoppable momentum", '["BTC"]', 8, "positive"),
    ("2024-11-13", "market_milestone", "BTC hits $90K — $10K move in 2 days", '["BTC"]', 8, "positive"),
    ("2024-11-21", "political", "Trump appoints pro-crypto Paul Atkins as SEC Chair", '["BTC","ETH","SOL"]', 8, "positive"),
    ("2024-11-22", "regulatory", "SEC Chair Gensler announces resignation effective Jan 20", '["BTC","ETH"]', 8, "positive"),
    ("2024-12-05", "market_milestone", "BTC breaks $100K for first time — historic milestone", '["BTC"]', 10, "positive"),
    ("2024-12-08", "political", "Trump: 'We'll do something great with crypto' — continued support", '["BTC"]', 5, "positive"),
    ("2024-12-17", "market_milestone", "BTC hits $108K ATH", '["BTC"]', 8, "positive"),
    ("2024-12-18", "macro", "Fed cuts rates 25bps but signals fewer cuts in 2025 — hawkish cut", '["BTC"]', 7, "negative"),
    ("2024-12-19", "market_event", "BTC drops from $108K to $92K on hawkish Fed — -15% correction", '["BTC"]', 7, "negative"),

    # ══════ 2025 Q1 ══════
    ("2025-01-02", "market_event", "BTC recovers to $97K — new year optimism", '["BTC"]', 5, "positive"),
    ("2025-01-06", "political", "Trump takes strong pro-crypto stance in interviews — 'Strategic reserve'", '["BTC"]', 7, "positive"),
    ("2025-01-20", "political", "Trump inaugurated — crypto rallies on pro-crypto administration", '["BTC","ETH","SOL"]', 9, "positive"),
    ("2025-01-21", "political", "Trump signs executive order establishing crypto regulatory framework", '["BTC","ETH"]', 8, "positive"),
    ("2025-01-22", "market_milestone", "BTC hits $106K on inaugural week momentum", '["BTC"]', 7, "positive"),
    ("2025-01-29", "macro", "Fed holds rates at 4.25-4.50% — wait-and-see approach", '["BTC"]', 5, "mixed"),
    ("2025-02-03", "political", "Trump announces 25% tariffs on Canada/Mexico — risk-off", '["BTC","ETH"]', 7, "negative"),
    ("2025-02-03", "market_event", "BTC drops 7% to $92K on tariff shock — crypto correlated with stocks", '["BTC"]', 7, "negative"),
    ("2025-02-04", "political", "Trump pauses tariffs for 1 month — markets recover", '["BTC"]', 6, "positive"),
    ("2025-02-12", "macro", "CPI comes in hot at 3.0% — rate cut expectations pushed back", '["BTC"]', 6, "negative"),
    ("2025-02-21", "hack", "Bybit hacked for $1.5B — largest crypto hack in history (Lazarus group)", '["ETH","BTC"]', 9, "negative"),
    ("2025-02-24", "market_event", "BTC drops below $88K on Bybit hack + tariff fears", '["BTC"]', 7, "negative"),
    ("2025-03-02", "political", "Trump announces US Strategic Bitcoin Reserve using seized BTC", '["BTC"]', 9, "positive"),
    ("2025-03-03", "political", "Trump also announces Digital Asset Stockpile (ETH, SOL, XRP, ADA)", '["ETH","SOL","XRP","ADA"]', 8, "positive"),
    ("2025-03-04", "market_event", "Sell the news — BTC drops from $94K to $85K after reserve details disappoint", '["BTC"]', 7, "negative"),
    ("2025-03-07", "political", "White House Crypto Summit — industry meets administration", '["BTC","ETH"]', 7, "positive"),
    ("2025-03-10", "market_event", "BTC drops below $80K — tariff escalation + macro fears", '["BTC"]', 7, "negative"),
    ("2025-03-12", "macro", "CPI cools to 2.8% — better than expected, rate cut hopes revive", '["BTC"]', 6, "positive"),
    ("2025-03-19", "macro", "Fed holds rates, projects 2 cuts in 2025 — slightly hawkish", '["BTC"]', 6, "mixed"),

    # ══════ MAJOR HACKS (not yet listed above) ══════
    ("2020-09-26", "hack", "KuCoin hacked for $281M — funds later partially recovered", '["BTC","ETH"]', 6, "negative"),
    ("2021-08-10", "hack", "Poly Network $611M hack — white hat returned funds", '["ETH"]', 7, "negative"),
    ("2022-02-02", "hack", "Wormhole bridge hack — $320M stolen (Solana-Ethereum)", '["SOL","ETH"]', 7, "negative"),
    ("2022-03-29", "hack", "Ronin/Axie Infinity bridge hack — $625M stolen by Lazarus Group", '["ETH"]', 8, "negative"),
    ("2022-06-24", "hack", "Harmony bridge hack — $100M stolen", '["ETH"]', 5, "negative"),
    ("2023-03-13", "hack", "Euler Finance hack — $197M stolen, later returned", '["ETH"]', 5, "negative"),
    ("2023-07-22", "hack", "Curve Finance exploit — $73M at risk, CRV dumps 30%", '["CRV","ETH"]', 6, "negative"),
    ("2023-09-12", "hack", "CoinEx hot wallet hack — $55M stolen", '["BTC","ETH"]', 5, "negative"),
    ("2023-11-10", "hack", "Poloniex hack — $126M stolen", '["ETH"]', 5, "negative"),
    ("2024-02-09", "hack", "PlayDapp exploit — $290M in PLA tokens minted", '["ETH"]', 5, "negative"),

    # ══════ MAJOR UPGRADES ══════
    ("2020-12-01", "upgrade", "Ethereum Beacon Chain (Phase 0) launches — ETH 2.0 begins", '["ETH"]', 7, "positive"),
    ("2021-04-15", "upgrade", "Ethereum Berlin upgrade — gas optimizations", '["ETH"]', 4, "positive"),
    ("2023-04-12", "upgrade", "Ethereum Shanghai/Capella — ETH staking withdrawals enabled", '["ETH"]', 7, "positive"),
    ("2023-10-04", "upgrade", "Solana Firedancer validator client testnet — Solana scaling", '["SOL"]', 5, "positive"),
    ("2024-03-13", "upgrade", "Ethereum Dencun upgrade (EIP-4844) — proto-danksharding, L2 fees drop 90%", '["ETH","ARB","OP"]', 7, "positive"),
    ("2024-09-09", "upgrade", "Solana v1.18 upgrade — improved performance", '["SOL"]', 4, "positive"),

    # ══════ DEFI MILESTONES ══════
    ("2020-11-03", "defi", "DeFi TVL crosses $10B for first time", '["ETH"]', 6, "positive"),
    ("2021-04-28", "defi", "DeFi TVL crosses $100B", '["ETH"]', 7, "positive"),
    ("2021-11-09", "defi", "DeFi TVL hits $180B ATH", '["ETH"]', 7, "positive"),
    ("2023-12-18", "defi", "Solana DeFi TVL surges to $1.5B — Jito airdrop mania", '["SOL"]', 6, "positive"),

    # ══════ MEME COIN EVENTS ══════
    ("2021-01-28", "social", "Dogecoin pumps 800% in 24h — Reddit/WallStreetBets mania", '["DOGE"]', 8, "positive"),
    ("2021-04-16", "social", "DOGE hits $0.40 — Elon on SNL hype, Coinbase listing rumors", '["DOGE"]', 7, "positive"),
    ("2021-05-08", "social", "DOGE hits ATH $0.74 — Elon Musk SNL appearance", '["DOGE"]', 8, "positive"),
    ("2021-05-09", "social", "DOGE crashes 40% during Elon SNL show — 'sell the news'", '["DOGE"]', 7, "negative"),
    ("2021-10-28", "social", "SHIB pumps 300% in a month — 'DOGE killer' narrative", '["SHIB"]', 6, "positive"),
    ("2023-11-15", "social", "BONK surges 1000% — Solana meme coin mania begins", '["BONK","SOL"]', 6, "positive"),
    ("2024-03-14", "social", "PEPE hits new ATH — meme supercycle narrative", '["PEPE"]', 5, "positive"),

    # ══════ INSTITUTIONAL WAVE 2024-2025 ══════
    ("2024-01-30", "institutional", "BlackRock IBIT becomes largest BTC ETF, surpassing GBTC", '["BTC"]', 7, "positive"),
    ("2024-02-20", "institutional", "BTC ETF net inflows hit $5B in 5 weeks — unprecedented demand", '["BTC"]', 7, "positive"),
    ("2024-03-12", "institutional", "BTC ETFs now hold 4% of all BTC supply", '["BTC"]', 6, "positive"),
    ("2024-05-02", "institutional", "Wisconsin pension fund discloses $163M in IBIT — state pension buying BTC", '["BTC"]', 6, "positive"),
    ("2024-08-13", "institutional", "Morgan Stanley allows advisors to offer BTC ETFs to clients", '["BTC"]', 7, "positive"),
    ("2024-11-11", "institutional", "IBIT options launch — $2B notional volume day 1", '["BTC"]', 7, "positive"),
    ("2024-11-21", "institutional", "MicroStrategy announces $2.6B BTC purchase — largest single buy ever", '["BTC"]', 8, "positive"),
    ("2025-01-06", "institutional", "MicroStrategy now holds 446,400 BTC ($42B+)", '["BTC"]', 7, "positive"),
    ("2025-02-06", "institutional", "Abu Dhabi sovereign wealth fund ($1.5T) discloses BTC ETF holding", '["BTC"]', 8, "positive"),

    # ══════ SOL/ALT SPECIFIC ══════
    ("2022-11-13", "market_event", "SOL drops to $12 from $260 ATH — 95% drawdown, FTX exposure", '["SOL"]', 8, "negative"),
    ("2023-10-20", "market_event", "SOL breaks $30 — recovery begins after FTX collapse", '["SOL"]', 6, "positive"),
    ("2023-12-25", "market_event", "SOL hits $120 — 10x from November low", '["SOL"]', 7, "positive"),
    ("2024-03-18", "market_event", "SOL hits $210 ATH — full recovery from FTX", '["SOL"]', 7, "positive"),
    ("2024-11-23", "market_milestone", "SOL hits $264 new ATH — outperforming ETH", '["SOL"]', 7, "positive"),

    # ══════ AI CRYPTO NARRATIVE ══════
    ("2023-02-01", "narrative", "ChatGPT hits 100M users — AI crypto tokens surge (FET, RNDR, AGIX)", '["FET","RENDER"]', 7, "positive"),
    ("2024-02-22", "narrative", "NVIDIA earnings blow out — AI crypto tokens pump 20-50%", '["FET","RENDER","TAO"]', 7, "positive"),
    ("2024-03-28", "narrative", "FET/AGIX/OCEAN merge into ASI Alliance — AI token consolidation", '["FET"]', 6, "positive"),
    ("2024-06-18", "narrative", "NVIDIA becomes most valuable company — AI narrative boosts RNDR, TAO", '["RENDER","TAO"]', 6, "positive"),
]


def insert_events(conn):
    """Insert all historical events."""
    inserted = 0
    skipped = 0
    for ev in EVENTS:
        try:
            conn.execute(
                "INSERT OR IGNORE INTO crypto_events "
                "(date, event_type, description, coins_affected, severity, sentiment) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ev
            )
            if conn.total_changes > inserted + skipped:
                inserted += 1
            else:
                skipped += 1
        except sqlite3.IntegrityError:
            skipped += 1

    conn.commit()
    print(f"[OK] Inserted {inserted} events, skipped {skipped} duplicates")
    print(f"     Total events in DB: {conn.execute('SELECT COUNT(*) FROM crypto_events').fetchone()[0]}")


# ═══════════════════════════════════════════════════════
# STEP 3: BACKFILL HISTORICAL BTC PRICES FROM COINGECKO
# ═══════════════════════════════════════════════════════

def backfill_prices(conn):
    """Backfill daily BTC prices from CoinGecko for 2020-01 to 2021-03."""
    # Check existing data
    earliest = conn.execute(
        "SELECT MIN(timestamp) FROM prices WHERE coin='BTC' AND timeframe='1d'"
    ).fetchone()[0]

    if earliest:
        from datetime import datetime
        earliest_dt = datetime.fromtimestamp(earliest)
        print(f"Earliest BTC daily price: {earliest_dt}")

        if earliest_dt.year <= 2020 and earliest_dt.month <= 1:
            print("[SKIP] Already have prices from early 2020")
            return

    print("Backfilling BTC daily prices from CoinGecko (2019-12 to 2021-04)...")

    # CoinGecko market_chart/range — max 365 days per call (free tier)
    coins = {
        'bitcoin': 'BTC',
        'ethereum': 'ETH',
        'solana': 'SOL',
        'binancecoin': 'BNB',
        'ripple': 'XRP',
        'cardano': 'ADA',
        'dogecoin': 'DOGE',
    }

    # Fetch in 90-day chunks to respect rate limits
    start = int(datetime(2019, 12, 1, tzinfo=timezone.utc).timestamp())
    end = int(datetime(2021, 4, 1, tzinfo=timezone.utc).timestamp())

    for cg_id, symbol in coins.items():
        print(f"\n  Fetching {symbol}...")
        chunk_start = start
        total_inserted = 0

        while chunk_start < end:
            chunk_end = min(chunk_start + 86400 * 90, end)

            url = (
                f"https://api.coingecko.com/api/v3/coins/{cg_id}/market_chart/range"
                f"?vs_currency=usd&from={chunk_start}&to={chunk_end}"
            )

            try:
                req = urllib.request.Request(url, headers={
                    'User-Agent': 'FORTIX/1.0',
                    'Accept': 'application/json'
                })
                resp = urllib.request.urlopen(req, timeout=30)
                data = json.loads(resp.read())

                prices = data.get('prices', [])
                for ts_ms, price in prices:
                    ts = int(ts_ms / 1000)
                    # Round to nearest day start
                    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                    day_ts = int(dt.replace(hour=0, minute=0, second=0).timestamp())

                    try:
                        conn.execute(
                            "INSERT OR IGNORE INTO prices (coin, timestamp, timeframe, open, high, low, close, volume) "
                            "VALUES (?, ?, '1d', ?, ?, ?, ?, 0)",
                            (symbol, day_ts, price, price, price, price)
                        )
                        total_inserted += 1
                    except:
                        pass

                conn.commit()

            except urllib.error.HTTPError as e:
                if e.code == 429:
                    print(f"    Rate limited, waiting 60s...")
                    time.sleep(60)
                    continue
                else:
                    print(f"    HTTP Error {e.code}: {e.reason}")
            except Exception as e:
                print(f"    Error: {e}")

            chunk_start = chunk_end
            time.sleep(6)  # CoinGecko free: ~10 calls/min

        print(f"    {symbol}: inserted {total_inserted} price points")

    # Verify
    for symbol in coins.values():
        r = conn.execute(
            "SELECT MIN(timestamp), MAX(timestamp), COUNT(*) "
            "FROM prices WHERE coin=? AND timeframe='1d'",
            (symbol,)
        ).fetchone()
        if r[0]:
            print(f"  {symbol}: {datetime.fromtimestamp(r[0]).date()} to "
                  f"{datetime.fromtimestamp(r[1]).date()}, {r[2]} rows")


# ═══════════════════════════════════════════════════════
# STEP 4: CALCULATE PRICE IMPACT FOR EACH EVENT
# ═══════════════════════════════════════════════════════

def calculate_impacts(conn):
    """Calculate 1h, 24h, 7d price impact for each event using BTC prices."""
    events = conn.execute(
        "SELECT id, date, coins_affected FROM crypto_events WHERE impact_24h IS NULL"
    ).fetchall()

    if not events:
        print("[SKIP] All events already have impact calculated")
        return

    print(f"Calculating impacts for {len(events)} events...")

    updated = 0
    for eid, date_str, coins_json in events:
        try:
            # Parse event date
            event_dt = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            event_ts = int(event_dt.timestamp())

            # Use BTC as primary impact reference
            coins = json.loads(coins_json) if coins_json else ['BTC']
            primary_coin = coins[0] if coins else 'BTC'

            # Get price at event time (closest daily candle)
            price_at = conn.execute(
                "SELECT close FROM prices WHERE coin=? AND timeframe='1d' "
                "AND timestamp BETWEEN ? AND ? ORDER BY ABS(timestamp - ?) LIMIT 1",
                (primary_coin, event_ts - 86400, event_ts + 86400, event_ts)
            ).fetchone()

            if not price_at or not price_at[0]:
                # Try BTC if primary coin not available
                if primary_coin != 'BTC':
                    price_at = conn.execute(
                        "SELECT close FROM prices WHERE coin='BTC' AND timeframe='1d' "
                        "AND timestamp BETWEEN ? AND ? ORDER BY ABS(timestamp - ?) LIMIT 1",
                        (event_ts - 86400, event_ts + 86400, event_ts)
                    ).fetchone()
                    primary_coin = 'BTC'

                if not price_at or not price_at[0]:
                    continue

            price_0 = price_at[0]

            # 1-hour impact (use 1h or 4h candles if available)
            impact_1h = None
            p1h = conn.execute(
                "SELECT close FROM prices WHERE coin=? "
                "AND timestamp BETWEEN ? AND ? ORDER BY timestamp LIMIT 1",
                (primary_coin, event_ts + 3600, event_ts + 7200)
            ).fetchone()
            if p1h and p1h[0]:
                impact_1h = (p1h[0] - price_0) / price_0 * 100

            # 24-hour impact
            impact_24h = None
            p24h = conn.execute(
                "SELECT close FROM prices WHERE coin=? AND timeframe='1d' "
                "AND timestamp BETWEEN ? AND ? ORDER BY ABS(timestamp - ?) LIMIT 1",
                (primary_coin, event_ts + 72000, event_ts + 172800,
                 event_ts + 86400)
            ).fetchone()
            if p24h and p24h[0]:
                impact_24h = (p24h[0] - price_0) / price_0 * 100

            # 7-day impact
            impact_7d = None
            p7d = conn.execute(
                "SELECT close FROM prices WHERE coin=? AND timeframe='1d' "
                "AND timestamp BETWEEN ? AND ? ORDER BY ABS(timestamp - ?) LIMIT 1",
                (primary_coin, event_ts + 518400, event_ts + 777600,
                 event_ts + 604800)
            ).fetchone()
            if p7d and p7d[0]:
                impact_7d = (p7d[0] - price_0) / price_0 * 100

            # Update
            conn.execute(
                "UPDATE crypto_events SET impact_1h=?, impact_24h=?, impact_7d=?, "
                "btc_price_at_event=? WHERE id=?",
                (impact_1h, impact_24h, impact_7d, price_0, eid)
            )
            updated += 1

        except Exception as e:
            print(f"  Error for event {eid} ({date_str}): {e}")

    conn.commit()
    print(f"[OK] Updated {updated}/{len(events)} events with price impacts")

    # Show distribution
    print("\n=== IMPACT BY EVENT TYPE ===")
    types = conn.execute(
        "SELECT event_type, COUNT(*), "
        "AVG(impact_24h), MIN(impact_24h), MAX(impact_24h), "
        "AVG(impact_7d), AVG(severity) "
        "FROM crypto_events WHERE impact_24h IS NOT NULL "
        "GROUP BY event_type ORDER BY COUNT(*) DESC"
    ).fetchall()
    print(f"{'Type':<16} {'Count':>5} {'Avg 24h':>8} {'Min 24h':>8} {'Max 24h':>8} {'Avg 7d':>8} {'Avg Sev':>8}")
    print("-" * 75)
    for t in types:
        print(f"{t[0]:<16} {t[1]:>5} {t[2]:>+7.1f}% {t[3]:>+7.1f}% {t[4]:>+7.1f}% {t[5]:>+7.1f}% {t[6]:>7.1f}")


# ═══════════════════════════════════════════════════════
# STEP 5: SUMMARY & VALIDATION
# ═══════════════════════════════════════════════════════

def print_summary(conn):
    total = conn.execute("SELECT COUNT(*) FROM crypto_events").fetchone()[0]
    with_impact = conn.execute("SELECT COUNT(*) FROM crypto_events WHERE impact_24h IS NOT NULL").fetchone()[0]
    types = conn.execute(
        "SELECT event_type, COUNT(*) FROM crypto_events GROUP BY event_type ORDER BY COUNT(*) DESC"
    ).fetchall()
    years = conn.execute(
        "SELECT SUBSTR(date,1,4) as yr, COUNT(*) FROM crypto_events GROUP BY yr ORDER BY yr"
    ).fetchall()

    print(f"\n{'='*60}")
    print(f"CRYPTO EVENTS DATABASE SUMMARY")
    print(f"{'='*60}")
    print(f"Total events:      {total}")
    print(f"With price impact: {with_impact}")
    print(f"\nBy type:")
    for t in types:
        print(f"  {t[0]:<20} {t[1]:>4}")
    print(f"\nBy year:")
    for y in years:
        print(f"  {y[0]}: {y[1]:>4} events")

    # Top 10 biggest impacts
    print(f"\n=== TOP 10 BIGGEST 24h IMPACTS ===")
    top = conn.execute(
        "SELECT date, event_type, description, impact_24h FROM crypto_events "
        "WHERE impact_24h IS NOT NULL ORDER BY ABS(impact_24h) DESC LIMIT 10"
    ).fetchall()
    for t in top:
        print(f"  {t[0]} [{t[1]:>12}] {t[3]:>+6.1f}%  {t[2][:60]}")


# ═══════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════

def main():
    conn = sqlite3.connect(str(DB_PATH), timeout=30)

    mode = sys.argv[1] if len(sys.argv) > 1 else '--all'

    if mode in ('--all', '--events'):
        create_table(conn)
        insert_events(conn)

    if mode in ('--all', '--backfill'):
        backfill_prices(conn)

    if mode in ('--all', '--impacts'):
        calculate_impacts(conn)

    print_summary(conn)
    conn.close()


if __name__ == '__main__':
    main()
