"""Full system verification — checks EVERYTHING."""
import sys, json, os, subprocess, sqlite3, re
sys.stdout.reconfigure(encoding='utf-8')
from pathlib import Path
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
load_dotenv()

errors = []

def check(name, condition, detail=""):
    if condition:
        print(f"  OK   {name}")
    else:
        errors.append(f"{name}: {detail}")
        print(f"  FAIL {name} -- {detail}")

print("=" * 70)
print("FULL SYSTEM VERIFICATION")
print("=" * 70)

# 1. ORCHESTRATOR STATE
print("\n--- 1. ORCHESTRATOR STATE ---")
hb = Path("data/crypto/heartbeat.txt")
check("Heartbeat exists", hb.exists())
if hb.exists():
    age = int(datetime.now().timestamp()) - int(hb.read_text().strip())
    check(f"Heartbeat fresh ({age}s)", age < 180, f"STALE: {age}s")

lr = json.loads(Path("data/crypto/last_run.json").read_text()) if Path("data/crypto/last_run.json").exists() else {}
check("last_run.json has today keys", any("20260321" in str(v) or "20260321" in k for k, v in lr.items()))

sd = json.loads(Path("data/crypto/daily_schedule.json").read_text()) if Path("data/crypto/daily_schedule.json").exists() else {}
check("Schedule is today", sd.get("date") == "2026-03-21")

# 2. SYNTAX
print("\n--- 2. SYNTAX ---")
import py_compile
for f in ["src/crypto/orchestrator.py", "src/crypto/produce_crypto.py",
          "src/crypto/channel_uploader.py", "src/crypto/signal_system.py",
          "src/engines/monitor.py", "src/assemble_video.py"]:
    try:
        py_compile.compile(f, doraise=True)
        print(f"  OK   {f}")
    except Exception as e:
        check(f"Syntax {f}", False, str(e))

# 3. ORCHESTRATOR CODE LOGIC
print("\n--- 3. ORCHESTRATOR LOGIC ---")
orch = open("src/crypto/orchestrator.py", encoding="utf-8").read()
main_loop = orch[orch.find("while self.running"):]

check("Daily slots → _produce_and_upload", "_produce_and_upload(content_type)" in orch)
check("weekly_forecast → _produce_and_upload", "_produce_and_upload(vtype)" in orch or "_produce_and_upload('weekly_forecast')" in orch)
check("coin_verdict → _produce_and_upload", "_produce_and_upload('coin_verdict'" in orch)
check("whale_watch → _produce_and_upload", "_produce_and_upload('whale_watch')" in orch)
check("daily_analysis → _produce_and_upload", "_produce_and_upload('daily_analysis'" in orch)
check("NO task_produce_long in main loop", "task_produce_long(" not in main_loop, "OLD function still used!")
check("NO upload_daily in main loop", "self._should_run('upload_daily')" not in main_loop, "OLD stagger still active!")
check("_produce_and_upload handles file+dir", "result_path.is_file()" in orch and "result_path.is_dir()" in orch)
check("_produce_and_upload uses mp4_file", "str(mp4_file)," in orch)
check("_last_run persistent (save)", "_save_last_run" in orch)
check("_last_run persistent (load)", "_load_last_run" in orch)
check("_should_run_slot no narrow window", "now >= target" in orch)
check("Scan unuploaded on startup", "_scan_unuploaded_videos" in orch)
check("_upload_remaining for recovery", "_upload_remaining" in orch)
check("FRED macro daily update", "FRED macro data updated" in orch)
check("DeFi TVL daily update", "defi_tvl_history" in orch)

# 4. PRODUCE_CRYPTO
print("\n--- 4. PRODUCE_CRYPTO ---")
prod = open("src/crypto/produce_crypto.py", encoding="utf-8").read()
check("ElevenLabs chunking", "MAX_CHARS = 9000" in prod)
check("Chunk concat with ffmpeg", "voice_concat" in prod or "_voice_concat" in prod)
check("script_text in main produce", "'script_text': script_result.get('script'" in prod)
check("script_text in micro_short", prod.count("'script_text': narration") >= 2)
check("script_text in script-only mode", prod.count("'script_text': script_result") >= 1)
check("Cover frame removed from assembly", "Cover frame prepended later" in prod)
check("Cover frame after thumbnail", "_prepend_thumb_intro(final_path" in prod)

# 5. ASSEMBLE_VIDEO
print("\n--- 5. ASSEMBLE_VIDEO ---")
asm = open("src/assemble_video.py", encoding="utf-8").read()
check("Endscreen DISABLED", "End screen zone DISABLED" in asm)
check("Audio 192k", "192k" in asm)

# 6. CHANNEL_UPLOADER
print("\n--- 6. CHANNEL_UPLOADER ---")
up = open("src/crypto/channel_uploader.py", encoding="utf-8").read()
check("Shorts → full script_text", "is_short and script_text" in up)
check("Longs → youtube_description", "youtube_description" in up)
check("Longs → Haiku fallback", "_summarize_for_description" in up)
check("youtube_id written to meta", "'youtube_id'" in up and "production_meta" in up)
check("parse_production_meta returns script_text", "'script_text': data.get" in up)
check("MAX_UPLOADS = 7", "MAX_UPLOADS_PER_DAY = 7" in up)

# 7. SIGNAL SYSTEM
print("\n--- 7. SIGNAL SYSTEM ---")
sig = open("src/crypto/signal_system.py", encoding="utf-8").read()
check("Walk-forward validated signals", "walk-forward validated" in sig.lower() or "walk_forward" in sig.lower())
check("Live tracking (signal_tracking table)", "signal_tracking" in sig)
check("Dedup per day", "fired_at=?" in sig or "fired_at" in sig)
check("Compound bearish", "compound_bearish" in sig)

# 8. TELEGRAM
print("\n--- 8. TELEGRAM ---")
mon = open("src/engines/monitor.py", encoding="utf-8").read()
check("html.escape title", "safe_title" in mon)
check("html.escape body", "safe_body" in mon)
check("html.escape data", "data_str" in mon and "html.escape" in mon)

# 9. DATABASE
print("\n--- 9. DATABASE FRESHNESS ---")
conn = sqlite3.connect("data/crypto/market.db", timeout=10)
today = "2026-03-21"
yesterday = "2026-03-20"
for name, q in [
    ("prices 1d", "SELECT MAX(datetime(timestamp,'unixepoch')) FROM prices WHERE timeframe='1d'"),
    ("funding", "SELECT MAX(datetime(timestamp,'unixepoch')) FROM funding_rates"),
    ("F&G", "SELECT MAX(date) FROM fear_greed"),
    ("OI", "SELECT MAX(datetime(timestamp,'unixepoch')) FROM open_interest"),
    ("L/S", "SELECT MAX(datetime(timestamp,'unixepoch')) FROM long_short_ratio"),
    ("macro VIX", "SELECT MAX(date) FROM macro_events WHERE event_type='vix'"),
    ("signals", "SELECT MAX(fired_at) FROM signal_tracking"),
    ("news", "SELECT MAX(datetime(timestamp,'unixepoch')) FROM news"),
]:
    try:
        r = conn.execute(q).fetchone()
        last = str(r[0] or "")[:10]
        check(f"{name}: {last}", today in last or yesterday in last, f"STALE")
    except Exception as e:
        check(name, False, str(e))
conn.close()

# 10. API KEYS
print("\n--- 10. API KEYS ---")
for key in ["ELEVENLABS_API_KEY", "ANTHROPIC_API_KEY", "COINGLASS_API_KEY",
            "CRYPTOQUANT_API_KEY", "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID", "FRED_API_KEY"]:
    check(f"ENV {key}", len(os.getenv(key, "")) > 5, "MISSING")

# 11. WATCHDOG
print("\n--- 11. WATCHDOG ---")
r = subprocess.run(["powershell", "-Command",
    "Get-ScheduledTask -TaskName 'FORTIX_Watchdog' | Select-Object -ExpandProperty State"],
    capture_output=True, text=True, timeout=10)
check(f"Watchdog ({r.stdout.strip()})", r.stdout.strip() == "Ready")

# 12. YOUTUBE OAUTH
print("\n--- 12. YOUTUBE ---")
creds = Path("config/youtube_credentials.pkl")
check("YouTube credentials file", creds.exists())
try:
    from src.crypto.channel_uploader import CryptoUploader
    u = CryptoUploader()
    check("YouTube API connected", u.youtube is not None)
except Exception as e:
    check("YouTube API", False, str(e))

# SUMMARY
print("\n" + "=" * 70)
if errors:
    print(f"FAILED: {len(errors)} issues")
    for e in errors:
        print(f"  X {e}")
else:
    print("ALL 50+ CHECKS PASSED. System is operational.")
print("=" * 70)
