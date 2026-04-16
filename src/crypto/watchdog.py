"""
FORTIX Watchdog — Monitors orchestrator heartbeat, auto-restarts on hang.

Cross-platform: works on macOS (launchd/cron) and Windows (Task Scheduler).

    python src/crypto/watchdog.py

How it works:
1. Reads heartbeat.txt (written by orchestrator every 60s)
2. If heartbeat is stale (>30 min old) → kill orchestrator → restart
3. If no heartbeat file → start orchestrator
4. Sends Telegram alert on restart
"""

import os
import sys
import time
import platform
import subprocess
import logging
from pathlib import Path
from datetime import datetime, timezone

FACTORY_DIR = Path(__file__).resolve().parent.parent.parent

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [WATCHDOG] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(FACTORY_DIR / 'logs' / 'watchdog.log', encoding='utf-8'),
    ]
)
log = logging.getLogger('watchdog')

HEARTBEAT_PATH = FACTORY_DIR / 'data' / 'crypto' / 'heartbeat.txt'
STALE_THRESHOLD_SEC = 3600  # 60 minutes (Binance futures retries can take 35+ min)
ORCHESTRATOR_CMD = [sys.executable, '-m', 'src.crypto.orchestrator']
IS_MACOS = platform.system() == 'Darwin'
IS_WINDOWS = platform.system() == 'Windows'


def _notify(title: str, message: str):
    """Send Telegram alert."""
    try:
        from dotenv import load_dotenv
        load_dotenv(FACTORY_DIR / '.env')
        bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        chat_id = os.getenv('TELEGRAM_CHAT_ID')
        if bot_token and chat_id:
            import requests
            requests.post(
                f'https://api.telegram.org/bot{bot_token}/sendMessage',
                json={'chat_id': chat_id, 'text': f'🔧 {title}\n{message}',
                      'parse_mode': 'HTML'},
                timeout=10
            )
    except Exception:
        pass


def _is_orchestrator_running() -> bool:
    """Check if orchestrator process is running."""
    try:
        if IS_MACOS:
            result = subprocess.run(
                ['pgrep', '-f', 'src.crypto.orchestrator'],
                capture_output=True, text=True, timeout=10
            )
            pids = [p for p in result.stdout.strip().split('\n') if p.strip()]
            return len(pids) > 0
        else:
            result = subprocess.run(
                ['tasklist', '/FI', 'IMAGENAME eq python.exe', '/FO', 'CSV'],
                capture_output=True, text=True, timeout=10
            )
            lines = [l for l in result.stdout.strip().split('\n')
                     if 'python' in l.lower()]
            return len(lines) >= 2  # watchdog + orchestrator
    except Exception:
        return False


def _kill_orchestrator():
    """Kill hung orchestrator processes."""
    log.warning("Killing hung orchestrator processes...")
    try:
        if IS_MACOS:
            result = subprocess.run(
                ['pgrep', '-f', 'src.crypto.orchestrator'],
                capture_output=True, text=True, timeout=10
            )
            for pid in result.stdout.strip().split('\n'):
                pid = pid.strip()
                if pid and pid != str(os.getpid()):
                    try:
                        subprocess.run(['kill', '-9', pid], timeout=5)
                        log.info(f"  Killed PID {pid}")
                    except Exception:
                        pass
        else:
            result = subprocess.run(
                ['wmic', 'process', 'where', "name='python.exe'",
                 'get', 'processid,commandline'],
                capture_output=True, text=True, timeout=10
            )
            for line in result.stdout.strip().split('\n'):
                if 'orchestrator' in line.lower() and 'watchdog' not in line.lower():
                    parts = line.strip().split()
                    if parts:
                        pid = parts[-1]
                        try:
                            subprocess.run(['taskkill', '/F', '/PID', pid], timeout=5)
                            log.info(f"  Killed PID {pid}")
                        except Exception:
                            pass
    except Exception as e:
        log.error(f"Failed to kill processes: {e}")


def _start_orchestrator():
    """Start orchestrator as detached background process."""
    log.info("Starting orchestrator...")
    try:
        log_stdout = open(FACTORY_DIR / 'logs' / 'orchestrator_stdout.log', 'a')
        log_stderr = open(FACTORY_DIR / 'logs' / 'orchestrator_stderr.log', 'a')

        if IS_MACOS:
            proc = subprocess.Popen(
                ORCHESTRATOR_CMD,
                cwd=str(FACTORY_DIR),
                stdout=log_stdout,
                stderr=log_stderr,
                start_new_session=True,
            )
        else:
            proc = subprocess.Popen(
                ORCHESTRATOR_CMD,
                cwd=str(FACTORY_DIR),
                stdout=log_stdout,
                stderr=log_stderr,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS,
            )
        log.info(f"  Orchestrator started (PID {proc.pid})")
        return proc.pid
    except Exception as e:
        log.error(f"Failed to start orchestrator: {e}")
        return None


def check_and_restart():
    """Main watchdog logic."""
    now_ts = int(time.time())

    # Check heartbeat file
    if not HEARTBEAT_PATH.exists():
        log.warning("No heartbeat file found — orchestrator may not be running")
        _notify("⚠️ Оркестратор не відповідає",
                "Файл активності відсутній. Запускаю оркестратор...")
        pid = _start_orchestrator()
        if pid:
            _notify("✅ Оркестратор запущено", f"PID: {pid}")
        return

    # Read heartbeat timestamp
    try:
        hb_text = HEARTBEAT_PATH.read_text().strip()
        if not hb_text:
            log.warning("Heartbeat file is empty")
            return
        hb_ts = int(hb_text)
    except (ValueError, OSError) as e:
        log.error(f"Invalid heartbeat file: {e}")
        return

    age_sec = now_ts - hb_ts
    age_min = age_sec / 60

    if age_sec > STALE_THRESHOLD_SEC:
        log.critical(f"STALE HEARTBEAT: {age_min:.1f} min old "
                     f"(threshold: {STALE_THRESHOLD_SEC/60:.0f} min)")
        _notify("🚨 Оркестратор завис",
                f"Останній раз працював: {age_min:.1f} хв тому\nЗупиняю і перезапускаю...")

        _kill_orchestrator()
        time.sleep(5)  # Wait for processes to die
        pid = _start_orchestrator()

        if pid:
            _notify("✅ Оркестратор перезапущено", f"Новий PID: {pid}")
            log.info(f"Orchestrator restarted (PID {pid})")
        else:
            _notify("🔴 Не вдалося перезапустити", "Потрібне ручне втручання!")
            log.critical("FAILED to restart orchestrator!")
    else:
        log.info(f"Heartbeat OK — {age_sec}s ago ({age_min:.1f} min)")


if __name__ == '__main__':
    (FACTORY_DIR / 'logs').mkdir(exist_ok=True)
    log.info("=" * 40)
    log.info("FORTIX Watchdog check")
    check_and_restart()
