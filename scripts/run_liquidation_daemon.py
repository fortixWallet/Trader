"""
Alpha Signal — Liquidation Listener Daemon (24/7)
==================================================
Watchdog wrapper that keeps the liquidation WebSocket listener
running continuously. Auto-restarts on crash. Logs to file.

Usage:
    python scripts/run_liquidation_daemon.py

Designed to run via Windows Task Scheduler at system startup.
"""

import sys
import time
import logging
from pathlib import Path
from datetime import datetime

# Project root
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Log to file (rotate daily via naming)
LOG_DIR = ROOT / 'logs'
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / 'liquidation_daemon.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(str(LOG_FILE), encoding='utf-8'),
        logging.StreamHandler(),
    ]
)
log = logging.getLogger('liq_daemon')

# PID file to prevent duplicate instances
PID_FILE = ROOT / 'logs' / 'liquidation_daemon.pid'


def write_pid():
    """Write current PID to file."""
    import os
    PID_FILE.write_text(str(os.getpid()))


def check_already_running() -> bool:
    """Check if another instance is already running."""
    import os
    if not PID_FILE.exists():
        return False
    try:
        old_pid = int(PID_FILE.read_text().strip())
        # Check if process exists (Windows)
        import ctypes
        kernel32 = ctypes.windll.kernel32
        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, old_pid)
        if handle:
            kernel32.CloseHandle(handle)
            return True
        return False
    except Exception:
        return False


def main():
    if check_already_running():
        log.warning("Liquidation daemon already running, exiting.")
        return

    write_pid()
    log.info("=" * 55)
    log.info("  ALPHA SIGNAL — Liquidation Daemon Starting")
    log.info(f"  PID: {PID_FILE.read_text().strip()}")
    log.info(f"  Log: {LOG_FILE}")
    log.info("=" * 55)

    # Ensure DB tables exist
    from src.crypto.data_collector import init_db
    init_db()

    from src.crypto.liquidation_listener import LiquidationListener

    consecutive_crashes = 0
    max_consecutive = 10

    while True:
        listener = LiquidationListener()
        try:
            listener.start()
            log.info("Listener started successfully")
            consecutive_crashes = 0

            # Monitor loop — check health every 60s
            last_count = 0
            stale_checks = 0

            while True:
                time.sleep(60)
                current_count = listener.count

                # Log stats every 5 minutes
                if int(time.time()) % 300 < 60:
                    log.info(f"  Running: {current_count} tracked events total "
                             f"({current_count - last_count} last min)")

                # Check if WebSocket is still receiving data
                if current_count == last_count:
                    stale_checks += 1
                    if stale_checks >= 10:  # 10 min with no data
                        log.warning("No events for 10 minutes, forcing reconnect...")
                        listener.stop()
                        time.sleep(5)
                        break  # Will restart in outer loop
                else:
                    stale_checks = 0

                last_count = current_count

        except KeyboardInterrupt:
            log.info("Ctrl+C received, shutting down...")
            listener.stop()
            break

        except Exception as e:
            consecutive_crashes += 1
            log.error(f"Listener crashed ({consecutive_crashes}/{max_consecutive}): {e}")

            try:
                listener.stop()
            except Exception:
                pass

            if consecutive_crashes >= max_consecutive:
                log.error(f"Too many consecutive crashes ({max_consecutive}), giving up.")
                break

            # Exponential backoff: 10s, 20s, 40s... max 5 min
            wait = min(10 * (2 ** (consecutive_crashes - 1)), 300)
            log.info(f"Restarting in {wait}s...")
            time.sleep(wait)

    # Cleanup
    if PID_FILE.exists():
        PID_FILE.unlink()
    log.info("Liquidation daemon stopped.")


if __name__ == '__main__':
    main()
