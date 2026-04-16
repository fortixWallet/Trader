"""
FORTIX — Production Scheduler
======================================
Automated scheduling for all FORTIX video production.

Schedule:
  - Daily:     14:00 UTC — Data collection + Daily Short
  - Daily:     14:30 UTC — Daily Analysis (10-12 min, derivatives + correlations)
  - Monday:    15:00 UTC — Weekly Forecast
  - Tuesday:   15:00 UTC — Coin Verdict
  - Wednesday: 15:00 UTC — Whale Watch
  - Thursday:  15:00 UTC — Coin Verdict
  - 1st Monday:15:00 UTC — Monthly Scorecard (instead of Weekly Forecast)
  - Every 30m: Signal Monitor check

Usage:
    python src/crypto/scheduler.py                  # Show schedule
    python src/crypto/scheduler.py --run-now daily   # Run daily tasks now
    python src/crypto/scheduler.py --run-now weekly   # Run weekly forecast now
    python src/crypto/scheduler.py --daemon          # Run as daemon
    python src/crypto/scheduler.py --install         # Install Windows Task Scheduler tasks
"""

import os
import sys
import json
import time
import logging
import subprocess
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger('scheduler')

PYTHON = sys.executable
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


# ════════════════════════════════════════════
# TASK DEFINITIONS
# ════════════════════════════════════════════

DAILY_TASKS = [
    {
        'name': 'data_collection',
        'description': 'Collect market data from all sources',
        'command': [PYTHON, 'src/crypto/data_collector.py'],
        'time_utc': '14:00',
    },
    {
        'name': 'prediction_check',
        'description': 'Check matured predictions and update scorecard',
        'command': [PYTHON, 'src/crypto/prediction_tracker.py', '--report', '--chart'],
        'time_utc': '14:05',
    },
    {
        'name': 'daily_short',
        'description': 'Produce daily YouTube Short',
        'command': [PYTHON, 'src/crypto/produce_crypto.py', 'daily_short', '--skip-collect'],
        'time_utc': '14:15',
    },
    {
        'name': 'daily_analysis',
        'description': 'Daily Analysis video (derivatives, correlations, forecasts)',
        'command': [PYTHON, 'src/crypto/produce_crypto.py', 'daily_analysis', '--skip-collect'],
        'time_utc': '14:30',
    },
]

WEEKLY_TASKS = {
    'monday': {
        'name': 'weekly_forecast',
        'description': 'Weekly Forecast video',
        'command': [PYTHON, 'src/crypto/produce_crypto.py', 'weekly_forecast'],
        'time_utc': '15:00',
    },
    'tuesday': {
        'name': 'coin_verdict_tue',
        'description': 'Coin Verdict video (Tuesday)',
        'command': [PYTHON, 'src/crypto/produce_crypto.py', 'coin_verdict', '--coin', 'BTC'],
        'time_utc': '15:00',
    },
    'wednesday': {
        'name': 'whale_watch',
        'description': 'Whale Watch video',
        'command': [PYTHON, 'src/crypto/produce_crypto.py', 'whale_watch'],
        'time_utc': '15:00',
    },
    'thursday': {
        'name': 'coin_verdict_thu',
        'description': 'Coin Verdict video (Thursday)',
        'command': [PYTHON, 'src/crypto/produce_crypto.py', 'coin_verdict', '--coin', 'ETH'],
        'time_utc': '15:00',
    },
}

# Coins to rotate for Coin Verdict (changes weekly)
COIN_VERDICT_ROTATION = [
    ['BTC', 'ETH'],   # Week 1
    ['SOL', 'BNB'],   # Week 2
    ['XRP', 'ADA'],   # Week 3
    ['AVAX', 'DOT'],  # Week 4
    ['LINK', 'DOGE'], # Week 5
]


def get_coin_for_verdict(day: str) -> str:
    """Get the coin for Coin Verdict based on current week and day."""
    week_num = datetime.now().isocalendar()[1]
    rotation = COIN_VERDICT_ROTATION[week_num % len(COIN_VERDICT_ROTATION)]
    if day == 'tuesday':
        return rotation[0]
    else:  # thursday
        return rotation[1] if len(rotation) > 1 else rotation[0]


# ════════════════════════════════════════════
# EXECUTION
# ════════════════════════════════════════════

def run_task(task: dict):
    """Run a single production task."""
    log.info(f"\n{'='*50}")
    log.info(f"Running: {task['name']} — {task['description']}")
    log.info(f"Command: {' '.join(str(c) for c in task['command'])}")
    log.info(f"{'='*50}")

    try:
        result = subprocess.run(
            task['command'],
            cwd=str(PROJECT_ROOT),
            timeout=3600,  # 1 hour max per task
            capture_output=False,  # Show output in real-time
        )
        if result.returncode == 0:
            log.info(f"  {task['name']}: COMPLETED")
        else:
            log.error(f"  {task['name']}: FAILED (exit code {result.returncode})")
    except subprocess.TimeoutExpired:
        log.error(f"  {task['name']}: TIMEOUT (exceeded 1 hour)")
    except Exception as e:
        log.error(f"  {task['name']}: ERROR — {e}")


def run_daily_tasks():
    """Run all daily tasks."""
    log.info("\n" + "=" * 60)
    log.info("DAILY TASKS")
    log.info("=" * 60)

    for task in DAILY_TASKS:
        run_task(task)


def run_weekly_task(day: str = None):
    """Run the weekly task for today (or specified day)."""
    if day is None:
        day = datetime.now().strftime('%A').lower()

    log.info(f"\n{'='*60}")
    log.info(f"WEEKLY TASK — {day.capitalize()}")
    log.info(f"{'='*60}")

    task = WEEKLY_TASKS.get(day)
    if not task:
        log.info(f"  No weekly task scheduled for {day}")
        return

    # Dynamic coin selection for Coin Verdict
    if 'coin_verdict' in task['name']:
        coin = get_coin_for_verdict(day)
        task = dict(task)  # Copy to avoid modifying the template
        task['command'] = list(task['command'])
        # Replace coin argument
        if '--coin' in task['command']:
            idx = task['command'].index('--coin')
            task['command'][idx + 1] = coin
        task['description'] += f" — {coin}"
        log.info(f"  Coin Verdict rotation: {coin}")

    # First Monday of month → Monthly Scorecard instead
    now = datetime.now()
    if day == 'monday' and now.day <= 7:
        log.info("  First Monday — producing Monthly Scorecard instead of Weekly Forecast")
        task = {
            'name': 'monthly_scorecard',
            'description': 'Monthly Scorecard video',
            'command': [PYTHON, 'src/crypto/produce_crypto.py', 'weekly_forecast'],
            'time_utc': '15:00',
        }

    run_task(task)


def run_signal_monitor():
    """Run a single signal monitor check."""
    from src.crypto.signal_monitor import run_check
    run_check(dry_run=False)


def show_schedule():
    """Display the full production schedule."""
    now = datetime.now()
    day = now.strftime('%A').lower()

    print("\n" + "=" * 60)
    print("  ALPHA SIGNAL — Production Schedule")
    print("=" * 60)

    print(f"\n  Current: {now.strftime('%A, %B %d %Y %H:%M')}")
    print(f"  Today: {day.capitalize()}")

    print(f"\n  DAILY (every day):")
    for task in DAILY_TASKS:
        print(f"    {task['time_utc']} UTC — {task['description']}")

    print(f"\n  WEEKLY:")
    for day_name, task in WEEKLY_TASKS.items():
        coin = ''
        if 'coin_verdict' in task['name']:
            coin = f" — {get_coin_for_verdict(day_name)}"
        marker = ' <-- TODAY' if day_name == day else ''
        print(f"    {day_name.capitalize():>12} {task['time_utc']} UTC — {task['description']}{coin}{marker}")

    print(f"\n  CONTINUOUS:")
    print(f"    Always  — Liquidation WebSocket listener (auto-start with --daemon)")
    print(f"    Every 30m — Signal Monitor (auto-trigger on ±5% BTC, ±7% ETH, etc.)")

    print(f"\n  MONTHLY:")
    print(f"    1st Monday — Monthly Scorecard (replaces Weekly Forecast)")

    print(f"\n  Coin Verdict Rotation (this week):")
    week_num = now.isocalendar()[1]
    rotation = COIN_VERDICT_ROTATION[week_num % len(COIN_VERDICT_ROTATION)]
    print(f"    Tuesday:  {rotation[0]}")
    print(f"    Thursday: {rotation[1] if len(rotation) > 1 else rotation[0]}")

    print("\n" + "=" * 60)


def install_windows_tasks():
    """Generate Windows Task Scheduler commands."""
    print("\n" + "=" * 60)
    print("  Windows Task Scheduler Setup")
    print("=" * 60)
    print("\n  Run these commands in an elevated (Admin) PowerShell:\n")

    python_path = str(Path(PYTHON).resolve())
    project_path = str(PROJECT_ROOT)

    tasks = [
        {
            'name': 'FORTIX_DataCollection',
            'time': '14:00',
            'script': 'src/crypto/data_collector.py',
            'schedule': 'DAILY',
        },
        {
            'name': 'FORTIX_PredictionCheck',
            'time': '14:05',
            'script': 'src/crypto/prediction_tracker.py --report --chart',
            'schedule': 'DAILY',
        },
        {
            'name': 'FORTIX_DailyShort',
            'time': '14:15',
            'script': 'src/crypto/produce_crypto.py daily_short --skip-collect',
            'schedule': 'DAILY',
        },
        {
            'name': 'FORTIX_DailyAnalysis',
            'time': '14:30',
            'script': 'src/crypto/produce_crypto.py daily_analysis --skip-collect',
            'schedule': 'DAILY',
        },
        {
            'name': 'FORTIX_WeeklyForecast',
            'time': '15:00',
            'script': 'src/crypto/produce_crypto.py weekly_forecast',
            'schedule': 'WEEKLY',
            'day': 'MON',
        },
        {
            'name': 'FORTIX_WhaleWatch',
            'time': '15:00',
            'script': 'src/crypto/produce_crypto.py whale_watch',
            'schedule': 'WEEKLY',
            'day': 'WED',
        },
        {
            'name': 'FORTIX_SignalMonitor',
            'time': '00:00',
            'script': 'src/crypto/signal_monitor.py --daemon',
            'schedule': 'ONSTART',
        },
    ]

    for t in tasks:
        args = f'"{python_path}" {t["script"]}'
        schedule = t['schedule']

        if schedule == 'DAILY':
            print(f'  schtasks /create /tn "{t["name"]}" /tr "{args}" '
                  f'/sc DAILY /st {t["time"]} /f /rl HIGHEST')
        elif schedule == 'WEEKLY':
            print(f'  schtasks /create /tn "{t["name"]}" /tr "{args}" '
                  f'/sc WEEKLY /d {t["day"]} /st {t["time"]} /f /rl HIGHEST')
        elif schedule == 'ONSTART':
            print(f'  schtasks /create /tn "{t["name"]}" /tr "{args}" '
                  f'/sc ONSTART /f /rl HIGHEST')

    print(f'\n  All tasks use working directory: {project_path}')
    print(f'  Python: {python_path}')
    print("\n" + "=" * 60)


def daemon_loop():
    """Run the scheduler as a daemon."""
    log.info("FORTIX Scheduler Daemon started")
    log.info("Press Ctrl+C to stop\n")

    # Start liquidation WebSocket listener in background
    liq_listener = None
    try:
        from src.crypto.liquidation_listener import LiquidationListener
        liq_listener = LiquidationListener()
        liq_listener.start()
    except Exception as e:
        log.warning(f"Liquidation listener failed to start: {e}")

    last_daily = None
    last_weekly = None
    last_signal_check = None

    while True:
        now = datetime.now()
        current_time = now.strftime('%H:%M')
        current_day = now.strftime('%A').lower()
        today_str = now.strftime('%Y-%m-%d')

        # Daily tasks at 14:00
        if current_time >= '14:00' and last_daily != today_str:
            log.info("Triggering daily tasks...")
            run_daily_tasks()
            last_daily = today_str

        # Weekly task at 15:00
        if current_time >= '15:00' and last_weekly != today_str:
            if current_day in WEEKLY_TASKS:
                log.info(f"Triggering weekly task ({current_day})...")
                run_weekly_task(current_day)
                last_weekly = today_str

        # Signal monitor every 30 minutes
        if last_signal_check is None or (now - last_signal_check).seconds >= 1800:
            try:
                run_signal_monitor()
            except Exception as e:
                log.error(f"Signal monitor error: {e}")
            last_signal_check = now

        # Sleep 1 minute
        time.sleep(60)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='FORTIX — Production Scheduler')
    parser.add_argument('--run-now', choices=['daily', 'weekly', 'short', 'analysis', 'monitor'],
                        help='Run a task immediately')
    parser.add_argument('--day', default=None,
                        help='Override day for weekly task (monday, tuesday, etc.)')
    parser.add_argument('--daemon', action='store_true',
                        help='Run as continuous daemon')
    parser.add_argument('--install', action='store_true',
                        help='Show Windows Task Scheduler setup commands')

    args = parser.parse_args()

    if args.install:
        install_windows_tasks()
    elif args.daemon:
        daemon_loop()
    elif args.run_now:
        if args.run_now == 'daily':
            run_daily_tasks()
        elif args.run_now == 'weekly':
            run_weekly_task(args.day)
        elif args.run_now == 'short':
            run_task(DAILY_TASKS[2])  # daily_short task
        elif args.run_now == 'analysis':
            run_task(DAILY_TASKS[3])  # daily_analysis task
        elif args.run_now == 'monitor':
            run_signal_monitor()
    else:
        show_schedule()
