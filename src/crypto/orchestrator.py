"""
FORTIX -- Central Orchestrator
=====================================
The brain of the operation. Manages all scheduled tasks, event-driven triggers,
data collection, video production, uploads, and self-improvement loops.

Runs as a Windows service or persistent process.

Usage:
    python src/crypto/orchestrator.py              # Start daemon
    python src/crypto/orchestrator.py --dry-run    # Log actions without executing
    python src/crypto/orchestrator.py --once       # Run one cycle and exit
"""

import os
import sys
import json
import time
import shutil
import signal
import sqlite3
import logging
import threading
import subprocess
from pathlib import Path
from datetime import datetime, timezone, timedelta
from logging.handlers import RotatingFileHandler

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
load_dotenv()

# Ensure Homebrew binaries (ffmpeg, etc.) are in PATH on macOS
import platform as _platform
if _platform.system() == 'Darwin':
    _brew_bin = '/opt/homebrew/bin'
    if _brew_bin not in os.environ.get('PATH', ''):
        os.environ['PATH'] = _brew_bin + ':' + os.environ.get('PATH', '')

# ════════════════════════════════════════════
# Configuration
# ════════════════════════════════════════════

DAILY_BUDGET_LIMIT = 50.00  # USD (Content Strategy v2: 6 shorts + 2 longs + alerts, no budget constraints)
DATA_COLLECTION_INTERVAL_MIN = 10
SIGNAL_CHECK_INTERVAL_MIN = 30  # Price/F&G/whale checks
NEWS_CHECK_INTERVAL_MIN = 30  # Breaking news checks (was 5min — caused CryptoPanic 429 rate limits)
MAX_VIDEOS_PER_DAY = 9  # 6 shorts + 1 long + 2 breaking news buffer
MIN_UPLOAD_GAP_HOURS = 1.5  # Min 1.5h between YouTube uploads
BACKUP_KEEP_DAYS = 7

# Launch mode: upload as PRIVATE, notify Viktor via Telegram for manual review
# After 7 days of validation → switch to LAUNCH_MODE = False for auto-public
LAUNCH_MODE = True  # Manual upload — Viktor reviews each video before publishing
SKIP_UPLOAD = True   # Disable auto-upload entirely — William uploads manually

# Paths
DB_PATH = Path('data/crypto/market.db')
PATTERNS_DB = Path('data/crypto/patterns.db')
OPTIMIZED_CONFIG = Path('data/crypto/optimized_config.json')
BACKUP_DIR = Path('data/crypto/backups')
LOG_DIR = Path('logs')
BUDGET_FILE = Path('output/crypto_signal/.daily_budget.json')
UPLOAD_DB = Path('data/crypto/uploads.db')

# Production schedule (UTC) — 4 shorts + 1 long per day
# Slots are RANDOMIZED daily within windows (see _generate_daily_schedule)
# Content diversity: data_signal + crypto_explained + long + story + emotional_hook
SCHEDULE = {
    'data_collection': {'interval_min': DATA_COLLECTION_INTERVAL_MIN},
    'signal_check': {'interval_min': SIGNAL_CHECK_INTERVAL_MIN},
    'news_check': {'interval_min': NEWS_CHECK_INTERVAL_MIN},
    'prediction_check': {'hour': 7, 'minute': 30},       # Daily 07:30 UTC (before production)
    'political_scan': {'interval_min': 60},               # Political/regulatory events every hour
    'event_scan': {'hour': 6, 'minute': 0},               # Token unlocks + hacks daily 06:00 UTC
    'orderbook_scan': {'interval_min': 60},              # Orderbook imbalance every hour
    'daily_backup': {'hour': 3, 'minute': 0},            # Daily 03:00 UTC
    'weekly_optimize': {'weekday': 6, 'hour': 12},       # Sun 12:00 UTC
    'daily_forti_refresh': {'hour': 4, 'minute': 0},       # Daily 04:00 UTC — replace stale FORTI clips
    'weekly_ml_retrain': {'weekday': 6, 'hour': 14, 'minute': 30},     # Sun 14:00 UTC (after optimize)
    'monthly_retrain': {'day': 1, 'hour': 6},            # 1st of month 06:00 UTC
    'community_post': {'hour': 14, 'minute': 45},         # Daily 14:45 UTC (after micro_shorts)
    'title_ab_check': {'hour': 7, 'minute': 0},          # Daily 07:00 UTC (check 12h+ old uploads)
    'comment_analysis': {'hour': 8, 'minute': 0},        # Daily 08:00 UTC
    'youtube_analytics': {'hour': 10, 'minute': 0},      # Daily 10:00 UTC
    'retention_analysis': {'hour': 11, 'minute': 0},     # Daily 11:00 UTC
    'prediction_eval': {'hour': 1, 'minute': 0},         # Daily 01:00 UTC (Level 5: auto-eval)
    'self_improve': {'hour': 2, 'minute': 0},            # Daily 02:00 UTC (v22: closed-loop learning)
}

# Coin rotation for coin_verdict (2 per week, rotating)
COIN_ROTATION = [
    ['BTC', 'ETH'], ['SOL', 'BNB'], ['XRP', 'ADA'],
    ['AVAX', 'DOT'], ['LINK', 'DOGE'],
]

# Phase configuration
PHASE_CONFIG_PATH = Path('data/crypto/phase_config.json')


def _load_phase_config() -> dict:
    """Load current phase configuration."""
    if PHASE_CONFIG_PATH.exists():
        try:
            return json.loads(PHASE_CONFIG_PATH.read_text())
        except Exception:
            pass
    return {
        'current_phase': 4,
        'phases': {
            '1': {'max_longs_per_week': 3, 'max_shorts_per_week': 14,
                   'daily_analysis': False, 'signal_alerts': False},
            '2': {'max_longs_per_week': 5, 'max_shorts_per_week': 21,
                   'daily_analysis': True, 'signal_alerts': True},
            '3': {'max_longs_per_week': 7, 'max_shorts_per_week': 28,
                   'daily_analysis': True, 'signal_alerts': True},
            '4': {'max_longs_per_week': 10, 'max_shorts_per_week': 35,
                   'daily_analysis': True, 'signal_alerts': True},
        }
    }


def setup_logging(dry_run: bool = False):
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / 'orchestrator.log'

    handler = RotatingFileHandler(str(log_file), maxBytes=10*1024*1024, backupCount=5)
    handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s'))

    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(handler)
    root.addHandler(console)

    if dry_run:
        logging.getLogger().info("DRY RUN MODE -- no actual production or uploads")


log = logging.getLogger('orchestrator')


class Orchestrator:
    """Central daemon managing all FORTIX operations."""

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.running = True
        self._last_run = self._load_last_run()  # Persistent — survives restarts
        self._pending_uploads = self._load_upload_queue()  # Persistent queue (survives crashes)
        self._upload_queue = []  # Thread-safe upload queue
        self._upload_lock = threading.Lock()
        self._upload_thread = None
        self._liquidation_proc = None
        self._consecutive_errors = 0
        self._phase_config = _load_phase_config()
        self._weekly_production_count = self._load_weekly_count()

        # Telegram notifications
        try:
            from src.engines.monitor import TelegramNotifier
            self._telegram = TelegramNotifier()
            if self._telegram.is_configured():
                log.info("Telegram notifications enabled")
            else:
                self._telegram = None
                log.warning("Telegram not configured (missing BOT_TOKEN or CHAT_ID)")
        except Exception:
            self._telegram = None

        # Graceful shutdown
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def _shutdown(self, signum, frame):
        log.info("Shutdown signal received -- cleaning up...")
        self.running = False
        if self._liquidation_proc:
            self._liquidation_proc.terminate()

    def _notify(self, title: str, body: str, level: str = 'info'):
        """Send Telegram notification (non-blocking, never raises)."""
        if not self._telegram:
            return
        try:
            from src.engines.monitor import NotificationMessage
            self._telegram.send(NotificationMessage(title=title, body=body, level=level))
        except Exception as e:
            log.warning(f"Telegram send failed: {e}")

    def _notify_upload(self, video_id: str, video_type: str, meta_path: str = None):
        """Send Telegram notification about uploaded video for review."""
        title_text = video_type
        if meta_path:
            try:
                import json
                data = json.load(open(meta_path, encoding='utf-8'))
                title_text = data.get('title', video_type) or video_type
            except Exception:
                pass

        privacy = 'PRIVATE' if LAUNCH_MODE else 'PUBLIC'
        link = f'https://youtube.com/watch?v={video_id}'
        studio_link = f'https://studio.youtube.com/video/{video_id}/edit'

        body = (
            f"Тип: {video_type}\n"
            f"Назва: {title_text}\n"
            f"Статус: {privacy}\n"
            f"Дивитись: {link}\n"
            f"Опублікувати: {studio_link}"
        )

        self._notify(
            f"🎬 Нове відео готове до перегляду",
            body,
            'success' if LAUNCH_MODE else 'info'
        )

    # ─── Persistent Upload Queue ───

    LAST_RUN_FILE = Path('data/crypto/last_run.json')

    def _load_last_run(self) -> dict:
        """Load task run history from disk."""
        try:
            if self.LAST_RUN_FILE.exists():
                import json
                data = json.loads(self.LAST_RUN_FILE.read_text())
                # Only keep today's entries, parse ISO strings back to datetime
                today = datetime.now(timezone.utc).strftime('%Y%m%d')
                result = {}
                for k, v in data.items():
                    if today not in k:
                        continue
                    if isinstance(v, str):
                        try:
                            result[k] = datetime.fromisoformat(v)
                        except (ValueError, TypeError):
                            result[k] = datetime.now(timezone.utc)
                    else:
                        result[k] = v
                return result
        except Exception:
            pass
        return {}

    def _save_last_run(self):
        """Persist task run history to disk."""
        try:
            import json
            self.LAST_RUN_FILE.parent.mkdir(parents=True, exist_ok=True)
            # Convert datetime values to strings for JSON
            serializable = {}
            for k, v in self._last_run.items():
                if isinstance(v, datetime):
                    serializable[k] = v.isoformat()
                else:
                    serializable[k] = str(v)
            self.LAST_RUN_FILE.write_text(json.dumps(serializable))
        except Exception:
            pass

    def _upload_remaining(self):
        """Upload videos stuck in persistent queue (crash recovery)."""
        if SKIP_UPLOAD:
            return
        if not self._pending_uploads:
            return
        from src.crypto.channel_uploader import CryptoUploader
        uploader = CryptoUploader()
        uploaded = []
        for video_info in list(self._pending_uploads):
            video_dir = Path(video_info['path'])
            mp4s = list(video_dir.glob('*_FINAL.mp4'))
            if not mp4s:
                uploaded.append(video_info)  # Remove dead entries
                continue
            meta_path = video_dir / 'production_meta.json'
            thumb_path = video_dir / 'thumbnail.jpg'
            privacy = 'private' if LAUNCH_MODE else 'public'
            try:
                video_id = uploader.upload_video(
                    str(mp4s[0]),
                    meta_path=str(meta_path) if meta_path.exists() else None,
                    privacy=privacy,
                    thumbnail_path=str(thumb_path) if thumb_path.exists() else None,
                )
                if video_id:
                    uploaded.append(video_info)
                    log.info(f"[RECOVERY] Uploaded {video_info['type']}: {video_id}")
                    self._notify_upload(video_id, video_info['type'], str(meta_path) if meta_path.exists() else None)
            except Exception as e:
                log.warning(f"[RECOVERY] Upload failed for {video_info['type']}: {e}")
        for v in uploaded:
            if v in self._pending_uploads:
                self._pending_uploads.remove(v)
        if uploaded:
            self._save_upload_queue()

    DAILY_SCHEDULE_FILE = Path('data/crypto/daily_schedule.json')
    UPLOAD_QUEUE_FILE = Path('data/crypto/pending_uploads.json')

    def _generate_daily_schedule(self):
        """Content Strategy V3 schedule.

        Weekday (Mon-Fri):
          Slot 1: 07:00 AM ET (11:00 UTC) — Academy Short #N
          Slot 2: 12:00 PM ET (17:00 UTC) — DAILY FORECAST (main product)
          Slot 3: 05:00 PM ET (22:00 UTC) — Academy Short #N+1

        Monday special:
          Slot 1: 07:00 AM ET — Academy Short
          Slot 2: 09:00 AM ET (14:00 UTC) — SIGNAL CHECK
          Slot 3: 12:00 PM ET — DAILY FORECAST
          Slot 4: 05:00 PM ET — Academy Short

        Sunday:
          Slot 1: 07:00 AM ET — Academy Short
          Slot 2: 05:00 PM ET (22:00 UTC) — THE WEEK

        Saturday:
          Slot 1: 12:00 PM ET — Academy Short x2
        """
        import json, random
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')

        if self.DAILY_SCHEDULE_FILE.exists():
            try:
                data = json.loads(self.DAILY_SCHEDULE_FILE.read_text())
                if data.get('date') == today:
                    log.info(f"  Daily schedule loaded: {len(data['slots'])} slots")
                    return data['slots']
            except Exception:
                pass

        now = datetime.now(timezone.utc)
        day_of_week = now.weekday()  # 0=Mon, 6=Sun

        # ═══ CONTENT STRATEGY V3 ═══
        # NY timezone (ET = UTC-4 summer)
        # Focus: Daily Forecast (main) + Academy Shorts (discovery) + weekly specials

        slot_configs = []
        labels = []

        if day_of_week == 0:  # MONDAY
            slot_configs = [
                {'hour': 11, 'content': [{'type': 'academy_short'}]},
                {'hour': 14, 'content': [{'type': 'signal_check'}]},
                {'hour': 17, 'content': [{'type': 'daily_forecast'}]},
                {'hour': 22, 'content': [{'type': 'academy_short'}]},
            ]
            labels = ['academy_AM', 'SIGNAL_CHECK', 'DAILY_FORECAST', 'academy_PM']

        elif day_of_week == 6:  # SUNDAY
            slot_configs = [
                {'hour': 11, 'content': [{'type': 'academy_short'}]},
                {'hour': 22, 'content': [{'type': 'the_week'}]},
            ]
            labels = ['academy_AM', 'THE_WEEK']

        elif day_of_week == 5:  # SATURDAY
            slot_configs = [
                {'hour': 11, 'content': [{'type': 'academy_short'}]},
                {'hour': 17, 'content': [{'type': 'daily_forecast'}]},
                {'hour': 22, 'content': [{'type': 'academy_short'}]},
            ]
            labels = ['academy_AM', 'DAILY_FORECAST', 'academy_PM']

        else:  # TUE-FRI
            slot_configs = [
                {'hour': 11, 'content': [{'type': 'academy_short'}]},
                {'hour': 17, 'content': [{'type': 'daily_forecast'}]},
                {'hour': 22, 'content': [{'type': 'academy_short'}]},
            ]
            labels = ['academy_AM', 'DAILY_FORECAST', 'academy_PM']
            labels = ['DAILY_FORECAST']

        # Build slots with randomized minutes
        slots = {}
        for i, cfg in enumerate(slot_configs):
            minute = random.randint(0, 30)
            slots[f'slot_{i+1}'] = {
                'hour': cfg['hour'],
                'minute': minute,
                'content': cfg['content'],
            }

        data = {'date': today, 'slots': slots}
        self.DAILY_SCHEDULE_FILE.parent.mkdir(parents=True, exist_ok=True)
        self.DAILY_SCHEDULE_FILE.write_text(json.dumps(data, indent=2))

        times = [f"{s['hour']:02d}:{s['minute']:02d}" for s in slots.values()]
        day_name = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'][day_of_week]
        log.info(f"  Schedule V3 ({day_name}, {len(slots)} slots): "
                 f"{', '.join(f'{l}@{t}' for l, t in zip(labels, times))}")
        return slots

    def _should_run_slot(self, slot_name: str, slots: dict) -> bool:
        """Check if a randomized slot should fire now.
        Fires if current time >= target AND not yet run today.
        Handles slots after midnight (hour 0-6) as next-day targets."""
        slot = slots.get(slot_name)
        if not slot:
            return False
        now = datetime.now(timezone.utc)
        target = now.replace(hour=slot['hour'], minute=slot['minute'], second=0, microsecond=0)

        # If slot hour is before the earliest normal slot (e.g., 0-6 UTC),
        # it belongs to the NEXT calendar day (evening prime time in US)
        if slot['hour'] < 8 and now.hour >= 8:
            target += timedelta(days=1)

        task_key = f"slot_{slot_name}_{now.strftime('%Y%m%d')}"
        # Fire if past target time AND not yet run today
        if now >= target and task_key not in self._last_run:
            return True
        return False

    def _mark_slot_run(self, slot_name: str):
        now = datetime.now(timezone.utc)
        task_key = f"slot_{slot_name}_{now.strftime('%Y%m%d')}"
        self._last_run[task_key] = now
        self._save_last_run()

    def _get_rotating_long(self, now) -> tuple:
        """Get daily long-form type based on day of week.

        Schedule:
        - Mon: weekly_forecast (Signal Check — accountability, trust builder)
        - Tue: coin_verdict (deep dive on specific coin)
        - Wed: whale_watch (on-chain + whale analysis)
        - Thu: coin_verdict (different coin from Tuesday)
        - Fri: daily_analysis (end_of_week wrap-up)
        - Sat: crypto_explained (LONG-FORM educational — evergreen SEO)
        - Sun: daily_analysis (week_ahead outlook)

        First Monday of month: monthly_scorecard replaces weekly_forecast.
        """
        week_num = now.isocalendar()[1]
        rotation = COIN_ROTATION[(week_num - 1) % len(COIN_ROTATION)]
        day_map = {
            0: ('weekly_forecast', {}),                          # Monday: Signal Check
            1: ('coin_verdict', {'coin': rotation[0]}),          # Tuesday: Coin Verdict A
            2: ('whale_watch', {}),                              # Wednesday: Whale Watch
            3: ('coin_verdict', {'coin': rotation[1]}),          # Thursday: Coin Verdict B
            4: ('daily_analysis', {'theme': 'end_of_week'}),     # Friday: Week in Review
            5: ('crypto_explained', {}),                         # Saturday: Educational Deep Dive
            6: ('daily_analysis', {'theme': 'week_ahead'}),      # Sunday: Week Ahead
        }
        vtype, kwargs = day_map.get(now.weekday(), ('daily_analysis', {}))
        # First Monday of month: monthly scorecard
        if now.weekday() == 0 and now.day <= 7:
            vtype = 'monthly_scorecard'
            kwargs = {}
        log.info(f"  Daily LONG: {vtype} (day={now.strftime('%A')})")
        return vtype, kwargs

    def _produce_academy(self):
        """Generate Academy script via Claude, then produce through vertical pipeline."""
        import re
        try:
            import anthropic
            from src.crypto.crypto_academy import (
                get_next_topic, mark_produced, ACADEMY_SYSTEM, ACADEMY_USER
            )
            from src.crypto.script_generator import parse_script_output
            from src.crypto.produce_crypto import produce_short

            topic = get_next_topic()
            log.info(f"  Academy #{topic['id']}: {topic['topic']}")

            client = anthropic.Anthropic(api_key=os.getenv('ANTHROPIC_API_KEY'))
            resp = client.messages.create(
                model='claude-opus-4-6', max_tokens=1500,
                system=ACADEMY_SYSTEM,
                messages=[{'role': 'user', 'content': ACADEMY_USER.format(
                    number=topic['id'], topic=topic['topic'], level=topic['level'],
                    hook=topic['hook'], analogy=topic['analogy'], takeaway=topic['takeaway']
                )}]
            )
            script_text = resp.content[0].text
            log.info(f"  Academy script: {len(script_text.split())} words")

            parsed = parse_script_output(script_text, 'daily_short', short_style='crypto_explained')
            tm = re.search(r'TITLE:\s*(.+?)(?:\n|$)', script_text)
            if tm:
                parsed['title'] = tm.group(1).strip()
            parsed['tags'] = ['crypto education', topic['topic'].lower(),
                              'crypto explained', 'learn crypto', 'fortix crypto']
            parsed['hashtags'] = '#crypto #cryptoeducation #shorts'

            result = produce_short(skip_collect=True, _pre_script=parsed, _skip_dedup=True)
            if result:
                mark_produced(topic['id'], title=parsed.get('title', ''))
            return result
        except Exception as e:
            log.error(f"  Academy production failed: {e}")
            return None

    def _produce_and_upload(self, video_type: str, **kwargs):
        """Produce a video with FRESH data and upload IMMEDIATELY.
        Enforces MIN_UPLOAD_GAP (90 min) to prevent YouTube spam detection."""
        if self.dry_run:
            log.info(f"[DRY] Would produce+upload {video_type}")
            return

        # Enforce upload gap: queue for later if too soon after last upload
        try:
            import sqlite3 as _sq
            _uconn = _sq.connect(str(UPLOAD_DB), timeout=5)
            last_upload = _uconn.execute(
                "SELECT uploaded_at FROM uploads ORDER BY uploaded_at DESC LIMIT 1"
            ).fetchone()
            _uconn.close()
            if last_upload and last_upload[0]:
                last_ts = datetime.fromisoformat(last_upload[0].replace('Z', '+00:00'))
                gap_min = (datetime.now(timezone.utc) - last_ts).total_seconds() / 60
                if gap_min < 90:  # MIN_UPLOAD_GAP = 90 minutes
                    log.info(f"  Upload gap too short ({gap_min:.0f}min < 90min) — "
                             f"queuing {video_type} for later upload")
                    # Still produce, but queue instead of immediate upload
                    kwargs['_queue_only'] = True
        except Exception:
            pass

        try:
            from src.crypto.produce_crypto import produce, produce_micro_short, produce_daily_brief, produce_was_i_right
            from src.crypto.channel_uploader import CryptoUploader

            short_style = kwargs.pop('short_style', None)
            style_label = f" [{short_style}]" if short_style else ""
            log.info(f"[PRODUCE+UPLOAD] {video_type}{style_label} — producing with fresh data...")

            # ── V3 Content Types ──
            if video_type == 'daily_forecast':
                from src.crypto.produce_crypto import produce_daily_forecast
                result = produce_daily_forecast(skip_collect=True)
            elif video_type == 'academy_short':
                # V3: generate Academy script, route through vertical produce_short pipeline
                result = self._produce_academy()
            elif video_type == 'signal_check':
                from src.crypto.produce_crypto import produce_signal_check
                result = produce_signal_check(skip_collect=True)
            elif video_type == 'the_week':
                from src.crypto.produce_crypto import produce_the_week
                result = produce_the_week(skip_collect=True)
            # ── Legacy Content Types ──
            elif video_type == 'micro_short':
                topic_angle = kwargs.pop('topic_angle', None)
                focus_coin = kwargs.pop('focus_coin', None)
                result = produce_micro_short(skip_collect=True, short_style=short_style,
                                            topic_angle=topic_angle, focus_coin=focus_coin)
            elif video_type == 'daily_brief':
                result = produce_daily_brief(skip_collect=True)
            elif video_type == 'was_i_right':
                result = produce_was_i_right(skip_collect=True)
            elif video_type == 'crypto_explained':
                result = produce(video_type='crypto_explained', skip_collect=True, **kwargs)
            else:
                result = produce(video_type=video_type, skip_collect=True, **kwargs)

            if not result:
                log.warning(f"  {video_type}: production returned None")
                return

            result_path = Path(str(result))
            self._record_production('short' if 'short' in video_type or 'micro' in video_type or 'was_i' in video_type else 'long')

            # Result can be MP4 file OR directory — handle both
            if result_path.is_file() and result_path.suffix == '.mp4':
                mp4_file = result_path
                video_dir = result_path.parent
            elif result_path.is_dir():
                video_dir = result_path
                mp4s = list(video_dir.glob('*_FINAL.mp4'))
                mp4_file = mp4s[0] if mp4s else None
            else:
                log.warning(f"  {video_type}: invalid result path {result_path}")
                return

            if not mp4_file or not mp4_file.exists():
                log.warning(f"  {video_type}: no MP4 found in {video_dir}")
                self._pending_uploads.append({'path': str(video_dir), 'type': video_type})
                self._save_upload_queue()
                return

            meta_path = video_dir / 'production_meta.json'
            thumb_path = video_dir / 'thumbnail.jpg'
            upload_privacy = 'private' if LAUNCH_MODE else 'public'

            # ── PRE-UPLOAD QUALITY GATE ──
            # Run quality checks BEFORE uploading or queuing.
            # Blocks obviously broken videos from entering the pipeline.
            try:
                from src.crypto.quality_gate import validate_before_upload, log_gate_failure
                gate_result = validate_before_upload(video_dir)
                if not gate_result['ok']:
                    log.error(
                        f"  {video_type}: QUALITY GATE BLOCKED — "
                        f"{len(gate_result['errors'])} errors"
                    )
                    for err in gate_result['errors']:
                        log.error(f"    GATE: {err}")
                    log_gate_failure(video_dir, gate_result)
                    self._notify(
                        f"⚠️ {video_type} не пройшов перевірку якості",
                        '\n'.join(gate_result['errors'][:5]),
                        "error"
                    )
                    return
                for w in gate_result.get('warnings', []):
                    log.warning(f"    GATE: {w}")
            except Exception as e:
                log.warning(f"  Quality gate check failed (non-blocking): {e}")

            # ── MANUAL UPLOAD MODE ──
            # Create UPLOAD_INFO.txt and notify Viktor via Telegram
            if LAUNCH_MODE:
                try:
                    import json as _json
                    meta_data = _json.loads(meta_path.read_text(encoding='utf-8')) if meta_path.exists() else {}
                    import re as _re
                    _title = meta_data.get('title', video_type)
                    _hashtags = meta_data.get('hashtags', '')
                    _desc = meta_data.get('description', '')
                    _script = meta_data.get('script_text', '')
                    _clean_script = _re.sub(r'\[VISUAL:.*?\]', '', _script)
                    _clean_script = _re.sub(r'\n{3,}', '\n\n', _clean_script).strip()
                    _tags = ', '.join(meta_data.get('tags', []))

                    info_path = video_dir / 'UPLOAD_INFO.txt'
                    with open(info_path, 'w', encoding='utf-8') as _f:
                        _f.write(f"TITLE: {_title}\n\n")
                        _f.write(f"DESCRIPTION:\n{_hashtags}\n\n")
                        _f.write(f"{_desc}\n\n")
                        _f.write(f"{_clean_script}\n\n")
                        _f.write(f"Subscribe for daily crypto signals.\n\n")
                        _f.write(f"TAGS: {_tags}\n")
                except Exception:
                    pass

                # Telegram: send video + thumbnail + upload info as separate messages
                log.info(f"  {video_type}: sending to Telegram for review...")
                try:
                    from src.engines.monitor import TelegramNotifier
                    tg = TelegramNotifier()
                    if tg.is_configured():
                        _size_mb = mp4_file.stat().st_size / 1024 / 1024

                        # 1. Text message with title, description, tags
                        _text = (
                            f"🎬 <b>{meta_data.get('title', video_type)}</b>\n\n"
                            f"Тип: {video_type}\n"
                            f"Розмір: {_size_mb:.1f}MB\n\n"
                            f"<b>Опис:</b>\n"
                            f"{meta_data.get('hashtags', '')}\n\n"
                            f"{meta_data.get('description', '')}\n\n"
                            f"<b>Теги:</b> {', '.join(meta_data.get('tags', [])[:8])}\n\n"
                            f"📁 Папка: {video_dir.name}"
                        )
                        from src.engines.monitor import NotificationMessage
                        tg.send(NotificationMessage(
                            title=f"🎬 {video_type} готово",
                            body=_text[:3500],
                            level="success"
                        ))

                        # 2. Thumbnail
                        if thumb_path.exists():
                            tg.send_file(str(thumb_path), caption="Мініатюра")

                        # 3. Video file
                        tg.send_file(str(mp4_file), caption=meta_data.get('title', '')[:200])

                        # 4. Upload info text file
                        if info_path.exists():
                            tg.send_file(str(info_path), caption="Інфо для завантаження (назва, опис, теги)")

                        log.info(f"  {video_type}: sent to Telegram (video + thumbnail + info)")
                except Exception as e:
                    log.warning(f"  Telegram file send failed: {e}")

                log.info(f"  {video_type}: READY for manual upload → {video_dir.name}")
                return

            # ── AUTO UPLOAD MODE ──
            if kwargs.get('_queue_only'):
                self._pending_uploads.append({'path': str(video_dir), 'type': video_type})
                self._save_upload_queue()
                log.info(f"  {video_type}: produced, queued for staggered upload")
                return

            uploader = CryptoUploader()
            video_id = uploader.upload_video(
                str(mp4_file),
                meta_path=str(meta_path) if meta_path.exists() else None,
                privacy=upload_privacy,
                thumbnail_path=str(thumb_path) if thumb_path.exists() else None,
            )

            if video_id:
                log.info(f"  {video_type}: uploaded as {upload_privacy}: {video_id}")
                self._notify_upload(video_id, video_type, str(meta_path) if meta_path.exists() else None)
            else:
                log.error(f"  {video_type}: upload FAILED, adding to retry queue")
                self._pending_uploads.append({'path': str(video_dir), 'type': video_type})
                self._save_upload_queue()

        except Exception as e:
            log.error(f"  {video_type} produce+upload failed: {e}")
            self._notify(f"❌ {video_type} не вдалось", str(e)[:300], "error")

    def _save_upload_queue(self):
        """Persist pending uploads to disk so they survive crashes."""
        try:
            import json
            self.UPLOAD_QUEUE_FILE.parent.mkdir(parents=True, exist_ok=True)
            self.UPLOAD_QUEUE_FILE.write_text(json.dumps(self._pending_uploads, default=str))
        except Exception as e:
            log.warning(f"Upload queue save failed: {e}")

    def _load_upload_queue(self):
        """Load pending uploads from disk on startup."""
        try:
            if self.UPLOAD_QUEUE_FILE.exists():
                import json
                data = json.loads(self.UPLOAD_QUEUE_FILE.read_text())
                if isinstance(data, list) and data:
                    log.info(f"Recovered {len(data)} pending uploads from disk")
                    return data
        except Exception as e:
            log.warning(f"Upload queue load failed: {e}")
        return []

    def _scan_unuploaded_videos(self):
        """Scan output directory for videos produced today but not yet uploaded.

        Supports new nested layout:  output/crypto_signal/{type_slug}/{MON-DD-YYYY}-#N/
        Also supports legacy flat layout:  output/crypto_signal/{type}_{YYYYMMDD_HHMMSS}/
        """
        import json as _json
        now = datetime.now(timezone.utc)
        today_legacy = now.strftime('%Y%m%d')                 # e.g. 20260402
        _months = ['JAN','FEB','MAR','APR','MAY','JUN',
                    'JUL','AUG','SEP','OCT','NOV','DEC']
        today_new = f"{_months[now.month - 1]}-{now.day:02d}-{now.year}"  # e.g. APR-02-2026
        output_dir = Path('output/crypto_signal')
        if not output_dir.exists():
            return []

        # Collect candidate directories (both layouts)
        candidates = []
        for entry in output_dir.iterdir():
            if not entry.is_dir():
                continue
            # Legacy flat: daily_forecast_20260402_011101
            if today_legacy in entry.name:
                candidates.append(entry)
            else:
                # New nested: type_slug/ subdirectory containing date dirs
                for sub in entry.iterdir():
                    if sub.is_dir() and sub.name.startswith(today_new):
                        candidates.append(sub)

        recovered = []
        for d in candidates:
            meta_path = d / 'production_meta.json'
            if not meta_path.exists():
                continue
            try:
                meta = _json.loads(meta_path.read_text(encoding='utf-8'))
            except Exception:
                continue

            # Skip if already uploaded (has youtube_id)
            if meta.get('youtube_id'):
                continue

            # Find MP4
            mp4s = list(d.glob('*_FINAL.mp4'))
            if not mp4s:
                continue

            # Check if already in pending queue
            path_str = str(d)
            if any(v.get('path') == path_str for v in self._pending_uploads):
                continue

            video_type = meta.get('video_type', 'unknown')
            recovered.append({
                'path': path_str,
                'type': video_type,
                'priority': video_type in ('breaking_news', 'signal_alert'),
            })

        if recovered:
            log.info(f"Found {len(recovered)} unuploaded videos from today: "
                    f"{[v['type'] for v in recovered]}")
        return recovered

    # ─── Weekly Count Persistence ───

    WEEKLY_COUNT_FILE = Path('data/crypto/weekly_count.json')

    def _load_weekly_count(self) -> dict:
        """Load weekly production count from disk."""
        try:
            if self.WEEKLY_COUNT_FILE.exists():
                import json
                data = json.loads(self.WEEKLY_COUNT_FILE.read_text())
                current_week = datetime.now(timezone.utc).isocalendar()[1]
                if data.get('week') == current_week:
                    return data
        except Exception as e:
            log.warning(f"Weekly count load failed: {e}")
        return {'longs': 0, 'shorts': 0, 'week': datetime.now(timezone.utc).isocalendar()[1]}

    def _save_weekly_count(self):
        """Persist weekly production count to disk."""
        try:
            import json
            self.WEEKLY_COUNT_FILE.parent.mkdir(parents=True, exist_ok=True)
            self.WEEKLY_COUNT_FILE.write_text(json.dumps(self._weekly_production_count))
        except Exception as e:
            log.warning(f"Weekly count save failed: {e}")

    def _should_run(self, task_name: str) -> bool:
        """Check if a scheduled task should run now."""
        now = datetime.now(timezone.utc)
        cfg = SCHEDULE.get(task_name, {})

        # Interval-based tasks
        if 'interval_min' in cfg:
            last = self._last_run.get(task_name)
            if last is None:
                return True
            return (now - last).total_seconds() >= cfg['interval_min'] * 60

        # Time-based tasks
        hour = cfg.get('hour')
        minute = cfg.get('minute', 0)
        weekday = cfg.get('weekday')
        day = cfg.get('day')

        if hour is None:
            return False

        # Check if we're in the right time window (±5 min)
        if now.hour != hour:
            return False
        if abs(now.minute - minute) > 5:
            return False

        # Weekday check (0=Mon, 6=Sun)
        if weekday is not None and now.weekday() != weekday:
            return False

        # Day of month check
        if day is not None and now.day != day:
            return False

        # Don't re-run within the hour
        last = self._last_run.get(task_name)
        if last and (now - last).total_seconds() < 3600:
            return False

        return True

    def _mark_run(self, task_name: str):
        self._last_run[task_name] = datetime.now(timezone.utc)
        self._save_last_run()

    def _get_daily_spend(self) -> float:
        """Get today's API spending."""
        if BUDGET_FILE.exists():
            try:
                data = json.loads(BUDGET_FILE.read_text())
                today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
                if data.get('date') == today:
                    return data.get('spent', 0)
            except (json.JSONDecodeError, KeyError):
                pass
        return 0

    def _can_spend(self, amount: float) -> bool:
        return self._get_daily_spend() + amount <= DAILY_BUDGET_LIMIT

    # ─── Phase Management ───

    def _get_phase_limits(self) -> dict:
        """Get production limits for current phase."""
        phase = self._phase_config.get('current_phase', 1)
        phases = self._phase_config.get('phases', {})
        return phases.get(str(phase), phases.get('1', {}))

    def _check_weekly_limits(self, content_class: str) -> bool:
        """Check if we're within weekly production limits for current phase."""
        current_week = datetime.now(timezone.utc).isocalendar()[1]
        if self._weekly_production_count['week'] != current_week:
            self._weekly_production_count = {'longs': 0, 'shorts': 0, 'week': current_week}
        limits = self._get_phase_limits()
        if content_class == 'long':
            return self._weekly_production_count['longs'] < limits.get('max_longs_per_week', 4)
        elif content_class == 'short':
            return self._weekly_production_count['shorts'] < limits.get('max_shorts_per_week', 5)
        return True

    def _record_production(self, content_class: str):
        """Record a production for weekly limit tracking (persisted to disk)."""
        current_week = datetime.now(timezone.utc).isocalendar()[1]
        if self._weekly_production_count['week'] != current_week:
            self._weekly_production_count = {'longs': 0, 'shorts': 0, 'week': current_week}
        key = 'longs' if content_class == 'long' else 'shorts'
        self._weekly_production_count[key] += 1
        self._save_weekly_count()

    # ─── Data Collection ───

    def task_collect_data(self):
        """Collect market data from all sources."""
        if self.dry_run:
            log.info("[DRY] Would collect data")
            return

        try:
            from src.crypto.data_collector import collect_all
            collect_all(heartbeat_fn=self._write_heartbeat)
            self._write_heartbeat()
            log.info("Data collection complete")
        except Exception as e:
            log.error(f"Data collection failed: {e}")
            self._notify("❌ Збір даних не вдався", str(e)[:300], "error")

        self._write_heartbeat()
        # Daily macro data (FRED: VIX, yield curve, treasury) — runs once per day
        try:
            now = datetime.now(timezone.utc)
            macro_key = f"macro_update_{now.strftime('%Y%m%d')}"
            if macro_key not in self._last_run:
                self._update_macro_data()
                self._last_run[macro_key] = now
        except Exception as e:
            log.warning(f"Macro update failed (non-critical): {e}")

    def _update_macro_data(self):
        """Fetch daily FRED data + weekly Google Trends + DeFi TVL."""
        import os
        import sqlite3 as _sql
        import requests as _req

        conn = _sql.connect(str(Path('data/crypto/market.db')), timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")

        # 1. FRED: VIX, yield curve, 10Y treasury (daily)
        fred_key = os.getenv('FRED_API_KEY', '')
        if fred_key:
            for series_id, event_type in [('VIXCLS', 'vix'), ('T10Y2Y', 'yield_curve'),
                                           ('DGS10', 'treasury_10y'),
                                           ('SP500', 'sp500'), ('NASDAQCOM', 'nasdaq'),
                                           ('DTWEXBGS', 'dxy')]:
                try:
                    r = _req.get('https://api.stlouisfed.org/fred/series/observations',
                                params={'series_id': series_id, 'api_key': fred_key,
                                        'file_type': 'json', 'sort_order': 'desc', 'limit': 5},
                                timeout=15)
                    if r.status_code == 200:
                        for obs in r.json().get('observations', []):
                            if obs['value'] != '.':
                                conn.execute(
                                    "INSERT OR REPLACE INTO macro_events (date, event_type, value) VALUES (?,?,?)",
                                    (obs['date'], event_type, float(obs['value']))
                                )
                except Exception:
                    pass
            conn.commit()
            log.info("  FRED macro data updated (VIX, yield curve, 10Y)")

        # 2. DeFi TVL History (daily)
        try:
            r = _req.get('https://api.llama.fi/v2/historicalChainTvl', timeout=15)
            if r.status_code == 200:
                data = r.json()
                for point in data[-7:]:  # last 7 days
                    ts = point.get('date', 0)
                    date = datetime.fromtimestamp(ts).strftime('%Y-%m-%d')
                    conn.execute(
                        "INSERT OR REPLACE INTO defi_tvl_history (date, total_tvl) VALUES (?,?)",
                        (date, point.get('tvl', 0))
                    )
                conn.commit()
                log.info("  DeFi TVL history updated")
        except Exception:
            pass

        # 3. Google Trends (weekly — only on Sundays)
        now = datetime.now(timezone.utc)
        if now.weekday() == 6:  # Sunday
            try:
                from pytrends.request import TrendReq
                pytrends = TrendReq(hl='en-US', tz=0)
                keywords = ['bitcoin', 'ethereum', 'crypto']
                pytrends.build_payload(keywords, timeframe='now 7-d', geo='')
                data = pytrends.interest_over_time()
                if not data.empty:
                    for _, row in data.iterrows():
                        date = row.name.strftime('%Y-%m-%d')
                        for kw in keywords:
                            if kw in data.columns:
                                conn.execute(
                                    "INSERT OR REPLACE INTO google_trends (date, keyword, value) VALUES (?,?,?)",
                                    (date, kw, int(row[kw]))
                                )
                    conn.commit()
                    log.info("  Google Trends updated")
            except Exception:
                pass

        conn.close()

    # ─── Signal Monitor ───

    def task_check_signals(self):
        """Check for signal triggers (price moves, F&G extremes, whales)."""
        try:
            from src.crypto.signal_monitor import check_all_triggers, load_alert_history, save_alert
            fired = check_all_triggers()

            if fired:
                history = load_alert_history()
                for t in fired:
                    log.info(f"SIGNAL TRIGGERED: {t['trigger']} -- {t['event']}")
                    t['timestamp'] = datetime.now(timezone.utc).isoformat()
                    save_alert(t, history)

                    if t.get('trigger_type') == 'breaking_news':
                        # Breaking news -> immediate short + upload
                        if not self.dry_run:
                            self._produce_breaking_news_short(t)
                    else:
                        # Regular signal -> full alert video
                        if not self.dry_run:
                            self._produce_signal_alert(t)
            else:
                log.debug("No signal triggers")
        except Exception as e:
            log.error(f"Signal check failed: {e}")
            self._notify("❌ Перевірка сигналів не вдалась", str(e)[:300], "error")

    def task_check_fast_triggers(self):
        """Fast check every 5 min: news + derivatives + structural triggers.

        STRICT: only high/critical severity + max 2 breaking news/day.
        Most content comes from scheduled micro_shorts (3/day) + daily_brief.
        """
        try:
            from src.crypto.signal_monitor import check_fast_triggers, load_alert_history, save_alert
            fired = check_fast_triggers()

            if fired:
                history = load_alert_history()
                # Already rate-limited by signal_monitor — but double-check daily count
                for t in fired:
                    severity = t.get('severity', '?').upper()
                    category = t.get('category', 'unknown')
                    log.info(f"BREAKING NEWS [{severity}] ({category}): {t['event']}")
                    t['timestamp'] = datetime.now(timezone.utc).isoformat()
                    save_alert(t, history)

                    if not self.dry_run:
                        self._produce_breaking_news_short(t)
                    # Only produce ONE breaking news per check cycle
                    break
            else:
                log.debug("No fast triggers")
        except Exception as e:
            log.error(f"Fast trigger check failed: {e}")

    def _produce_signal_alert(self, trigger: dict):
        """Produce a signal alert video from trigger."""
        limits = self._get_phase_limits()
        if not limits.get('signal_alerts', True):
            log.info("Signal alerts disabled in current phase -- skipping")
            return
        if not self._can_spend(1.34):
            log.warning("Budget limit -- skipping signal alert production")
            return

        try:
            from src.crypto.produce_crypto import produce
            result = produce(
                video_type='signal_alert',
                event=trigger.get('event', trigger.get('description', '')),
                coin=trigger.get('coin'),
                skip_collect=True,
            )
            if result:
                self._pending_uploads.append({
                    'path': str(result),
                    'type': 'signal_alert',
                    'priority': True,
                })
                self._save_upload_queue()
                log.info(f"Signal alert produced: {result}")
        except Exception as e:
            log.error(f"Signal alert production failed: {e}")

    def _produce_breaking_news_short(self, trigger: dict):
        """Produce a breaking news short and upload IMMEDIATELY.

        Target: news published -> short on YouTube within 10 MINUTES MAX.
        Pipeline: script (30s) + voice (30s) + charts (pre-cached) + assembly (30s) + upload (2min)
        Skips Kling AI scenes, music, Whisper — speed is EVERYTHING.
        10 MINUTE RULE: if we can't beat 10 min, we're too slow.
        """
        if not self._can_spend(0.65):
            log.warning("Budget limit -- skipping breaking news short")
            return

        event = trigger.get('event', '')
        coins = trigger.get('coins', '')
        category = trigger.get('category', 'market_event')
        news_title = trigger.get('news_title', event)

        log.info(f"PRODUCING BREAKING NEWS SHORT: {event}")
        log.info(f"  Category: {category}, Coins: {coins}")

        try:
            from src.crypto.produce_crypto import produce_short
            result = produce_short(
                skip_collect=True,
                skip_visuals=False,  # Charts + Ken Burns (visuals required for MP4)
                breaking_news=news_title,
            )

            if result:
                log.info(f"Breaking news short produced: {result}")
                # IMMEDIATE upload -- don't wait for scheduled time
                self._upload_immediately(str(result), 'breaking_news_short')
        except Exception as e:
            log.error(f"Breaking news short production failed: {e}")

    def _upload_immediately(self, video_path: str, video_type: str):
        """Upload a video immediately (for time-sensitive content)."""
        if SKIP_UPLOAD:
            log.info(f"  {video_type}: SKIP_UPLOAD=True — skipping auto-upload")
            return
        from pathlib import Path
        video_path = Path(video_path)

        # Handle both file paths and directory paths
        if video_path.is_file() and video_path.suffix == '.mp4':
            video_file = str(video_path)
            video_dir = video_path.parent
        elif video_path.is_dir():
            video_dir = video_path
            # Try FINAL.mp4 first, then any .mp4
            mp4s = list(video_dir.glob('*FINAL.mp4')) or list(video_dir.glob('*.mp4'))
            if not mp4s:
                log.error(f"No MP4 found in {video_dir}")
                return
            video_file = str(mp4s[0])
        else:
            log.error(f"Invalid video path: {video_path}")
            return

        script_path = None
        meta_path = None

        scripts = list(video_dir.glob('scripts/*.txt'))
        if scripts:
            script_path = str(scripts[0])
        # Search for metadata: production_meta.json first, then scripts/*.json
        prod_meta = video_dir / 'production_meta.json'
        if prod_meta.exists():
            meta_path = str(prod_meta)
        else:
            metas = list(video_dir.glob('scripts/*.json')) or list(video_dir.glob('*.json'))
            if metas:
                meta_path = str(metas[0])

        try:
            from src.crypto.channel_uploader import CryptoUploader
            uploader = CryptoUploader()

            # Find thumbnail
            thumbs = list(video_dir.glob('thumbnail.*'))
            thumbnail_path = str(thumbs[0]) if thumbs else None

            video_id = uploader.upload_video(
                video_file,
                script_path=script_path,
                meta_path=meta_path,
                privacy='private' if LAUNCH_MODE else 'public',
                thumbnail_path=thumbnail_path,
            )
            if video_id:
                privacy_label = 'PRIVATE (review needed)' if LAUNCH_MODE else 'PUBLIC'
                log.info(f"BREAKING NEWS uploaded as {privacy_label}: {video_id}")
                self._notify_upload(video_id, 'breaking_news', meta_path)
            else:
                log.error("Immediate upload failed -- no video_id returned")
        except Exception as e:
            log.error(f"Immediate upload failed: {e}")

    # ─── Scheduled Production ───

    def task_produce_micro_shorts(self):
        """Produce batch of 3 micro shorts (45-60s each, 1 Claude call)."""
        if self.dry_run:
            log.info("[DRY] Would produce 3 micro shorts")
            return
        if not self._check_weekly_limits('short'):
            log.info("Weekly short limit reached -- skipping micro shorts")
            return
        if not self._can_spend(0.40):
            log.warning("Budget limit -- skipping micro shorts batch (need $0.40)")
            return

        try:
            from src.crypto.produce_crypto import produce_micro_shorts_batch
            results = produce_micro_shorts_batch(skip_collect=True)
            for i, result in enumerate(results):
                if result:
                    self._record_production('short')
                    self._pending_uploads.append({
                        'path': str(result),
                        'type': 'micro_short',
                        'upload_time': f'micro_{i+1}',
                    })
                    self._save_upload_queue()
            log.info(f"Produced {len(results)}/3 micro shorts")
        except Exception as e:
            log.error(f"Micro shorts batch failed: {e}")
            self._notify("❌ Мікро-шортси не вдались", str(e)[:300], "error")

    def task_produce_was_i_right(self):
        """Produce a 'Was I Right?' Short (45-60s, reviews a 3-day-old prediction)."""
        if self.dry_run:
            log.info("[DRY] Would produce Was I Right short")
            return
        if not self._check_weekly_limits('short'):
            log.info("Weekly short limit reached -- skipping was_i_right")
            return
        if not self._can_spend(0.30):
            log.warning("Budget limit -- skipping was_i_right (need $0.30)")
            return

        try:
            from src.crypto.produce_crypto import produce_was_i_right
            result = produce_was_i_right(skip_collect=True)
            if result:
                self._record_production('short')
                self._pending_uploads.append({
                    'path': str(result),
                    'type': 'was_i_right',
                    'upload_time': 'was_i_right',
                })
                self._save_upload_queue()
                log.info(f"Produced Was I Right short: {result}")
            else:
                log.info("Was I Right skipped (no suitable predictions found)")
        except Exception as e:
            log.error(f"Was I Right failed: {e}")
            self._notify("❌ 'Чи я мав рацію' не вдалось", str(e)[:300], "error")

    def task_produce_daily_brief(self):
        """Produce a Daily Brief (horizontal, 3-5 min, auto-topic)."""
        if self.dry_run:
            log.info("[DRY] Would produce daily brief")
            return
        if not self._check_weekly_limits('short'):
            log.info("Weekly limit reached -- skipping daily brief")
            return
        if not self._can_spend(0.75):
            log.warning("Budget limit -- skipping daily brief (need $0.75)")
            return

        try:
            from src.crypto.produce_crypto import produce_daily_brief
            result = produce_daily_brief(skip_collect=True)
            if result:
                self._record_production('long')
                self._pending_uploads.append({
                    'path': str(result),
                    'type': 'daily_brief',
                    'upload_time': 'brief',
                })
                self._save_upload_queue()
                log.info(f"Daily brief produced: {result}")
            else:
                log.warning("Daily brief production failed")
        except Exception as e:
            log.error(f"Daily brief failed: {e}")
            self._notify("❌ Денний огляд не вдався", str(e)[:300], "error")

    def task_produce_daily_shorts(self, count: int = 3):
        """LEGACY: Produce batch of daily shorts with diverse topics."""
        if self.dry_run:
            log.info(f"[DRY] Would produce {count} daily shorts")
            return
        if not self._check_weekly_limits('short'):
            log.info("Weekly short limit reached for current phase -- skipping")
            return
        if not self._can_spend(0.65 * count):
            log.warning(f"Budget limit -- skipping daily shorts batch (need ${0.65*count:.2f})")
            return

        try:
            from src.crypto.produce_crypto import produce_daily_shorts
            results = produce_daily_shorts(skip_collect=True, count=count)
            for i, result in enumerate(results):
                if result:
                    self._record_production('short')
                    self._pending_uploads.append({
                        'path': str(result),
                        'type': 'daily_short',
                        'upload_time': f'short_{i+1}',
                    })
                    self._save_upload_queue()
            log.info(f"Produced {len(results)}/{count} daily shorts")
        except Exception as e:
            log.error(f"Daily shorts batch failed: {e}")

    def task_produce_long(self, video_type: str, coin: str = None, theme: str = None):
        """Produce a long-form video."""
        if self.dry_run:
            log.info(f"[DRY] Would produce {video_type} (coin={coin}, theme={theme})")
            return
        if not self._check_weekly_limits('long'):
            log.info(f"Weekly long limit reached for current phase -- skipping {video_type}")
            return
        if not self._can_spend(1.34):
            log.warning(f"Budget limit -- skipping {video_type}")
            return

        try:
            from src.crypto.produce_crypto import produce
            result = produce(
                video_type=video_type,
                coin=coin,
                skip_collect=True,
                theme=theme,
            )
            if result:
                self._record_production('long')
                self._pending_uploads.append({
                    'path': str(result),
                    'type': video_type,
                    'upload_time': 'long',
                })
                self._save_upload_queue()
        except Exception as e:
            log.error(f"{video_type} production failed: {e}")
            self._notify(f"❌ Виробництво {video_type} не вдалось", str(e)[:300], "error")

    # ─── Uploads ───

    def task_upload_daily(self):
        """Enqueue pending videos for background upload with human-like timing.
        Non-blocking: returns immediately, uploads happen in a daemon thread."""
        if self.dry_run:
            for v in self._pending_uploads:
                log.info(f"[DRY] Would upload: {v['path']} ({v['type']})")
            return

        if self._upload_thread and self._upload_thread.is_alive():
            log.info("  Upload thread still running — skipping new batch")
            return

        with self._upload_lock:
            self._upload_queue = list(self._pending_uploads)

        if not self._upload_queue:
            return

        log.info(f"  Enqueued {len(self._upload_queue)} videos for background upload")
        self._upload_thread = threading.Thread(
            target=self._upload_worker, daemon=True, name='upload-worker')
        self._upload_thread.start()

    def _upload_worker(self):
        """Background thread: uploads videos staggered across peak viewing hours.

        Target upload times (UTC):
          - Shorts: ~14:00, ~17:00, ~20:00 (spread across peak hours)
          - Daily brief: ~30 min after last short
          - Longs: ~60 min after brief
        Each target has ±15 min jitter for human-like randomness.
        """
        import random

        with self._upload_lock:
            queue = list(self._upload_queue)

        shorts = [v for v in queue if v['type'] in ('micro_short', 'daily_short', 'was_i_right')]
        briefs = [v for v in queue if v['type'] == 'daily_brief']
        longs = [v for v in queue if v['type'] not in ('micro_short', 'daily_short', 'was_i_right', 'daily_brief')]

        # Staggered target hours for shorts (UTC) — peak viewing windows
        SHORT_TARGET_HOURS = [14, 17, 20]

        for i, short in enumerate(shorts):
            if not self.running:
                break
            if i < len(SHORT_TARGET_HOURS):
                target_hour = SHORT_TARGET_HOURS[i]
                now = datetime.now(timezone.utc)
                target_time = now.replace(hour=target_hour, minute=0, second=0, microsecond=0)
                # Add ±15 min jitter
                jitter = random.uniform(-15 * 60, 15 * 60)
                target_time = target_time + timedelta(seconds=jitter)
                delay = (target_time - now).total_seconds()
                if delay > 0:
                    log.info(f"  [upload-worker] Short #{i+1}: waiting until ~{target_time.strftime('%H:%M')} UTC "
                             f"({delay/60:.0f} min)...")
                    # Sleep in 30s chunks so shutdown is responsive
                    for _ in range(int(delay / 30)):
                        if not self.running:
                            return
                        time.sleep(30)
                else:
                    log.info(f"  [upload-worker] Short #{i+1}: target {target_hour}:00 UTC already passed, uploading now")
            else:
                # Extra shorts beyond 3: space them 2-3h apart
                delay = random.uniform(2.0 * 3600, 3.0 * 3600)
                log.info(f"  [upload-worker] Extra short #{i+1}: waiting {delay/60:.0f} min...")
                for _ in range(int(delay / 30)):
                    if not self.running:
                        return
                    time.sleep(30)
            self._upload_one_video(short)

        # Upload daily briefs ~30 min after last short
        for brief in briefs:
            if not self.running:
                break
            delay = random.uniform(20 * 60, 40 * 60)
            log.info(f"  [upload-worker] Waiting {delay/60:.0f} min before daily brief...")
            for _ in range(int(delay / 30)):
                if not self.running:
                    return
                time.sleep(30)
            self._upload_one_video(brief)

        # Upload longs ~60 min after brief
        if longs and self.running:
            delay = random.uniform(45 * 60, 90 * 60)
            log.info(f"  [upload-worker] Waiting {delay/60:.0f} min before long upload...")
            for _ in range(int(delay / 30)):
                if not self.running:
                    return
                time.sleep(30)
            self._upload_one_video(longs[0])

        log.info("  [upload-worker] Upload batch complete")

    def _upload_one_video(self, video_info: dict):
        """Upload a single video."""
        if SKIP_UPLOAD:
            log.info(f"  SKIP_UPLOAD=True — skipping upload for {video_info.get('type', 'unknown')}")
            return
        from src.crypto.channel_uploader import CryptoUploader
        uploader = CryptoUploader()

        video_path = Path(video_info['path'])

        # Find the actual video file (FINAL.mp4)
        if video_path.is_dir():
            mp4s = list(video_path.glob('*FINAL.mp4'))
            if mp4s:
                video_file = mp4s[0]
            else:
                log.warning(f"No FINAL.mp4 found in {video_path}")
                return
        else:
            video_file = video_path

        # Find script for metadata
        script_files = (list(video_path.parent.glob('scripts/*.txt'))
                        if video_path.is_file()
                        else list(video_path.glob('scripts/*.txt')))
        script_path = str(script_files[0]) if script_files else None

        # Find metadata — production_meta.json first, then scripts/*.json
        base_dir = video_path if video_path.is_dir() else video_path.parent
        meta_file = base_dir / 'production_meta.json'
        if meta_file.exists():
            meta_path = str(meta_file)
        else:
            meta_jsons = list(base_dir.glob('scripts/*.json')) or list(base_dir.glob('*.json'))
            meta_path = str(meta_jsons[0]) if meta_jsons else None

        # Find thumbnail
        base_dir = video_path if video_path.is_dir() else video_path.parent
        thumb_candidates = list(base_dir.glob('thumbnail.*'))
        thumbnail_path = str(thumb_candidates[0]) if thumb_candidates else None

        upload_privacy = 'private' if LAUNCH_MODE else 'public'
        video_id = uploader.upload_video(
            str(video_file),
            script_path=script_path,
            meta_path=meta_path,
            privacy=upload_privacy,
            thumbnail_path=thumbnail_path,
        )

        if video_id:
            self._pending_uploads.remove(video_info)
            self._save_upload_queue()
            log.info(f"Uploaded {video_info['type']} as {upload_privacy}: {video_id}")
            self._notify_upload(video_id, video_info.get('type', 'unknown'), meta_path)

    def task_upload_pending(self, upload_type: str = 'short'):
        """Legacy: Upload pending videos of a given type (kept for backward compat)."""
        if SKIP_UPLOAD:
            log.info(f"  SKIP_UPLOAD=True — skipping pending upload ({upload_type})")
            return
        if self.dry_run:
            for v in self._pending_uploads:
                if v.get('upload_time') == upload_type or v.get('priority'):
                    log.info(f"[DRY] Would upload: {v['path']}")
            return

        from src.crypto.channel_uploader import CryptoUploader
        uploader = CryptoUploader()

        to_upload = [v for v in self._pending_uploads
                     if v.get('upload_time') == upload_type or v.get('priority')]

        for video in to_upload:
            video_path = Path(video['path'])

            # Find the actual video file (FINAL.mp4)
            if video_path.is_dir():
                mp4s = list(video_path.glob('*FINAL.mp4'))
                if mp4s:
                    video_file = mp4s[0]
                else:
                    log.warning(f"No FINAL.mp4 found in {video_path}")
                    continue
            else:
                video_file = video_path

            # Find script for metadata
            script_files = list(video_path.parent.glob('scripts/*.txt')) if video_path.is_file() else list(video_path.glob('scripts/*.txt'))
            script_path = str(script_files[0]) if script_files else None

            # Find metadata
            meta_path = None
            meta_file = (video_path if video_path.is_dir() else video_path.parent) / 'production_meta.json'
            if meta_file.exists():
                meta_path = str(meta_file)

            # Find thumbnail
            base_dir = video_path if video_path.is_dir() else video_path.parent
            thumb_candidates = list(base_dir.glob('thumbnail.*'))
            thumbnail_path = str(thumb_candidates[0]) if thumb_candidates else None

            video_id = uploader.upload_video(
                str(video_file),
                script_path=script_path,
                meta_path=meta_path,
                privacy='private' if LAUNCH_MODE else 'public',
                thumbnail_path=thumbnail_path,
            )

            if video_id:
                self._pending_uploads.remove(video)
                self._save_upload_queue()
                log.info(f"Uploaded {video['type']} as public: {video_id}")

    # ─── Prediction Tracker ───

    def task_check_predictions(self):
        """Check matured predictions and update scorecard."""
        if self.dry_run:
            log.info("[DRY] Would check predictions")
            return
        try:
            subprocess.run(
                [sys.executable, 'src/crypto/prediction_tracker.py', '--report', '--chart'],
                timeout=120, capture_output=True
            )
            log.info("Prediction check complete")
        except Exception as e:
            log.error(f"Prediction check failed: {e}")

    # ─── Level 5: Auto-Evaluation + Feedback Loop ───

    def task_evaluate_predictions(self):
        """Daily: evaluate matured predictions using local prices and update rolling accuracy.

        Level 5 feedback loop — runs at 01:00 UTC.
        Uses local price data only (no API calls).
        Also feeds prediction accuracy back into Thompson Sampling (content_optimizer)
        so the content mix adapts toward video types with better forecast accuracy.
        """
        if self.dry_run:
            log.info("[DRY] Would evaluate predictions from local DB")
            return
        try:
            from src.crypto.prediction_tracker import (
                evaluate_from_local_db, update_rolling_accuracy, generate_scorecard_report
            )
            results = evaluate_from_local_db()
            if results:
                update_rolling_accuracy()
                hits = sum(1 for r in results if r.get('correct'))
                log.info(f"Prediction evaluation: {len(results)} scored, "
                         f"{hits} hits ({hits/len(results)*100:.0f}%)")

                # Feed accuracy back into Thompson Sampling per video_type
                self._update_thompson_from_predictions(results)
            else:
                log.info("Prediction evaluation: no matured predictions to score")

            # Always regenerate scorecard (persists to DB for other components)
            try:
                report = generate_scorecard_report()
                log.info(f"Scorecard updated: {report.get('total', 0)} predictions, "
                         f"{report.get('win_rate', 0):.1f}% win rate")
            except Exception as e:
                log.warning(f"Scorecard generation failed: {e}")

        except Exception as e:
            log.warning(f"Prediction evaluation failed: {e}")

    def _update_thompson_from_predictions(self, results: list):
        """Convert prediction accuracy per video_type into Thompson Sampling updates.

        Groups evaluated predictions by video_type, computes accuracy rate per type,
        and maps it to a 0-10 score for content_optimizer.update_thompson().

        Scoring: accuracy_rate * 10 (e.g. 80% accuracy -> score 8.0).
        The Thompson threshold is 5.0 (>5 = alpha++, <=5 = beta++),
        so video types with >50% accuracy get rewarded.
        """
        try:
            from collections import defaultdict
            from src.crypto.content_optimizer import ContentOptimizer

            # Group results by video_type
            by_type = defaultdict(list)
            for r in results:
                vtype = r.get('video_type')
                if vtype:
                    by_type[vtype].append(r)

            if not by_type:
                log.info("Thompson update: no results with video_type, skipping")
                return

            optimizer = ContentOptimizer()
            for vtype, type_results in by_type.items():
                hits = sum(1 for r in type_results if r.get('correct'))
                total = len(type_results)
                accuracy = hits / total
                # Map accuracy to 0-10 score
                score = accuracy * 10.0
                optimizer.update_thompson(vtype, score)
                log.info(f"Thompson update: {vtype} -- {hits}/{total} "
                         f"({accuracy:.0%}) -> score {score:.1f}")
        except Exception as e:
            log.warning(f"Thompson update from predictions failed: {e}")

    # ─── Title A/B Testing ───

    def task_check_title_ab(self):
        """Check pending title A/B tests and swap underperformers after 12h."""
        if self.dry_run:
            log.info("[DRY] Would check title A/B tests")
            return
        try:
            from src.crypto.title_ab_tester import TitleABTester
            tester = TitleABTester()
            results = tester.check_all_pending(hours_elapsed=12.0)
            if results:
                swapped = sum(1 for r in results if r.get('swapped'))
                log.info(f"Title A/B: {len(results)} checked, {swapped} swapped")
            else:
                log.debug("Title A/B: no pending tests ready")
        except Exception as e:
            log.error(f"Title A/B check failed: {e}")

    # ─── Community Posts ───

    def task_post_community_predictions(self):
        """Post top forecast signals to YouTube community tab (1x daily after micro_shorts)."""
        if self.dry_run:
            log.info("[DRY] Would post community predictions")
            return
        try:
            from src.crypto.forecast_engine import forecast_all
            from src.crypto.channel_uploader import CryptoUploader

            forecasts = forecast_all()
            uploader = CryptoUploader()

            # Get rolling 30d accuracy from local DB
            accuracy = None
            try:
                import sqlite3 as _sql
                _aconn = _sql.connect(str(DB_PATH), timeout=60)
                _aconn.execute("PRAGMA journal_mode=WAL")
                _aconn.execute("PRAGMA busy_timeout=60000")
                _row = _aconn.execute(
                    "SELECT AVG(accuracy_30d) FROM accuracy_rolling "
                    "WHERE date = (SELECT MAX(date) FROM accuracy_rolling) "
                    "AND accuracy_30d IS NOT NULL"
                ).fetchone()
                _aconn.close()
                if _row and _row[0] is not None:
                    accuracy = _row[0] / 100.0  # DB stores as percentage
            except Exception:
                pass

            # Find the strongest non-NEUTRAL signal
            best = None
            for f in forecasts:
                if f.get('error'):
                    continue
                pred = f.get('prediction', 'NEUTRAL')
                if pred == 'NEUTRAL':
                    continue
                score = abs(f.get('composite_score', 0))
                if not best or score > abs(best.get('composite_score', 0)):
                    best = f

            if best:
                coin = best['coin']
                prediction = best['prediction']
                confidence = best.get('confidence', 0.5)
                price = best.get('current_price', 0)
                target = best.get('target_price', price)
                support = best.get('support')
                resistance = best.get('resistance')

                post_id = uploader.post_community_prediction(
                    coin=coin,
                    prediction=prediction,
                    confidence=confidence,
                    price=price,
                    target=target,
                    support=support,
                    resistance=resistance,
                    accuracy_30d=accuracy,
                )
                if post_id:
                    log.info(f"Community post: {coin} {prediction} (id={post_id})")
                else:
                    log.warning("Community prediction post failed or API not available")
            else:
                log.info("Community post: no strong signals today, skipping")
        except Exception as e:
            log.error(f"Community prediction post failed: {e}")

    # ─── Self-Improvement Loops ───

    def task_analyze_comments(self):
        """Loop 5: Analyze YouTube comments for audience feedback."""
        if self.dry_run:
            log.info("[DRY] Would analyze comments")
            return
        try:
            from src.crypto.comment_analyzer import CommentAnalyzer
            ca = CommentAnalyzer()
            total = ca.fetch_all_video_comments()
            if total > 0:
                ca.analyze_comments(days=7)
            log.info(f"Comment analysis: {total} comments processed")
        except Exception as e:
            log.error(f"Comment analysis failed: {e}")

    def task_collect_youtube_analytics(self):
        """Loop 3: Collect YouTube analytics for recent uploads."""
        if self.dry_run:
            log.info("[DRY] Would collect YouTube analytics")
            return
        try:
            from src.uploaders.youtube_analytics import YouTubeAnalyticsTracker
            ya = YouTubeAnalyticsTracker()
            if ya.youtube is None:
                log.warning("YouTube analytics: no credentials, skipping")
                return
            # Register any new uploads that aren't tracked yet
            import sqlite3 as _sql
            upload_conn = _sql.connect('data/crypto/uploads.db', timeout=10)
            uploads = upload_conn.execute(
                "SELECT youtube_id, title, video_type FROM uploads "
                "WHERE uploaded_at > datetime('now', '-7 days')"
            ).fetchall()
            upload_conn.close()
            for yt_id, title, vtype in uploads:
                try:
                    ya.register_video(yt_id, title, vtype)
                except Exception:
                    pass  # Already registered
            # Update metrics for all tracked videos
            updated = ya.update_all_videos()
            log.info(f"YouTube analytics: updated {updated} videos")

            # ── BRIDGE: Feed YouTube metrics into Thompson Sampling ──
            # This is the missing link: analytics → content_optimizer → better decisions
            try:
                self._feed_thompson_from_analytics(ya)
            except Exception as e2:
                log.warning(f"  Thompson feeding failed: {e2}")

        except Exception as e:
            log.error(f"YouTube analytics collection failed: {e}")

    def _feed_thompson_from_analytics(self, ya):
        """Bridge: YouTube analytics → Thompson Sampling.

        For each video with 48h+ of data, compute a quality score (0-10)
        and feed it into the content_optimizer's Thompson arms.

        Score formula (research-backed):
          views_score (30%) + engagement_rate (40%) + velocity (30%)
        where engagement = (likes + comments*5) / views
        and velocity = views / hours_since_upload
        """
        import sqlite3

        # Read analytics data
        analytics_conn = sqlite3.connect(str(ya.db_path), timeout=10)
        rows = analytics_conn.execute("""
            SELECT v.video_id, v.title, v.video_type,
                   m.views, m.likes, m.comments, m.collected_at,
                   v.uploaded_at
            FROM videos v
            JOIN metrics m ON v.video_id = m.video_id
            WHERE m.collected_at = (
                SELECT MAX(m2.collected_at) FROM metrics m2 WHERE m2.video_id = m.video_id
            )
            AND v.uploaded_at < datetime('now', '-48 hours')
        """).fetchall()
        analytics_conn.close()

        if not rows:
            return

        # Feed into Thompson Sampling
        try:
            from src.crypto.content_optimizer import ContentOptimizer
            co = ContentOptimizer()

            fed = 0
            for video_id, title, video_type, views, likes, comments, collected_at, uploaded_at in rows:
                if not views or views < 1:
                    continue

                # Compute quality score (0-10)
                engagement_rate = (likes + (comments or 0) * 5) / max(views, 1)
                # Normalize: 0.05 engagement = score 5, 0.10 = score 8
                engagement_score = min(engagement_rate / 0.012, 10)

                # Views velocity (views per 24h)
                try:
                    from datetime import datetime
                    upload_dt = datetime.fromisoformat(uploaded_at.replace('Z', '+00:00'))
                    collect_dt = datetime.fromisoformat(collected_at.replace('Z', '+00:00'))
                    hours = max((collect_dt - upload_dt).total_seconds() / 3600, 1)
                    daily_views = views / hours * 24
                    # 100 views/day = score 5, 500 = score 8, 1000+ = score 10
                    velocity_score = min(daily_views / 100, 10)
                except Exception:
                    velocity_score = 5

                # Final score
                score = engagement_score * 0.4 + velocity_score * 0.3 + min(views / 200, 10) * 0.3

                # Map video_type to Thompson arm
                arm_type = video_type
                if arm_type in ('micro_short', 'daily_short'):
                    arm_type = 'micro_short'

                co.update_thompson(arm_type, score)
                fed += 1

            if fed > 0:
                log.info(f"  Thompson Sampling: fed {fed} videos with real YouTube metrics")

        except Exception as e:
            log.warning(f"  Thompson feed error: {e}")

    def task_analyze_retention(self):
        """Loop 4: Analyze video retention for videos older than 7 days."""
        if self.dry_run:
            log.info("[DRY] Would analyze retention")
            return
        try:
            from src.engines.vision_retention_analyzer import VisionRetentionAnalyzer
            analyzer = VisionRetentionAnalyzer()
            analyzer.analyze_recent(min_age_days=7)
            log.info("Retention analysis complete")
        except Exception as e:
            log.error(f"Retention analysis failed: {e}")

    def task_weekly_optimize(self):
        """Weekly: Run content optimizer (Thompson Sampling + all feedback loops)."""
        if self.dry_run:
            log.info("[DRY] Would run weekly optimization")
            return
        try:
            from src.crypto.content_optimizer import ContentOptimizer
            co = ContentOptimizer()
            recs = co.run_weekly_optimization()
            log.info(f"Weekly optimization: coin rotation = {recs.get('coin_rotation', [])}")
        except Exception as e:
            log.error(f"Weekly optimization failed: {e}")

    def task_daily_forti_refresh(self):
        """Daily: replace FORTI clips that have been used 3+ times.
        Runs at 04:00 UTC (before production starts).
        Generates fresh Kling I2V clips for stale ones, keeping visual content fresh."""
        if self.dry_run:
            log.info("[DRY] Would refresh stale FORTI clips")
            return
        try:
            from src.crypto.forti_manager import refresh_stale_clips, get_stale_clips, FortiBudgetTracker
            stale = get_stale_clips()
            if stale:
                log.info(f"FORTI refresh: {len(stale)} stale clips found")
                budget = FortiBudgetTracker()
                refreshed = refresh_stale_clips(max_clips=5, budget=budget)
                log.info(f"FORTI refresh: {refreshed} clips replaced")
                if refreshed > 0:
                    self._notify(f"🔄 FORTI оновлення", f"Замінено {refreshed} застарілих кліпів (всього застарілих: {len(stale)})", "info")
            else:
                log.info("FORTI refresh: no stale clips")
        except Exception as e:
            log.error(f"FORTI refresh failed: {e}")

    def _legacy_weekly_forti_refresh(self):
        """DEPRECATED: replaced by task_daily_forti_refresh with usage tracking."""
        pass

    def _old_task_weekly_forti_refresh(self):
        """Generate 5 new FORTI clips weekly for visual variety.
        DEPRECATED — replaced by daily usage-based refresh.
        Retires oldest 5 clips to maintain ~40 clip inventory."""
        if self.dry_run:
            log.info("[DRY] Would refresh FORTI clips")
            return
        try:
            from src.crypto.forti_manager import FORTIManager
            fm = FORTIManager()

            # Generate new clips with varied poses/sentiments
            new_combos = [
                ('analyst', 'neutral'), ('bearish', 'cautious'),
                ('bullish', 'confident'), ('thinker', 'bearish'),
                ('presenting', 'bullish'),
            ]

            generated = 0
            for pose, sentiment in new_combos:
                for aspect in ['9x16', '16x9']:
                    try:
                        clip = fm.get_or_generate_clip(pose, sentiment, duration=5, aspect_ratio=aspect)
                        if clip:
                            generated += 1
                    except Exception as e:
                        log.warning(f"  FORTI clip {pose}_{sentiment}_{aspect}: {e}")

            log.info(f"  FORTI refresh: {generated} new clips generated")
            self._notify("🔄 FORTI кліпи оновлено", f"Згенеровано {generated} нових кліпів", "info")
        except Exception as e:
            log.warning(f"  FORTI refresh failed: {e}")

    def task_weekly_ml_retrain(self):
        """Weekly: Check live accuracy and retrain ML models if needed."""
        if self.dry_run:
            log.info("[DRY] Would check ML model health and retrain if needed")
            return
        try:
            from src.crypto.auto_retrain import auto_retrain, retrain_v5_ranking
            result = auto_retrain(force=False)
            log.info(f"Weekly ML retrain: {result['action']}")
            if result['action'] == 'retrained':
                details = result.get('details', {})
                acc = details.get('accuracy_check', {}).get('accuracy_30d')
                if acc is not None:
                    log.info(f"  Triggered by accuracy: {acc:.1%}")
            # Also retrain v5 ranking model
            v5_result = retrain_v5_ranking(force=False)
            log.info(f"Weekly v5 ranking retrain: {v5_result['action']}")
        except Exception as e:
            log.error(f"Weekly ML retrain failed: {e}")

    def task_monthly_retrain(self):
        """Monthly: Retrain forecast model (mass_trainer + weight_optimizer)."""
        if self.dry_run:
            log.info("[DRY] Would retrain forecast model")
            return
        try:
            subprocess.run(
                [sys.executable, 'src/crypto/mass_trainer.py'],
                timeout=600, capture_output=True
            )
            log.info("Monthly retraining complete")
        except Exception as e:
            log.error(f"Monthly retraining failed: {e}")

    # ─── Maintenance ───

    def task_daily_backup(self):
        """Backup critical databases."""
        if self.dry_run:
            log.info("[DRY] Would backup databases")
            return

        BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        today = datetime.now(timezone.utc).strftime('%Y%m%d')

        for db_file in [DB_PATH, PATTERNS_DB, OPTIMIZED_CONFIG]:
            if db_file.exists():
                dest = BACKUP_DIR / f"{db_file.stem}_{today}{db_file.suffix}"
                shutil.copy2(db_file, dest)
                log.info(f"  Backed up: {db_file.name} -> {dest.name}")

        # Clean old backups
        cutoff = datetime.now(timezone.utc) - timedelta(days=BACKUP_KEEP_DAYS)
        for old_file in BACKUP_DIR.iterdir():
            if old_file.is_file():
                try:
                    mtime = datetime.fromtimestamp(old_file.stat().st_mtime, tz=timezone.utc)
                    if mtime < cutoff:
                        old_file.unlink()
                        log.info(f"  Cleaned old backup: {old_file.name}")
                except (OSError, ValueError):
                    pass

        # ── DATA RETENTION: prune old high-frequency data to control DB growth ──
        # market.db grows ~30MB/day. Keep 90d of 1h candles, 365d of 4h, unlimited 1d.
        try:
            conn = sqlite3.connect(str(DB_PATH), timeout=60)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=60000")
            now_ts = int(datetime.now(timezone.utc).timestamp())
            pruned = 0

            # 1h candles: keep last 90 days
            cutoff_1h = now_ts - 90 * 86400
            r = conn.execute("DELETE FROM prices WHERE timeframe='1h' AND timestamp < ?",
                             (cutoff_1h,))
            pruned += r.rowcount

            # 4h candles: keep last 365 days
            cutoff_4h = now_ts - 365 * 86400
            r = conn.execute("DELETE FROM prices WHERE timeframe='4h' AND timestamp < ?",
                             (cutoff_4h,))
            pruned += r.rowcount

            # Old signal tracking: keep last 365 days
            r = conn.execute("DELETE FROM signal_tracking WHERE fired_at < datetime('now', '-365 days')")
            pruned += r.rowcount

            # Old predictions: keep last 365 days
            r = conn.execute("DELETE FROM predictions WHERE created_at < datetime('now', '-365 days')")
            pruned += r.rowcount

            if pruned > 0:
                conn.execute("PRAGMA incremental_vacuum")
                log.info(f"  Data retention: pruned {pruned} old rows")

            conn.commit()
            conn.close()
        except Exception as e:
            log.warning(f"  Data retention failed: {e}")

    def _start_liquidation_listener(self):
        """Start liquidation WebSocket listener as subprocess.
        Only starts if not already running. Tracks PID to avoid orphans."""
        # Check if we already have a running listener
        if hasattr(self, '_liquidation_proc') and self._liquidation_proc:
            if self._liquidation_proc.poll() is None:
                log.info(f"  Liquidation listener already running (PID {self._liquidation_proc.pid})")
                return

        try:
            self._liquidation_proc = subprocess.Popen(
                [sys.executable, 'src/crypto/liquidation_listener.py'],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            log.info(f"Liquidation listener started (PID {self._liquidation_proc.pid})")
        except Exception as e:
            log.error(f"Failed to start liquidation listener: {e}")

    # ─── Main Loop ───

    def _get_week_number(self) -> int:
        """Get current week number (for coin rotation)."""
        return datetime.now(timezone.utc).isocalendar()[1]

    def run(self):
        """Main orchestrator loop."""
        log.info("=" * 60)
        log.info("FORTIX ORCHESTRATOR STARTING")
        self._notify("✅ FORTIX Оркестратор запущено",
                     f"Бюджет: ${DAILY_BUDGET_LIMIT}/день\nТестовий режим: {self.dry_run}", "success")
        log.info(f"  Budget limit: ${DAILY_BUDGET_LIMIT}/day")
        log.info(f"  Data collection: every {DATA_COLLECTION_INTERVAL_MIN} min")
        log.info(f"  Signal checks: every {SIGNAL_CHECK_INTERVAL_MIN} min")
        log.info(f"  News checks: every {NEWS_CHECK_INTERVAL_MIN} min (breaking news -> immediate short)")
        log.info(f"  Dry run: {self.dry_run}")
        log.info("=" * 60)

        # Recover unuploaded videos from today (crash recovery)
        recovered = self._scan_unuploaded_videos()
        if recovered:
            self._pending_uploads.extend(recovered)
            self._save_upload_queue()
            log.info(f"  Recovered {len(recovered)} unuploaded videos for upload")

        # Start liquidation listener
        if not self.dry_run:
            self._start_liquidation_listener()

        while self.running:
            try:
                now = datetime.now(timezone.utc)
                self._write_heartbeat()  # Heartbeat FIRST — watchdog sees we're alive

                # ─── Interval-based tasks ───
                if self._should_run('data_collection'):
                    self.task_collect_data()
                    self._mark_run('data_collection')

                if self._should_run('signal_check'):
                    self.task_check_signals()
                    self._mark_run('signal_check')

                # Fast triggers (every 5 min): news + derivatives + structural
                if self._should_run('news_check'):
                    self.task_check_fast_triggers()
                    self._mark_run('news_check')

                # ─── Political & Event Monitoring ───
                if self._should_run('political_scan'):
                    try:
                        from src.crypto.political_monitor import run_political_scan
                        result = run_political_scan()
                        log.info(f"  Political scan: {result.get('political_events', 0)} events, {result.get('macro_events', 0)} macro")
                    except Exception as e:
                        log.warning(f"  Political scan failed: {e}")
                    self._mark_run('political_scan')

                if self._should_run('event_scan'):
                    try:
                        from src.crypto.event_monitor import run_event_scan
                        result = run_event_scan()
                        log.info(f"  Event scan: {result.get('unlocks', 0)} unlocks, {result.get('hacks', 0)} hacks")
                    except Exception as e:
                        log.warning(f"  Event scan failed: {e}")
                    self._mark_run('event_scan')

                # ─── Orderbook imbalance (hourly) ───
                if self._should_run('orderbook_scan'):
                    try:
                        from src.crypto.orderbook_collector import collect_orderbook
                        collect_orderbook()
                    except Exception as e:
                        log.warning(f"  Orderbook scan failed: {e}")
                    self._mark_run('orderbook_scan')

                # ─── Daily tasks ───
                if self._should_run('prediction_check'):
                    self.task_check_predictions()
                    self._mark_run('prediction_check')

                # ─── Randomized daily content slots (produce + upload immediately) ───
                if not hasattr(self, '_daily_slots'):
                    self._daily_slots = self._generate_daily_schedule()

                # Regenerate schedule at midnight
                if now.hour == 0 and now.minute < 2:
                    self._daily_slots = self._generate_daily_schedule()

                for slot_name, slot_info in self._daily_slots.items():
                    if self._should_run_slot(slot_name, self._daily_slots):
                        for item in slot_info.get('content', []):
                            # Content Strategy v2: items are dicts with type + short_style
                            if isinstance(item, dict):
                                vtype = item.get('type', 'micro_short')
                                style = item.get('short_style')
                                topic_angle = item.get('topic_angle')
                                # Long rotating: determine by weekday
                                if vtype == '_long_rotating':
                                    vtype, kwargs = self._get_rotating_long(now)
                                    self._produce_and_upload(vtype, **kwargs)
                                elif style:
                                    extra = {}
                                    if topic_angle:
                                        extra['topic_angle'] = topic_angle
                                    self._produce_and_upload(vtype, short_style=style, **extra)
                                else:
                                    self._produce_and_upload(vtype)
                            else:
                                # Backward compat: string content type
                                self._produce_and_upload(item)
                        self._mark_slot_run(slot_name)

                # Community prediction post (daily after micro_shorts, 14:45 UTC)
                if self._should_run('community_post'):
                    self.task_post_community_predictions()
                    self._mark_run('community_post')

                # Title A/B check (daily 07:00 UTC — check uploads older than 12h)
                if self._should_run('title_ab_check'):
                    self.task_check_title_ab()
                    self._mark_run('title_ab_check')

                # Upload any remaining videos from persistent queue (crash recovery)
                if self._pending_uploads and now.minute % 15 == 0:
                    self._upload_remaining()

                if self._should_run('comment_analysis'):
                    self.task_analyze_comments()
                    self._mark_run('comment_analysis')

                if self._should_run('youtube_analytics'):
                    self.task_collect_youtube_analytics()
                    self._mark_run('youtube_analytics')

                if self._should_run('retention_analysis'):
                    self.task_analyze_retention()
                    self._mark_run('retention_analysis')

                if self._should_run('daily_backup'):
                    self.task_daily_backup()
                    self._mark_run('daily_backup')

                # ─── Level 5: Auto-Evaluation (01:00 UTC) ───
                if self._should_run('prediction_eval'):
                    self.task_evaluate_predictions()
                    self._mark_run('prediction_eval')

                # ─── v22: Self-Improvement Loop (02:00 UTC — after prediction eval) ───
                if self._should_run('self_improve'):
                    try:
                        from src.crypto.self_improver import run_daily_improvement
                        improvement = run_daily_improvement()
                        actions = improvement.get('actions', [])
                        log.info(f"  Self-improve: {len(actions)} actions taken")
                    except Exception as e:
                        log.error(f"  Self-improve failed: {e}")
                    self._mark_run('self_improve')

                # ─── v22: Refresh YouTube trending keywords (daily before production) ───
                if self._should_run('prediction_check'):  # Runs at 14:00, before video production
                    try:
                        from src.crypto.keyword_researcher import refresh_keywords
                        kw = refresh_keywords()
                        log.info(f"  Keywords refreshed: {len(kw.get('top_trending', []))} trending")
                    except Exception as e:
                        log.warning(f"  Keyword refresh failed: {e}")

                # ─── Weekly tasks (non-production: optimization + ML retrain) ───
                # NOTE: weekly long-form production moved to slot_6 (_long_rotating)
                # in Content Strategy v2 daily schedule

                if self._should_run('weekly_optimize'):
                    self.task_weekly_optimize()
                    self._mark_run('weekly_optimize')

                if self._should_run('daily_forti_refresh'):
                    self.task_daily_forti_refresh()
                    self._mark_run('daily_forti_refresh')

                if self._should_run('weekly_ml_retrain'):
                    self.task_weekly_ml_retrain()
                    self._mark_run('weekly_ml_retrain')

                # ─── Monthly tasks ───
                if self._should_run('monthly_retrain'):
                    self.task_monthly_retrain()
                    self._mark_run('monthly_retrain')

                # ─── Health checks ───
                if self._liquidation_proc and self._liquidation_proc.poll() is not None:
                    log.warning("Liquidation listener died -- restarting")
                    self._start_liquidation_listener()

                # Heartbeat file for external watchdog
                self._write_heartbeat()

            except Exception as e:
                log.error(f"Orchestrator cycle error: {e}", exc_info=True)
                self._notify("❌ Помилка циклу оркестратора", str(e)[:500], "error")
                self._consecutive_errors += 1
                if self._consecutive_errors >= 5:
                    log.critical("5 consecutive errors — restarting orchestrator")
                    self._notify("🔴 КРИТИЧНО: 5 помилок поспіль",
                                 "Автоматичний перезапуск оркестратора", "error")
                    break  # Exit loop → nssm/watchdog restarts us
            else:
                self._consecutive_errors = 0

            # Sleep 60 seconds between cycles
            time.sleep(60)

        log.info("Orchestrator stopped.")
        self._notify("⛔ FORTIX Оркестратор зупинено", "Сервіс завершує роботу", "warning")

    def _write_heartbeat(self):
        """Write heartbeat timestamp for external watchdog monitoring."""
        try:
            hb_path = Path('data/crypto/heartbeat.txt')
            hb_path.parent.mkdir(parents=True, exist_ok=True)
            hb_path.write_text(str(int(time.time())))
        except Exception:
            pass


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='FORTIX Orchestrator')
    parser.add_argument('--dry-run', action='store_true',
                        help='Log actions without executing')
    parser.add_argument('--once', action='store_true',
                        help='Run one cycle and exit')
    args = parser.parse_args()

    setup_logging(dry_run=args.dry_run)

    orch = Orchestrator(dry_run=args.dry_run)

    if args.once:
        # Run all tasks once (for testing)
        log.info("Running single cycle...")
        orch.task_collect_data()
        orch.task_check_signals()
        orch.task_daily_backup()
        log.info("Single cycle complete.")
    else:
        orch.run()
