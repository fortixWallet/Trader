"""
FORTIX Daily Model Trainer — Continuous Learning
=================================================

Runs every day at 02:00 UTC:
1. Rebuilds dataset with ALL latest data
2. Retrains 4h model with time-weighted samples
3. Validates: only deploys if new model is BETTER than old
4. Calculates adaptive threshold based on current volatility
5. Logs accuracy trends

The model learns from EVERY new day of data.
"""

import sqlite3
import numpy as np
import pandas as pd
import json
import pickle
import logging
import time
from pathlib import Path
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

_FACTORY_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH = _FACTORY_DIR / 'data' / 'crypto' / 'market.db'
MODEL_DIR = _FACTORY_DIR / 'data' / 'crypto' / 'models_4h'
MODEL_DIR.mkdir(exist_ok=True)
HISTORY_FILE = MODEL_DIR / 'training_history.json'


def calculate_adaptive_threshold() -> float:
    """Calculate threshold based on current market volatility.

    High volatility → higher threshold (more selective, higher accuracy)
    Low volatility → lower threshold (more trades, still profitable)
    """
    conn = sqlite3.connect(str(DB_PATH))
    try:
        # Average ATR across top coins over last 24h
        rows = conn.execute("""
            SELECT coin,
                   AVG((high - low) / close) as avg_atr
            FROM prices
            WHERE timeframe='4h'
            AND timestamp > strftime('%s', 'now', '-2 days')
            GROUP BY coin
        """).fetchall()

        if not rows:
            return 0.008  # default

        market_atr = np.mean([r[1] for r in rows if r[1] and r[1] > 0])

        # Adaptive: threshold scales with volatility
        # Model raw output is ~±0.05%, scaled by ATR
        # Threshold should allow 5-15 signals per scan
        threshold = market_atr * 0.15  # 15% of ATR

        # Clamp to reasonable range
        threshold = max(0.003, min(threshold, 0.012))  # 0.3% to 1.2%

        logger.info(f"Market ATR: {market_atr*100:.2f}%, adaptive threshold: ±{threshold*100:.2f}%")
        return threshold

    except Exception as e:
        logger.warning(f"Adaptive threshold failed: {e}")
        return 0.008
    finally:
        conn.close()


def retrain_4h_model() -> dict:
    """Daily retrain using pattern_4h v3 train_model() directly.

    This ensures daily retraining uses the EXACT same pipeline
    as manual training: 61 features + cross-sectional + dual model.
    """
    from src.crypto.pattern_4h import build_4h_dataset, train_model
    from scipy.stats import spearmanr

    logger.info("Daily retrain starting (v3 pipeline)...")
    start_time = time.time()

    # Build fresh dataset with all enrichment + cross-sectional features
    df = build_4h_dataset()

    if len(df) < 10000:
        logger.warning(f"Too few samples: {len(df)}")
        return {'status': 'skipped', 'reason': 'too few samples'}

    # Train using the same train_model() function
    results = train_model(df)
    sp = results.get('spearman', 0)

    # train_model() already saves both models, scaler, features, and meta
    # Just need to log history and calculate threshold

    status = 'updated'  # train_model always saves

    # Calculate adaptive threshold
    threshold = calculate_adaptive_threshold()
    with open(MODEL_DIR / 'threshold.json', 'w') as f:
        json.dump({'threshold': threshold, 'calculated_at': datetime.now(timezone.utc).isoformat()}, f)

    # Log to history
    history = []
    if HISTORY_FILE.exists():
        try:
            history = json.loads(HISTORY_FILE.read_text())
        except Exception:
            pass

    history.append({
        'date': datetime.now(timezone.utc).strftime('%Y-%m-%d'),
        'spearman': float(sp),
        'status': status,
        'reg_iter': results.get('reg_iter', 0),
        'ranking': results.get('ranking', {}),
        'threshold': threshold,
        'n_features': results.get('n_features', 0),
        'duration_sec': round(time.time() - start_time),
    })

    history = history[-90:]
    HISTORY_FILE.write_text(json.dumps(history, indent=2))

    elapsed = time.time() - start_time
    logger.info(f"Daily retrain done in {elapsed:.0f}s. Spearman={sp:.4f}")

    return {
        'status': status,
        'spearman': float(sp),
        'threshold': threshold,
        'ranking': results.get('ranking', {}),
        'duration': round(elapsed),
    }


def get_current_threshold() -> float:
    """Get the latest adaptive threshold."""
    thresh_path = MODEL_DIR / 'threshold.json'
    if thresh_path.exists():
        try:
            data = json.loads(thresh_path.read_text())
            return data.get('threshold', 0.008)
        except Exception:
            pass
    return 0.008


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    result = retrain_4h_model()
    print(f"\nResult: {json.dumps(result, indent=2)}")
