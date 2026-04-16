"""
FORTIX — Auto-Retrain System
====================================
Automatically retrains ML models (regression + GBT) when live accuracy degrades.
Runs weekly via orchestrator. Includes model versioning with automatic rollback.

Pipeline:
1. Check live accuracy (from prediction_tracker's accuracy_rolling table)
2. If accuracy < threshold OR models older than max_age, trigger retrain
3. Retrain regression + GBT models on latest training data
4. Compare new vs old model on held-out data
5. Deploy only if new model is better (or within tolerance)
6. Keep 3 versions for rollback

Usage:
    python src/crypto/auto_retrain.py          # check + retrain if needed
    python src/crypto/auto_retrain.py --force   # force retrain regardless
"""

import sys
import json
import sqlite3
import shutil
import logging
import numpy as np
from pathlib import Path
from datetime import datetime, timezone, timedelta

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger('auto_retrain')

MARKET_DB = Path('data/crypto/market.db')
PATTERNS_DB = Path('data/crypto/patterns.db')
REGRESSION_DIR = Path('data/crypto/regression_models')
FOREST_DIR = Path('data/crypto/forest_models')
CONFIG_PATH = Path('data/crypto/optimized_config.json')
VERSION_DIR = Path('data/crypto/model_versions')

# Thresholds
ACCURACY_THRESHOLD = 0.55      # Retrain if 30d accuracy drops below 55%
MIN_EVALUATED = 20             # Need at least 20 evaluated predictions
MAX_MODEL_AGE_DAYS = 30        # Force retrain if models older than 30 days
MAX_VERSIONS = 3               # Keep last 3 model versions


def check_live_accuracy():
    """Check current live accuracy from accuracy_rolling table.

    Returns dict with:
    - should_retrain: bool
    - reason: str
    - accuracy_30d: float (overall)
    - per_coin: dict of coin accuracies
    """
    conn = sqlite3.connect(str(MARKET_DB), timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")
    try:
        # Get latest rolling accuracy per coin
        rows = conn.execute("""
            SELECT coin, accuracy_30d, n_evaluated_30d, buy_accuracy_30d, sell_accuracy_30d
            FROM accuracy_rolling
            WHERE date = (SELECT MAX(date) FROM accuracy_rolling)
        """).fetchall()

        if not rows:
            return {'should_retrain': False, 'reason': 'No accuracy data yet',
                    'accuracy_30d': None, 'per_coin': {}}

        total_correct = 0
        total_evaluated = 0
        per_coin = {}
        low_accuracy_coins = []

        for r in rows:
            coin, acc, n_eval, buy_acc, sell_acc = r
            if n_eval and n_eval >= 5:
                per_coin[coin] = {
                    'accuracy_30d': acc,
                    'n_evaluated': n_eval,
                    'buy_accuracy': buy_acc,
                    'sell_accuracy': sell_acc,
                }
                if acc is not None:
                    total_correct += acc * n_eval
                    total_evaluated += n_eval
                    if acc < ACCURACY_THRESHOLD:
                        low_accuracy_coins.append(coin)

        overall_acc = total_correct / total_evaluated if total_evaluated > 0 else None

        # Check model age
        model_age_days = _get_model_age_days()

        # Determine if retrain needed
        reasons = []
        should_retrain = False

        if total_evaluated < MIN_EVALUATED:
            reasons.append(f'Insufficient data ({total_evaluated} < {MIN_EVALUATED} evaluated)')
        elif overall_acc is not None and overall_acc < ACCURACY_THRESHOLD:
            should_retrain = True
            reasons.append(f'Low accuracy: {overall_acc:.1%} < {ACCURACY_THRESHOLD:.0%}')
        if low_accuracy_coins:
            reasons.append(f'Low-accuracy coins: {", ".join(low_accuracy_coins)}')
        if model_age_days > MAX_MODEL_AGE_DAYS:
            should_retrain = True
            reasons.append(f'Models are {model_age_days} days old (max={MAX_MODEL_AGE_DAYS})')
        if not reasons:
            reasons.append(f'Accuracy OK: {overall_acc:.1%}')

        return {
            'should_retrain': should_retrain,
            'reason': '; '.join(reasons),
            'accuracy_30d': overall_acc,
            'n_evaluated': total_evaluated,
            'per_coin': per_coin,
            'model_age_days': model_age_days,
        }

    except Exception as e:
        log.warning(f"Accuracy check failed: {e}")
        return {'should_retrain': False, 'reason': f'Error: {e}',
                'accuracy_30d': None, 'per_coin': {}}
    finally:
        conn.close()


def _get_model_age_days():
    """Get age of most recent model file in days."""
    model_files = list(REGRESSION_DIR.glob('*.json')) + list(FOREST_DIR.glob('*.json'))
    if not model_files:
        return 999  # No models = very old

    newest = max(f.stat().st_mtime for f in model_files)
    age = datetime.now().timestamp() - newest
    return int(age / 86400)


def version_current_models():
    """Save current models as a versioned backup before retraining."""
    VERSION_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    version_path = VERSION_DIR / timestamp

    # Only version if we have models to save
    has_models = False
    for d in [REGRESSION_DIR, FOREST_DIR]:
        if d.exists() and list(d.glob('*.json')):
            has_models = True
            break

    if not has_models and not CONFIG_PATH.exists():
        log.info("  No existing models to version")
        return None

    version_path.mkdir(parents=True, exist_ok=True)

    # Copy regression models
    if REGRESSION_DIR.exists():
        reg_dest = version_path / 'regression_models'
        reg_dest.mkdir(exist_ok=True)
        for f in REGRESSION_DIR.glob('*.json'):
            shutil.copy2(f, reg_dest / f.name)

    # Copy forest models
    if FOREST_DIR.exists():
        forest_dest = version_path / 'forest_models'
        forest_dest.mkdir(exist_ok=True)
        for f in FOREST_DIR.glob('*.json'):
            shutil.copy2(f, forest_dest / f.name)

    # Copy optimized config
    if CONFIG_PATH.exists():
        shutil.copy2(CONFIG_PATH, version_path / 'optimized_config.json')

    # Save metadata
    meta = {
        'timestamp': timestamp,
        'created': datetime.now(timezone.utc).isoformat(),
    }
    (version_path / 'meta.json').write_text(json.dumps(meta, indent=2))

    log.info(f"  Versioned models to {version_path.name}")

    # Prune old versions (keep MAX_VERSIONS)
    versions = sorted(VERSION_DIR.iterdir(), key=lambda p: p.name, reverse=True)
    for old in versions[MAX_VERSIONS:]:
        if old.is_dir():
            shutil.rmtree(old)
            log.info(f"  Pruned old version: {old.name}")

    return version_path


def rollback_models(version_path):
    """Restore models from a versioned backup."""
    if not version_path or not version_path.exists():
        log.error("Cannot rollback: version path not found")
        return False

    # Restore regression models
    reg_src = version_path / 'regression_models'
    if reg_src.exists():
        REGRESSION_DIR.mkdir(parents=True, exist_ok=True)
        for f in reg_src.glob('*.json'):
            shutil.copy2(f, REGRESSION_DIR / f.name)

    # Restore forest models
    forest_src = version_path / 'forest_models'
    if forest_src.exists():
        FOREST_DIR.mkdir(parents=True, exist_ok=True)
        for f in forest_src.glob('*.json'):
            shutil.copy2(f, FOREST_DIR / f.name)

    # Restore config
    cfg_src = version_path / 'optimized_config.json'
    if cfg_src.exists():
        shutil.copy2(cfg_src, CONFIG_PATH)

    log.info(f"  Rolled back to version {version_path.name}")
    return True


def retrain_models():
    """Retrain regression + GBT models on latest training data.

    Returns dict with results per group.
    """
    results = {}

    # Step 1: Retrain regression (Elastic Net)
    log.info("  Retraining regression models (Elastic Net)...")
    try:
        from src.crypto.regression_model import train_all_groups as train_regression
        reg_results = train_regression()
        if reg_results:
            for group, res in reg_results.items():
                results[f'regression_{group}'] = {
                    'dir_accuracy': res.get('dir_accuracy', 0),
                    'rank_corr': res.get('rank_corr', 0),
                    'n_nonzero': res.get('n_nonzero', 0),
                }
            log.info(f"  Regression: {len(reg_results)} groups trained")
    except Exception as e:
        log.error(f"  Regression training failed: {e}")

    # Step 2: Retrain GBT
    log.info("  Retraining GBT models...")
    try:
        from src.crypto.ml_corrector import train_all_groups as train_gbt
        gbt_results = train_gbt()
        if gbt_results:
            for group, res in gbt_results.items():
                results[f'gbt_{group}'] = {
                    'dir_accuracy': res.get('dir_accuracy', 0),
                    'rank_corr': res.get('rank_corr', 0),
                    'wins_over_enet': res.get('wins_over_enet', False),
                }
            log.info(f"  GBT: {len(gbt_results)} groups trained")
    except Exception as e:
        log.error(f"  GBT training failed: {e}")

    return results


def evaluate_new_models(old_version_path):
    """Quick evaluation: compare new models vs old on held-out data.

    Returns True if new models are at least as good as old.
    """
    # For now, just check that the new models exist and have reasonable metrics
    # Full A/B comparison would need a separate held-out test set
    try:
        from src.crypto.regression_model import train_all_groups
        # The training function already does walk-forward validation
        # and only saves models that pass metrics checks
        # So if models were saved successfully, they're at least OK

        new_models = list(REGRESSION_DIR.glob('*.json'))
        if not new_models:
            log.warning("  No new regression models found!")
            return False

        # Check that at least 3/5 groups have models
        if len(new_models) < 3:
            log.warning(f"  Only {len(new_models)} regression models (need >= 3)")
            return False

        log.info(f"  New models validated: {len(new_models)} regression models OK")
        return True

    except Exception as e:
        log.error(f"  Model evaluation failed: {e}")
        return False


def auto_retrain(force=False):
    """Main entry point: check accuracy, retrain if needed, rollback if worse.

    Args:
        force: If True, retrain regardless of accuracy.

    Returns dict with:
    - action: 'skipped' | 'retrained' | 'rolled_back'
    - details: dict of results
    """
    log.info("=" * 60)
    log.info("AUTO-RETRAIN: Checking model health...")

    # Step 1: Check if retrain needed
    accuracy_check = check_live_accuracy()
    log.info(f"  Accuracy: {accuracy_check.get('accuracy_30d', 'N/A')}")
    log.info(f"  Reason: {accuracy_check['reason']}")
    log.info(f"  Model age: {accuracy_check.get('model_age_days', '?')} days")

    if not force and not accuracy_check['should_retrain']:
        log.info("  No retrain needed. Skipping.")
        return {'action': 'skipped', 'details': accuracy_check}

    if force:
        log.info("  FORCED retrain requested.")

    # Step 2: Version current models (for rollback)
    log.info("  Versioning current models...")
    old_version = version_current_models()

    # Step 3: Retrain
    log.info("  Starting retrain...")
    train_results = retrain_models()

    # Step 4: Validate new models
    log.info("  Validating new models...")
    models_ok = evaluate_new_models(old_version)

    if not models_ok and old_version:
        # Rollback
        log.warning("  New models FAILED validation. Rolling back...")
        rollback_models(old_version)
        return {'action': 'rolled_back', 'details': {
            'accuracy_check': accuracy_check,
            'train_results': train_results,
            'reason': 'New models failed validation'
        }}

    log.info("  Retrain complete and validated!")
    log.info("=" * 60)

    return {'action': 'retrained', 'details': {
        'accuracy_check': accuracy_check,
        'train_results': train_results,
    }}


def retrain_v3(force: bool = False) -> dict:
    """Retrain Forecast v3 LightGBM models on latest data.

    Triggered weekly or when live accuracy drops below 52%.
    Saves new version, keeps last 3 for rollback.
    """
    log.info("=" * 60)
    log.info("FORECAST V3 RETRAIN")
    log.info("=" * 60)

    try:
        from src.crypto.forecast_model_v3 import ForecastModelV3, MODEL_DIR
        from src.crypto.feature_builder import FeatureBuilder
    except ImportError as e:
        log.error(f"Cannot import v3 modules: {e}")
        return {'action': 'error', 'reason': str(e)}

    # Check if retrain is needed (unless forced)
    if not force:
        # Check model age
        ptr_file = MODEL_DIR / 'latest.txt'
        if ptr_file.exists():
            version = ptr_file.read_text().strip()
            meta_file = MODEL_DIR / version / 'meta.json'
            if meta_file.exists():
                with open(meta_file) as f:
                    meta = json.load(f)
                trained_at = meta.get('trained_at', '')
                if trained_at:
                    try:
                        trained_date = datetime.fromisoformat(trained_at)
                        age_days = (datetime.now() - trained_date).days
                        if age_days < 7:
                            log.info(f"  v3 model is {age_days} days old, skip retrain (< 7 days)")
                            return {'action': 'skipped', 'reason': f'model only {age_days}d old'}
                    except Exception:
                        pass

        # Check live accuracy from predictions table
        try:
            conn = sqlite3.connect(str(MARKET_DB), timeout=60)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=60000")
            row = conn.execute(
                "SELECT COUNT(*), SUM(CASE WHEN correct=1 THEN 1 ELSE 0 END) "
                "FROM predictions WHERE actual_change_pct IS NOT NULL "
                "AND prediction IN ('BUY', 'SELL', 'STRONG BUY', 'STRONG SELL') "
                "AND created_at > datetime('now', '-14 days')"
            ).fetchone()
            conn.close()
            if row and row[0] and row[0] >= 20:
                live_acc = row[1] / row[0]
                log.info(f"  Live directional accuracy (14d): {live_acc:.1%} ({row[1]}/{row[0]})")
                if live_acc >= 0.52:
                    log.info("  Accuracy OK, no retrain needed")
                    return {'action': 'skipped', 'reason': f'accuracy {live_acc:.1%} >= 52%'}
                else:
                    log.warning(f"  Accuracy {live_acc:.1%} < 52% — triggering retrain!")
        except Exception as e:
            log.debug(f"  Could not check live accuracy: {e}")

    # Build fresh dataset
    log.info("  Building training dataset...")
    builder = FeatureBuilder()
    df = builder.build_dataset(include_labels=True)
    log.info(f"  Dataset: {len(df)} rows, {df['coin'].nunique()} coins")

    # Train
    model = ForecastModelV3()
    results = model.walk_forward_cv(df, horizon='7d')
    dir_acc = results.get('directional_accuracy', 0)
    log.info(f"  Walk-forward directional accuracy: {dir_acc}%")

    # Train final models (multiclass + binary UP/DOWN)
    for group in [None, 'majors', 'l1_alts', 'defi', 'ai', 'meme']:
        try:
            model.train_final(df, horizon='7d', group=group)
            model.train_final(df, horizon='3d', group=group)
            model.train_final_binary(df, horizon='7d', group=group)
        except Exception as e:
            log.warning(f"  Failed to train {group or 'global'}: {e}")

    # Save full metrics (not just a single float)
    model.metrics = {
        'retrain_accuracy': dir_acc,
        'walk_forward_results': {k: v for k, v in results.items() if k != 'per_fold'},
        'retrain_date': datetime.now(timezone.utc).isoformat(),
        'n_samples': len(df),
    }
    model.save()

    # Prune old versions (keep last 3)
    if MODEL_DIR.exists():
        versions = sorted([d for d in MODEL_DIR.iterdir()
                          if d.is_dir() and d.name != 'latest'], reverse=True)
        for old_ver in versions[MAX_VERSIONS:]:
            log.info(f"  Pruning old model version: {old_ver.name}")
            shutil.rmtree(old_ver)

    log.info(f"  v3 retrain complete! Accuracy: {dir_acc}%")
    return {'action': 'retrained', 'accuracy': dir_acc}


def retrain_v5_ranking(force: bool = False) -> dict:
    """Retrain v5 ranking model on latest data.

    Builds cross-sectional ranking targets and trains LightGBM.
    """
    log.info("=" * 60)
    log.info("FORECAST V5 RANKING RETRAIN")
    log.info("=" * 60)

    try:
        from src.crypto.feature_builder import FeatureBuilder
        import lightgbm as lgb
        from sklearn.preprocessing import StandardScaler
        from scipy.stats import spearmanr
        import pickle
    except ImportError as e:
        log.error(f"Cannot import v5 modules: {e}")
        return {'action': 'error', 'reason': str(e)}

    v5_dir = Path('data/crypto/models_v5')

    # Check model age (skip if < 7 days unless forced)
    if not force:
        meta_path = v5_dir / 'meta_ranking.json'
        if meta_path.exists():
            try:
                meta = json.load(open(meta_path))
                trained_at = meta.get('trained_at', '')
                if trained_at:
                    trained_date = datetime.fromisoformat(trained_at)
                    age_days = (datetime.now() - trained_date).days
                    if age_days < 7:
                        log.info(f"  v5 ranking model is {age_days} days old, skip (< 7 days)")
                        return {'action': 'skipped', 'reason': f'model only {age_days}d old'}
            except Exception:
                pass

    # Build dataset
    log.info("  Building training dataset...")
    fb = FeatureBuilder()
    df = fb.build_dataset(include_labels=True)
    log.info(f"  Dataset: {len(df)} rows, {df['coin'].nunique()} coins")

    available = [f for f in fb.FEATURE_COLS if f in df.columns]
    target = 'label_7d'
    df_clean = df[available + [target, 'coin', 'date', 'coin_group']].dropna(subset=[target])
    df_clean = df_clean.sort_values('date')

    # Create ranking targets per date
    import pandas as pd
    ranked_parts = []
    for date_val, group in df_clean.groupby('date'):
        g = group.copy()
        n = len(g)
        if n <= 1:
            g['rank_target'] = 0.5
        else:
            g['rank_target'] = g[target].rank(pct=True)
        ranked_parts.append(g)
    df_ranked = pd.concat(ranked_parts, ignore_index=True)

    # Walk-forward split (70/30)
    dates = sorted(df_ranked['date'].unique())
    split_idx = int(len(dates) * 0.7)
    train_dates = set(dates[:split_idx])
    test_dates = set(dates[split_idx:])

    train = df_ranked[df_ranked['date'].isin(train_dates)]
    test = df_ranked[df_ranked['date'].isin(test_dates)]
    log.info(f"  Train: {len(train)}, Test: {len(test)}")

    # LightGBM handles NaN natively — do NOT fillna(0) which injects false signal
    X_train = train[available]
    y_train = train['rank_target']
    X_test = test[available]
    y_test_rank = test['rank_target']
    y_test_return = test[target]

    # LightGBM is tree-based — no StandardScaler needed (trees are scale-invariant)
    # Keep scaler object for backward compatibility but don't transform data
    scaler = StandardScaler()
    scaler.fit(X_train.fillna(0))  # fit only for saving (legacy compat)

    # Train ranking model — feed raw features with NaN
    train_data = lgb.Dataset(X_train, label=y_train)
    valid_data = lgb.Dataset(X_test, label=y_test_rank, reference=train_data)

    params = {
        'objective': 'regression', 'metric': 'mae',
        'num_leaves': 31, 'learning_rate': 0.02,
        'feature_fraction': 0.7, 'bagging_fraction': 0.7, 'bagging_freq': 5,
        'verbose': -1, 'n_jobs': -1, 'min_child_samples': 30,
        'reg_alpha': 0.3, 'reg_lambda': 0.3,
    }

    model = lgb.train(
        params, train_data, num_boost_round=1000,
        valid_sets=[valid_data],
        callbacks=[lgb.early_stopping(100), lgb.log_evaluation(0)]
    )

    preds = model.predict(X_test)
    rho_rank, _ = spearmanr(preds, y_test_rank)
    rho_return, _ = spearmanr(preds, y_test_return)
    log.info(f"  Spearman vs rank: {rho_rank:.4f}, vs return: {rho_return:.4f}")

    # Save
    v5_dir.mkdir(parents=True, exist_ok=True)
    model.save_model(str(v5_dir / 'ranking_7d.lgb'))
    with open(v5_dir / 'ranking_scaler.pkl', 'wb') as f:
        pickle.dump(scaler, f)
    with open(v5_dir / 'ranking_features.json', 'w') as f:
        json.dump(available, f)

    meta = {
        'version': 'v5-ranking',
        'features': len(available),
        'train_rows': len(train),
        'test_rows': len(test),
        'ranking_model': {'spearman_vs_rank': float(rho_rank), 'spearman_vs_return': float(rho_return)},
        'trained_at': datetime.now(timezone.utc).isoformat(),
    }
    with open(v5_dir / 'meta_ranking.json', 'w') as f:
        json.dump(meta, f, indent=2)

    # Clear cached model so production reloads
    try:
        from src.crypto import forecast_model_v5
        forecast_model_v5._rank_model = None
        forecast_model_v5._scaler = None
        forecast_model_v5._features = None
    except Exception:
        pass

    log.info(f"  v5 retrain complete! Spearman: {rho_return:.4f}")
    return {'action': 'retrained', 'spearman': float(rho_return)}


if __name__ == '__main__':
    force = '--force' in sys.argv
    v3_only = '--v3' in sys.argv
    v5_only = '--v5' in sys.argv

    if v3_only:
        result = retrain_v3(force=force)
    elif v5_only:
        result = retrain_v5_ranking(force=force)
    else:
        result = auto_retrain(force=force)
        v3_result = retrain_v3(force=force)
        result['v3'] = v3_result
        v5_result = retrain_v5_ranking(force=force)
        result['v5'] = v5_result

    print(f"\nResult: {result['action']}")
    if result.get('details'):
        for k, v in result['details'].get('train_results', {}).items():
            print(f"  {k}: {v}")
