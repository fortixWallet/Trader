"""
FORTIX — Forecast Model v4 (LightGBM with Macro Features)
===========================================================
Wrapper for the v4 LightGBM model trained with S&P500, NASDAQ, DXY, CPI.

Spearman: 0.182, BUY precision: 52.2%
"""

import json
import pickle
import logging
import sqlite3
import numpy as np
from pathlib import Path
from datetime import datetime, timezone

log = logging.getLogger('forecast_v4')

MODEL_DIR = Path('data/crypto/models_v4')
_model = None
_scaler = None
_features = None


def _load_model():
    """Load v4 model, scaler, and feature list."""
    global _model, _scaler, _features

    if _model is not None:
        return True

    model_path = MODEL_DIR / 'all_7d.lgb'
    scaler_path = MODEL_DIR / 'scaler.pkl'
    features_path = MODEL_DIR / 'features.json'

    if not model_path.exists():
        log.warning("Model v4 not found")
        return False

    try:
        import lightgbm as lgb
        _model = lgb.Booster(model_file=str(model_path))
        with open(scaler_path, 'rb') as f:
            _scaler = pickle.load(f)
        _features = json.loads(features_path.read_text())
        log.info(f"Model v4 loaded: {len(_features)} features")
        return True
    except Exception as e:
        log.warning(f"Model v4 load failed: {e}")
        return False


def predict_v4(conn: sqlite3.Connection, coin: str) -> float:
    """Predict 7-day return for a coin using v4 model.

    Returns predicted return (e.g., 0.05 = +5%) or None on failure.
    """
    if not _load_model():
        return None

    try:
        from src.crypto.feature_builder import FeatureBuilder

        fb = FeatureBuilder(db_path=str(Path('data/crypto/market.db')))
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        features = fb.build_features_single(coin, today)

        if not features:
            return None

        # Build feature vector in correct order
        X = []
        for f in _features:
            val = features.get(f, 0)
            if val is None or (isinstance(val, float) and np.isnan(val)):
                val = 0
            X.append(float(val))

        X = np.array([X])
        X_scaled = _scaler.transform(X)
        pred = _model.predict(X_scaled)[0]

        # Clip to reasonable range
        pred = float(np.clip(pred, -0.3, 0.3))

        return pred

    except Exception as e:
        log.debug(f"v4 predict failed for {coin}: {e}")
        return None
