"""
Ensemble Predictor — 3 models vote for stronger signals
========================================================
LightGBM + XGBoost + Neural Net
Trade only when 2/3 agree on direction.
"""

import json
import pickle
import logging
import numpy as np
from pathlib import Path

logger = logging.getLogger(__name__)

_FACTORY_DIR = Path(__file__).resolve().parent.parent.parent
MODEL_DIR = _FACTORY_DIR / 'data' / 'crypto' / 'models_4h'


class EnsemblePredictor:
    def __init__(self):
        self._lgb = None
        self._xgb = None
        self._nn = None
        self._scaler = None
        self._features = None
        self._loaded = False

    def load(self):
        try:
            import lightgbm as lgb
            self._lgb = lgb.Booster(model_file=str(MODEL_DIR / 'model_4h_reg.lgb'))

            import xgboost as xgb
            self._xgb = xgb.Booster()
            self._xgb.load_model(str(MODEL_DIR / 'model_4h_xgb.json'))

            with open(MODEL_DIR / 'model_4h_nn.pkl', 'rb') as f:
                self._nn = pickle.load(f)

            with open(MODEL_DIR / 'scaler_4h.pkl', 'rb') as f:
                self._scaler = pickle.load(f)

            with open(MODEL_DIR / 'features_4h.json') as f:
                self._features = json.load(f)

            self._loaded = True
            logger.info("Ensemble loaded: LightGBM + XGBoost + NeuralNet")
            return True
        except Exception as e:
            logger.error(f"Ensemble load failed: {e}")
            return False

    def predict(self, features_dict: dict) -> dict:
        """Predict using all 3 models.

        Returns:
            {
                'score': float (ensemble average),
                'lgb': float, 'xgb': float, 'nn': float,
                'agreement': int (how many agree on direction: 0-3),
                'direction': 'LONG' or 'SHORT' or 'SKIP'
            }
        """
        if not self._loaded:
            self.load()

        X = np.array([[features_dict.get(f, 0) for f in self._features]])
        X_s = self._scaler.transform(X)

        # LightGBM
        lgb_score = float(self._lgb.predict(X_s)[0])

        # XGBoost
        import xgboost as xgb
        xgb_score = float(self._xgb.predict(xgb.DMatrix(X_s))[0])

        # Neural Net
        nn_score = float(self._nn.predict(X_s)[0])

        # Ensemble
        avg_score = (lgb_score + xgb_score + nn_score) / 3

        # Agreement: how many models agree on direction
        signs = [1 if s > 0 else -1 for s in [lgb_score, xgb_score, nn_score]]
        agreement = max(signs.count(1), signs.count(-1))

        # Direction
        if agreement == 3:
            direction = 'LONG' if avg_score > 0 else 'SHORT'
        elif agreement == 2:
            majority = 1 if signs.count(1) >= 2 else -1
            direction = 'LONG' if majority > 0 else 'SHORT'
        else:
            direction = 'SKIP'  # no consensus

        return {
            'score': avg_score,
            'lgb': lgb_score,
            'xgb': xgb_score,
            'nn': nn_score,
            'agreement': agreement,
            'direction': direction,
        }
