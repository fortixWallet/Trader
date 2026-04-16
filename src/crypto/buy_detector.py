"""
BUY Detector — Dedicated model for detecting BUY opportunities
================================================================
The composite forecast model is structurally biased against BUY signals
(avg composite for actual UP moves = 0.009, threshold = 0.06 → misses 91%).

This module uses a fundamentally different approach:
1. Logistic classifier trained on "did price go UP >3% in 7 days?"
2. Contrarian signals (extreme fear = buying opportunity)
3. Relative scoring (coin outperforming its group = relative BUY)
4. Volume confirmation (actual buying pressure required)

Integrated into forecast_engine.py as an additional signal source.

Usage:
    from src.crypto.buy_detector import detect_buy_opportunity
    result = detect_buy_opportunity(conn, coin, category_scores, tech_data)
"""

import json
import logging
import sqlite3
import numpy as np
from pathlib import Path
from datetime import datetime

log = logging.getLogger('buy_detector')

FACTORY_DIR = Path(__file__).resolve().parent.parent.parent
PATTERNS_DB = FACTORY_DIR / 'data' / 'crypto' / 'patterns.db'
MODEL_PATH = FACTORY_DIR / 'data' / 'crypto' / 'buy_model.json'

# Features for BUY classification
FEATURE_NAMES = [
    'rsi', 'fear_greed', 'funding_rate', 'volatility',
    'btc_7d_change', 'ma200_below',  # 6 base
    'composite_score',  # From forecast engine
    'fg_extreme_fear',  # F&G < 20 (contrarian)
    'rsi_oversold',     # RSI < 35 (oversold)
    'funding_negative', # Funding < 0 (shorts paying = potential squeeze)
    # NOTE: volume_surge removed — always 0 in training data (not in patterns.db)
    # which causes train/test skew. Re-add once training pipeline stores taker_volume.
]


class BuyDetector:
    """Logistic regression classifier for BUY opportunities."""

    def __init__(self):
        self.weights = None
        self.bias = 0.0
        self.normalizer = None
        self._load_model()

    def _load_model(self):
        """Load trained model from JSON."""
        if MODEL_PATH.exists():
            try:
                data = json.loads(MODEL_PATH.read_text())
                self.weights = np.array(data['weights'])
                self.bias = data['bias']
                self.normalizer = data.get('normalizer', {})
                log.info(f"BUY detector loaded: {len(self.weights)} features")
            except Exception as e:
                log.warning(f"BUY detector load failed: {e}")
                self.weights = None

    def _extract_features(self, conn, coin: str, composite_score: float,
                          category_scores: dict, tech_data: dict) -> np.ndarray:
        """Extract features for BUY classification."""
        # Base features
        rsi = tech_data.get('signals', {}).get('rsi', {}).get('value', 50)
        price = tech_data.get('price', 0)

        # Fear & Greed
        try:
            fg = conn.execute("SELECT value FROM fear_greed ORDER BY date DESC LIMIT 1").fetchone()
            fg_val = fg[0] if fg else 50
        except Exception:
            fg_val = 50

        # Funding rate
        try:
            fund = conn.execute(
                "SELECT rate FROM funding_rates WHERE coin=? ORDER BY timestamp DESC LIMIT 1",
                (coin,)).fetchone()
            fund_val = fund[0] if fund else 0
        except Exception:
            fund_val = 0

        # Volatility (std of daily returns)
        try:
            prices = conn.execute(
                "SELECT close FROM prices WHERE coin=? AND timeframe='1d' "
                "ORDER BY timestamp DESC LIMIT 15", (coin,)).fetchall()
            closes = [p[0] for p in prices if p[0]]
            if len(closes) >= 7:
                rets = [(c1 - c2) / c2 for c1, c2 in zip(closes[:-1], closes[1:]) if c2]
                vol = float(np.std(rets)) if rets else 0.04
            else:
                vol = 0.04
        except Exception:
            vol = 0.04

        # BTC 7d change
        try:
            btc_p = conn.execute(
                "SELECT close FROM prices WHERE coin='BTC' AND timeframe='1d' "
                "ORDER BY timestamp DESC LIMIT 8").fetchall()
            if len(btc_p) >= 7 and btc_p[-1][0]:
                btc_7d = (btc_p[0][0] - btc_p[-1][0]) / btc_p[-1][0] * 100
            else:
                btc_7d = 0
        except Exception:
            btc_7d = 0

        # MA200 position
        ma200_below = 1.0 if tech_data.get('ma200_trend') == 'below' else 0.0

        # Contrarian indicators
        fg_extreme_fear = 1.0 if fg_val < 20 else 0.0
        rsi_oversold = 1.0 if rsi < 35 else 0.0
        funding_negative = 1.0 if fund_val < -0.001 else 0.0

        features = np.array([
            rsi, fg_val, fund_val, vol, btc_7d, ma200_below,
            composite_score,
            fg_extreme_fear, rsi_oversold, funding_negative,
        ])

        return features

    def _normalize(self, features: np.ndarray) -> np.ndarray:
        """Z-score normalize features."""
        if not self.normalizer:
            return features
        means = np.array(self.normalizer.get('means', np.zeros(len(features))))
        stds = np.array(self.normalizer.get('stds', np.ones(len(features))))
        stds = np.where(stds < 1e-8, 1.0, stds)
        return (features - means) / stds

    def predict_buy_probability(self, conn, coin: str, composite_score: float,
                                 category_scores: dict, tech_data: dict) -> float:
        """Predict probability of price going UP >3% in 7 days.

        Returns float 0.0-1.0. If no trained model, uses rule-based fallback.
        """
        features = self._extract_features(conn, coin, composite_score, category_scores, tech_data)

        if self.weights is not None and len(self.weights) == len(features):
            # Trained logistic regression
            norm_features = self._normalize(features)
            z = np.dot(self.weights, norm_features) + self.bias
            prob = 1.0 / (1.0 + np.exp(-np.clip(z, -10, 10)))
            return float(prob)

        # Rule-based fallback (contrarian + momentum)
        return self._rule_based_score(features)

    def _rule_based_score(self, features: np.ndarray) -> float:
        """Rule-based BUY probability when no trained model exists.

        Based on analysis of 50K predictions:
        - Extreme fear (F&G < 20) + RSI < 35 → historically 65% chance of 3%+ up in 7d
        - Negative funding → short squeeze signal
        - BTC 7d change < -10% → contrarian bounce opportunity
        """
        rsi, fg, funding, vol, btc_7d, ma200_below = features[:6]
        # volume_surge removed — was always 0 in training data (not in patterns.db),
        # so the signal carried no information. Re-add once taker_volume is stored.
        composite, fg_fear, rsi_os, fund_neg = features[6:]

        score = 0.5  # Base: 50% (random)

        # Contrarian fear signal (strongest predictor from data analysis)
        # F&G < 20 + RSI < 35 → 65% accuracy historically
        if fg_fear and rsi_os:
            score += 0.15
        elif fg_fear:
            score += 0.08
        elif rsi_os:
            score += 0.05

        # Funding negative = shorts paying = squeeze potential
        if fund_neg:
            score += 0.05

        # BTC deep dip = bounce likely
        if btc_7d < -10:
            score += 0.08
        elif btc_7d < -5:
            score += 0.04

        # Composite positive = model agrees
        if composite > 0.03:
            score += 0.05
        elif composite < -0.05:
            score -= 0.05

        # High volatility = wider moves = more BUY opportunities
        if vol > 0.05:
            score += 0.03

        return float(np.clip(score, 0.1, 0.9))

    def train(self):
        """Train logistic regression on historical data from patterns.db."""
        if not PATTERNS_DB.exists():
            log.warning("patterns.db not found, cannot train BUY detector")
            return

        conn = sqlite3.connect(str(PATTERNS_DB))
        c = conn.cursor()

        c.execute('''SELECT composite_score, rsi_at_forecast, fg_at_forecast,
            funding_rate_at_forecast, volatility_at_forecast, btc_change_7d,
            ma200_trend, actual_change_pct
            FROM training_results
            WHERE actual_change_pct IS NOT NULL
            AND rsi_at_forecast IS NOT NULL
            AND fg_at_forecast IS NOT NULL''')
        rows = c.fetchall()
        conn.close()

        if len(rows) < 100:
            log.warning(f"Not enough training data ({len(rows)} rows, need 100+)")
            return

        # Build feature matrix
        X = []
        y = []
        for row in rows:
            composite, rsi, fg, funding, vol, btc_7d, ma200, actual_change = row
            if rsi is None or fg is None:
                continue

            funding = funding or 0
            vol = vol or 0.04
            btc_7d = btc_7d or 0
            ma200_below = 1.0 if ma200 == 'below' else 0.0

            features = [
                rsi, fg, funding, vol, btc_7d, ma200_below,
                composite or 0,
                1.0 if fg < 20 else 0.0,       # fg_extreme_fear
                1.0 if rsi < 35 else 0.0,       # rsi_oversold
                1.0 if (funding or 0) < -0.001 else 0.0,  # funding_negative
            ]
            X.append(features)
            y.append(1.0 if (actual_change or 0) > 3.0 else 0.0)

        X = np.array(X)
        y = np.array(y)

        log.info(f"Training BUY detector: {len(X)} samples, {sum(y):.0f} positive ({100*sum(y)/len(y):.1f}%)")

        # Z-score normalize
        means = X.mean(axis=0)
        stds = X.std(axis=0)
        stds = np.where(stds < 1e-8, 1.0, stds)
        X_norm = (X - means) / stds

        # Walk-forward split (70/30)
        split = int(len(X) * 0.7)
        X_train, X_test = X_norm[:split], X_norm[split:]
        y_train, y_test = y[:split], y[split:]

        # Logistic regression via gradient descent
        n_features = X_train.shape[1]
        weights = np.zeros(n_features)
        bias = 0.0
        lr = 0.01
        n_iter = 2000

        for i in range(n_iter):
            z = X_train @ weights + bias
            pred = 1.0 / (1.0 + np.exp(-np.clip(z, -10, 10)))
            error = pred - y_train

            # L2 regularization
            grad_w = (X_train.T @ error) / len(y_train) + 0.01 * weights
            grad_b = error.mean()

            weights -= lr * grad_w
            bias -= lr * grad_b

        # Evaluate on test set
        z_test = X_test @ weights + bias
        pred_test = 1.0 / (1.0 + np.exp(-np.clip(z_test, -10, 10)))
        pred_binary = (pred_test > 0.5).astype(float)

        accuracy = (pred_binary == y_test).mean()
        true_positives = ((pred_binary == 1) & (y_test == 1)).sum()
        predicted_positives = (pred_binary == 1).sum()
        actual_positives = (y_test == 1).sum()

        precision = true_positives / max(predicted_positives, 1)
        recall = true_positives / max(actual_positives, 1)

        log.info(f"BUY Detector trained:")
        log.info(f"  Test accuracy: {100*accuracy:.1f}%")
        log.info(f"  Precision: {100*precision:.1f}% (of predicted BUYs, how many correct)")
        log.info(f"  Recall: {100*recall:.1f}% (of actual UPs, how many caught)")
        log.info(f"  Predicted BUYs: {predicted_positives:.0f}/{len(y_test)} ({100*predicted_positives/len(y_test):.1f}%)")

        # Feature importance
        importance = np.abs(weights)
        sorted_idx = np.argsort(importance)[::-1]
        log.info("  Feature importance:")
        for idx in sorted_idx[:6]:
            log.info(f"    {FEATURE_NAMES[idx]:20s}: {weights[idx]:+.4f} (|{importance[idx]:.4f}|)")

        # Save model
        model = {
            'weights': weights.tolist(),
            'bias': float(bias),
            'normalizer': {
                'means': means.tolist(),
                'stds': stds.tolist(),
            },
            'metrics': {
                'accuracy': float(accuracy),
                'precision': float(precision),
                'recall': float(recall),
                'n_train': len(y_train),
                'n_test': len(y_test),
                'positive_rate': float(sum(y) / len(y)),
            },
            'feature_names': FEATURE_NAMES,
            'trained_at': datetime.utcnow().isoformat(),
        }

        MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
        MODEL_PATH.write_text(json.dumps(model, indent=2))
        log.info(f"  Model saved to {MODEL_PATH}")

        self.weights = np.array(model['weights'])
        self.bias = model['bias']
        self.normalizer = model['normalizer']

        return model['metrics']


# Singleton instance
_detector = None


def get_detector() -> BuyDetector:
    """Get or create singleton BUY detector."""
    global _detector
    if _detector is None:
        _detector = BuyDetector()
    return _detector


def detect_buy_opportunity(conn, coin: str, composite_score: float,
                           category_scores: dict, tech_data: dict) -> dict:
    """Main entry point: detect if this coin is a BUY opportunity.

    Returns dict with:
        - probability: float 0-1 (>0.60 = BUY signal)
        - is_buy: bool
        - reason: str (why BUY or why not)
        - confidence: int 1-10
    """
    detector = get_detector()
    prob = detector.predict_buy_probability(conn, coin, composite_score, category_scores, tech_data)

    is_buy = prob >= 0.60
    if prob >= 0.75:
        confidence = 8
        reason = "Strong BUY: multiple contrarian + momentum signals align"
    elif prob >= 0.65:
        confidence = 7
        reason = "BUY: fear + oversold conditions with buying pressure"
    elif prob >= 0.60:
        confidence = 6
        reason = "Mild BUY: contrarian indicators suggest upside potential"
    elif prob >= 0.45:
        confidence = 4
        reason = "Neutral: mixed signals, no clear directional edge"
    else:
        confidence = 3
        reason = "No BUY: conditions don't favor upside"

    return {
        'probability': round(prob, 3),
        'is_buy': is_buy,
        'reason': reason,
        'confidence': confidence,
    }


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

    import sys
    if '--train' in sys.argv:
        detector = BuyDetector()
        metrics = detector.train()
        if metrics:
            print(f"\nAccuracy: {100*metrics['accuracy']:.1f}%")
            print(f"Precision: {100*metrics['precision']:.1f}%")
            print(f"Recall: {100*metrics['recall']:.1f}%")
    else:
        print("Usage: python -m src.crypto.buy_detector --train")
