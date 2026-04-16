"""
FORTIX — Pattern Learner
================================
Extracts actionable patterns from training results and stores them
in patterns.db for integration into forecast_engine.

Algorithm:
  1. Load training results
  2. Bin features (RSI, F&G, funding, MA200, volatility)
  3. For each 2-feature combination with >= 7 samples → compute stats
  4. For each 3-feature combination with >= 7 samples → compute stats
  5. Score patterns using Wilson confidence interval
  6. Store in learned_patterns table

Usage:
    from src.crypto.pattern_learner import PatternLearner
    learner = PatternLearner('data/crypto/patterns.db')
    patterns = learner.extract_patterns(run_id)
    matches = learner.match_current_conditions({'rsi': 28, 'fg': 12, ...})
"""

import sqlite3
import hashlib
import logging
import json
import numpy as np
from pathlib import Path
from datetime import datetime, timezone
from itertools import combinations
from collections import defaultdict

log = logging.getLogger('pattern_learner')


class PatternLearner:
    """Extract and store learned patterns from training results."""

    MIN_SAMPLES = 20   # Was 7 — too low, many noise patterns with <15 samples
    MIN_CONFIDENCE = 0.15  # Was 0.05 — filter out coin-flip patterns

    # Feature bins for pattern discovery
    FEATURE_BINS = {
        'rsi': [
            (0, 20, 'extremely_oversold'),
            (20, 30, 'oversold'),
            (30, 45, 'mild_bear'),
            (45, 55, 'neutral'),
            (55, 70, 'mild_bull'),
            (70, 80, 'overbought'),
            (80, 100, 'extremely_overbought'),
        ],
        'fg': [
            (0, 10, 'extreme_fear'),
            (10, 25, 'fear'),
            (25, 45, 'mild_fear'),
            (45, 55, 'neutral'),
            (55, 75, 'mild_greed'),
            (75, 90, 'greed'),
            (90, 100, 'extreme_greed'),
        ],
        'funding_rate': [
            (-1, -0.0005, 'negative'),
            (-0.0005, 0.0005, 'neutral'),
            (0.0005, 0.002, 'elevated'),
            (0.002, 1, 'high'),
        ],
        'ma200_trend': ['above', 'below'],  # Categorical, not numeric
        'volatility': [
            (0, 0.015, 'low'),
            (0.015, 0.035, 'medium'),
            (0.035, 0.06, 'high'),
            (0.06, 1, 'extreme'),
        ],
    }

    # Map DB columns to feature names
    FEATURE_COLUMNS = {
        'rsi': 'rsi_at_forecast',
        'fg': 'fg_at_forecast',
        'funding_rate': 'funding_rate_at_forecast',
        'ma200_trend': 'ma200_trend',
        'volatility': 'volatility_at_forecast',
    }

    def __init__(self, db_path: str = 'data/crypto/patterns.db'):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._init_tables()

    def _init_tables(self):
        """Create pattern storage tables."""
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS learned_patterns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pattern_id TEXT UNIQUE NOT NULL,
                conditions_json TEXT NOT NULL,
                description TEXT NOT NULL,
                sample_size INTEGER NOT NULL,
                direction_accuracy REAL NOT NULL,
                avg_actual_change REAL NOT NULL,
                median_actual_change REAL NOT NULL,
                positive_pct REAL NOT NULL,
                avg_magnitude REAL NOT NULL,
                confidence_score REAL NOT NULL,
                pattern_score REAL NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_run_id TEXT,
                n_conditions INTEGER,
                active INTEGER DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS pattern_observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pattern_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                coin TEXT NOT NULL,
                forecast_date TEXT NOT NULL,
                actual_change_pct REAL NOT NULL,
                direction_correct INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_patterns_active
                ON learned_patterns(active, confidence_score DESC);
            CREATE INDEX IF NOT EXISTS idx_obs_pattern
                ON pattern_observations(pattern_id);
        """)
        self.conn.commit()

    def _bin_feature(self, feature_name: str, value) -> str:
        """Assign a feature value to its bin. Returns bin label or None."""
        if value is None:
            return None

        bins = self.FEATURE_BINS.get(feature_name)
        if bins is None:
            return None

        # Categorical features (list of strings)
        if isinstance(bins[0], str):
            return value if value in bins else None

        # Numeric features (list of tuples)
        for low, high, label in bins:
            if low <= value < high:
                return label
        return None

    def _pattern_id(self, conditions: dict) -> str:
        """Generate unique hash for a condition set."""
        key = json.dumps(conditions, sort_keys=True)
        return hashlib.md5(key.encode()).hexdigest()[:12]

    def _compute_confidence(self, n: int, accuracy: float,
                            baseline: float = 0.5) -> float:
        """Wilson score lower bound adjusted by deviation from baseline."""
        if n < 2:
            return 0.0

        z = 1.96  # 95% CI

        # Wilson score lower bound
        denom = 1 + z**2 / n
        center = (accuracy + z**2 / (2 * n)) / denom
        spread = z * np.sqrt(
            (accuracy * (1 - accuracy) + z**2 / (4 * n)) / n
        ) / denom
        lower = center - spread

        # Weight by deviation from baseline
        deviation = abs(accuracy - baseline)
        confidence = lower * min(deviation * 3, 1.0)

        return max(0.0, float(confidence))

    def _compute_pattern_score(self, changes: list) -> float:
        """Compute forecast score suggestion from outcome distribution.

        Returns float in [-1, +1].
        """
        if not changes:
            return 0.0

        positive_pct = sum(1 for c in changes if c > 0) / len(changes)
        avg_change = np.mean(changes)

        # Direction: positive_pct maps to [-1, +1]
        direction = (positive_pct - 0.5) * 2  # 0.75 → +0.5, 0.25 → -0.5

        # Scale by magnitude
        magnitude = min(abs(avg_change) / 5, 1.0)  # 5% → 1.0 scale

        score = direction * max(magnitude, 0.3)  # min scale 0.3

        return float(np.clip(score, -0.8, 0.8))

    def extract_patterns(self, run_id: str) -> list:
        """Discover patterns from training results.

        Returns list of discovered pattern dicts.
        """
        log.info(f"Extracting patterns from run {run_id}...")

        # Load all training results
        rows = self.conn.execute(
            "SELECT * FROM training_results WHERE run_id = ?", (run_id,)
        ).fetchall()

        if not rows:
            log.warning("  No training results found")
            return []

        results = [dict(r) for r in rows]
        log.info(f"  Loaded {len(results)} results")

        # Bin all features for each result
        binned_results = []
        for r in results:
            binned = {}
            for feat, col in self.FEATURE_COLUMNS.items():
                binned[feat] = self._bin_feature(feat, r.get(col))
            binned['_result'] = r  # Keep original for stats
            binned_results.append(binned)

        # Find patterns: 2-feature combinations
        features = list(self.FEATURE_BINS.keys())
        discovered = []

        # 2-feature patterns
        for f1, f2 in combinations(features, 2):
            groups = defaultdict(list)
            for br in binned_results:
                b1 = br.get(f1)
                b2 = br.get(f2)
                if b1 is not None and b2 is not None:
                    groups[(b1, b2)].append(br['_result'])

            for (v1, v2), obs in groups.items():
                if len(obs) < self.MIN_SAMPLES:
                    continue

                pattern = self._create_pattern(
                    conditions={f1: v1, f2: v2},
                    observations=obs,
                    run_id=run_id,
                )
                if pattern and pattern['confidence_score'] >= self.MIN_CONFIDENCE:
                    discovered.append(pattern)

        # 3-feature patterns (only if sample size allows)
        for f1, f2, f3 in combinations(features, 3):
            groups = defaultdict(list)
            for br in binned_results:
                b1 = br.get(f1)
                b2 = br.get(f2)
                b3 = br.get(f3)
                if b1 is not None and b2 is not None and b3 is not None:
                    groups[(b1, b2, b3)].append(br['_result'])

            for (v1, v2, v3), obs in groups.items():
                if len(obs) < self.MIN_SAMPLES:
                    continue

                pattern = self._create_pattern(
                    conditions={f1: v1, f2: v2, f3: v3},
                    observations=obs,
                    run_id=run_id,
                )
                if pattern and pattern['confidence_score'] >= self.MIN_CONFIDENCE:
                    discovered.append(pattern)

        # Sort by confidence (descending)
        discovered.sort(key=lambda p: p['confidence_score'], reverse=True)

        # Save to DB
        self._save_patterns(discovered, run_id)

        log.info(f"  Discovered {len(discovered)} patterns "
                 f"(2-feat: {sum(1 for p in discovered if p['n_conditions']==2)}, "
                 f"3-feat: {sum(1 for p in discovered if p['n_conditions']==3)})")

        return discovered

    def _create_pattern(self, conditions: dict, observations: list,
                        run_id: str) -> dict:
        """Create a pattern dict from conditions and observations."""
        changes = [o['actual_change_pct'] for o in observations]
        directions = [o.get('direction_correct') for o in observations]
        dir_valid = [d for d in directions if d is not None]

        if not changes or not dir_valid:
            return None

        n = len(observations)
        direction_acc = sum(dir_valid) / len(dir_valid) if dir_valid else 0.5
        positive_pct = sum(1 for c in changes if c > 0) / len(changes)
        avg_change = float(np.mean(changes))
        median_change = float(np.median(changes))
        avg_mag = float(np.mean([abs(c) for c in changes]))

        confidence = self._compute_confidence(len(dir_valid), direction_acc)
        pattern_score = self._compute_pattern_score(changes)

        # Build description
        parts = []
        for feat, val in sorted(conditions.items()):
            parts.append(f"{feat}={val}")
        description = ' + '.join(parts)

        pid = self._pattern_id(conditions)

        return {
            'pattern_id': pid,
            'conditions': conditions,
            'conditions_json': json.dumps(conditions, sort_keys=True),
            'description': description,
            'sample_size': n,
            'direction_accuracy': direction_acc,
            'avg_actual_change': avg_change,
            'median_actual_change': median_change,
            'positive_pct': positive_pct,
            'avg_magnitude': avg_mag,
            'confidence_score': confidence,
            'pattern_score': pattern_score,
            'n_conditions': len(conditions),
            'run_id': run_id,
            'observations': observations,
        }

    def _save_patterns(self, patterns: list, run_id: str):
        """Save patterns and observations to DB."""
        now = datetime.now(timezone.utc).isoformat()

        for p in patterns:
            self.conn.execute(
                "INSERT OR REPLACE INTO learned_patterns "
                "(pattern_id, conditions_json, description, sample_size, "
                "direction_accuracy, avg_actual_change, median_actual_change, "
                "positive_pct, avg_magnitude, confidence_score, pattern_score, "
                "created_at, updated_at, last_run_id, n_conditions, active) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)",
                (
                    p['pattern_id'], p['conditions_json'], p['description'],
                    p['sample_size'], p['direction_accuracy'],
                    p['avg_actual_change'], p['median_actual_change'],
                    p['positive_pct'], p['avg_magnitude'],
                    p['confidence_score'], p['pattern_score'],
                    now, now, run_id, p['n_conditions'],
                )
            )

            # Save individual observations
            for obs in p.get('observations', []):
                self.conn.execute(
                    "INSERT INTO pattern_observations "
                    "(pattern_id, run_id, coin, forecast_date, actual_change_pct, "
                    "direction_correct) VALUES (?,?,?,?,?,?)",
                    (
                        p['pattern_id'], run_id, obs['coin'],
                        obs['forecast_date'], obs['actual_change_pct'],
                        obs.get('direction_correct', 0) or 0,
                    )
                )

        self.conn.commit()
        log.info(f"  Saved {len(patterns)} patterns to DB")

    def get_active_patterns(self, min_confidence: float = 0.05,
                           min_samples: int = 7) -> list:
        """Retrieve all active patterns meeting thresholds."""
        rows = self.conn.execute(
            "SELECT * FROM learned_patterns WHERE active = 1 "
            "AND confidence_score >= ? AND sample_size >= ? "
            "ORDER BY confidence_score DESC",
            (min_confidence, min_samples)
        ).fetchall()
        return [dict(r) for r in rows]

    def match_current_conditions(self, conditions: dict) -> list:
        """Find patterns that match current market conditions.

        Args:
            conditions: raw values {'rsi': 28, 'fg': 12, 'funding_rate': -0.002,
                        'ma200_trend': 'below', 'volatility': 0.035}

        Returns: List of matching patterns with their scores.
        """
        # Bin the current conditions
        binned = {}
        for feat, value in conditions.items():
            b = self._bin_feature(feat, value)
            if b is not None:
                binned[feat] = b

        if not binned:
            return []

        # Load active patterns
        active = self.get_active_patterns()

        matches = []
        for pattern in active:
            conds = json.loads(pattern['conditions_json'])

            # Check if all pattern conditions match
            all_match = True
            for feat, required_val in conds.items():
                if feat not in binned or binned[feat] != required_val:
                    all_match = False
                    break

            if all_match:
                matches.append(pattern)

        return matches

    def print_patterns(self, patterns: list, top_n: int = 30):
        """Print discovered patterns."""
        log.info(f"\n{'='*60}")
        log.info(f"DISCOVERED PATTERNS ({len(patterns)} total)")
        log.info(f"{'='*60}")

        if not patterns:
            log.info("  No patterns found")
            return

        log.info(f"\n  {'#':>3} {'Conditions':<45} {'N':>4} {'DirAcc':>7} "
                 f"{'AvgChg':>7} {'Conf':>6} {'Score':>6}")
        log.info(f"  {'-'*82}")

        for i, p in enumerate(patterns[:top_n]):
            log.info(f"  {i+1:>3} {p['description']:<45} {p['sample_size']:>4} "
                     f"{p['direction_accuracy']*100:>6.1f}% "
                     f"{p['avg_actual_change']:>+6.1f}% "
                     f"{p['confidence_score']:>5.3f} "
                     f"{p['pattern_score']:>+5.2f}")

        # Summary stats
        if patterns:
            avg_acc = np.mean([p['direction_accuracy'] for p in patterns])
            avg_conf = np.mean([p['confidence_score'] for p in patterns])
            bullish = sum(1 for p in patterns if p['pattern_score'] > 0.1)
            bearish = sum(1 for p in patterns if p['pattern_score'] < -0.1)
            neutral = len(patterns) - bullish - bearish

            log.info(f"\n  Summary:")
            log.info(f"    Avg direction accuracy: {avg_acc*100:.1f}%")
            log.info(f"    Avg confidence:         {avg_conf:.3f}")
            log.info(f"    Bullish / Neutral / Bearish: {bullish} / {neutral} / {bearish}")

    def close(self):
        self.conn.close()
