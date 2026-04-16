"""
FORTIX — ML Corrector (Level 6)
======================================
Non-linear Random Forest correction on top of the Elastic Net (L2+L4).
Uses the same 12 features but captures non-linear patterns:
- Conditional splits (RSI<30 AND funding<0 → different from RSI<30 AND funding>0)
- Threshold effects (volume spike > 2x triggers different behavior)
- Complex interactions that polynomial terms can't capture

Pure numpy implementation — no sklearn dependency.

Usage:
    python src/crypto/ml_corrector.py              # train all groups
    python src/crypto/ml_corrector.py <run_id>     # train on specific run
"""

import sys
import json
import sqlite3
import logging
import numpy as np
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger('ml_corrector')

PATTERNS_DB = Path('data/crypto/patterns.db')
MODELS_DIR = Path('data/crypto/forest_models')

from src.crypto.regression_model import (
    FEATURE_NAMES, COIN_GROUPS, extract_features_from_row,
    extract_features_live, _get_coin_group,
)


# ═══════════════════════════════════════════════════════════════
# DECISION TREE (numpy only)
# ═══════════════════════════════════════════════════════════════

class SimpleTree:
    """Binary decision tree for regression. Pure numpy, no sklearn.

    Splits on feature thresholds to minimize variance (MSE) of targets.
    Regularized via max_depth and min_samples_leaf.
    """

    def __init__(self, max_depth=4, min_samples_leaf=20):
        self.max_depth = max_depth
        self.min_samples_leaf = min_samples_leaf
        self.tree = None

    def fit(self, X, y):
        self.tree = self._build(X, y, depth=0)
        return self

    def _build(self, X, y, depth):
        n = len(y)
        if depth >= self.max_depth or n < 2 * self.min_samples_leaf:
            return {'leaf': True, 'value': float(np.mean(y)), 'n': n}

        best_feature = None
        best_threshold = None
        best_gain = 0.0  # must improve over no split
        parent_var = np.var(y)

        for j in range(X.shape[1]):
            col = X[:, j]
            # Use percentile thresholds for efficiency (not every unique value)
            thresholds = np.unique(np.percentile(col, [20, 35, 50, 65, 80]))

            for t in thresholds:
                left_mask = col <= t
                right_mask = ~left_mask
                n_left = left_mask.sum()
                n_right = right_mask.sum()

                if n_left < self.min_samples_leaf or n_right < self.min_samples_leaf:
                    continue

                # Variance reduction (weighted)
                var_left = np.var(y[left_mask])
                var_right = np.var(y[right_mask])
                weighted_var = (n_left * var_left + n_right * var_right) / n
                gain = parent_var - weighted_var

                if gain > best_gain:
                    best_gain = gain
                    best_feature = j
                    best_threshold = float(t)

        if best_feature is None:
            return {'leaf': True, 'value': float(np.mean(y)), 'n': n}

        mask = X[:, best_feature] <= best_threshold
        return {
            'leaf': False,
            'feature': best_feature,
            'threshold': best_threshold,
            'left': self._build(X[mask], y[mask], depth + 1),
            'right': self._build(X[~mask], y[~mask], depth + 1),
        }

    def predict_one(self, x):
        node = self.tree
        while not node['leaf']:
            if x[node['feature']] <= node['threshold']:
                node = node['left']
            else:
                node = node['right']
        return node['value']

    def predict(self, X):
        return np.array([self.predict_one(x) for x in X])

    def to_dict(self):
        return self.tree

    def from_dict(self, d):
        self.tree = d
        return self


# ═══════════════════════════════════════════════════════════════
# RANDOM FOREST
# ═══════════════════════════════════════════════════════════════

class SimpleForest:
    """Random Forest for regression. Ensemble of decision trees.

    Each tree trained on:
    - Bootstrap sample (random 80% of rows with replacement)
    - Random feature subset (random 70% of features)

    Predictions averaged across all trees (reduces variance/overfitting).
    """

    def __init__(self, n_trees=30, max_depth=4, min_samples_leaf=20,
                 sample_ratio=0.8, feature_ratio=0.7, seed=42):
        self.n_trees = n_trees
        self.max_depth = max_depth
        self.min_samples_leaf = min_samples_leaf
        self.sample_ratio = sample_ratio
        self.feature_ratio = feature_ratio
        self.seed = seed
        self.trees = []  # list of (tree, feature_indices)

    def fit(self, X, y):
        rng = np.random.RandomState(self.seed)
        n_samples = len(y)
        n_features = X.shape[1]
        n_select_samples = int(n_samples * self.sample_ratio)
        n_select_features = max(3, int(n_features * self.feature_ratio))

        self.trees = []
        for i in range(self.n_trees):
            # Bootstrap sample (with replacement)
            sample_idx = rng.choice(n_samples, size=n_select_samples, replace=True)
            # Random feature subset (without replacement)
            feat_idx = np.sort(rng.choice(n_features, size=n_select_features, replace=False))

            X_sub = X[sample_idx][:, feat_idx]
            y_sub = y[sample_idx]

            tree = SimpleTree(max_depth=self.max_depth,
                              min_samples_leaf=self.min_samples_leaf)
            tree.fit(X_sub, y_sub)

            self.trees.append((tree, feat_idx.tolist()))

        return self

    def predict(self, X):
        preds = []
        for tree, feat_idx in self.trees:
            X_sub = X[:, feat_idx]
            preds.append(tree.predict(X_sub))
        return np.mean(preds, axis=0)

    def predict_one(self, x):
        preds = []
        for tree, feat_idx in self.trees:
            preds.append(tree.predict_one(x[feat_idx]))
        return float(np.mean(preds))

    def to_dict(self):
        return {
            'n_trees': self.n_trees,
            'max_depth': self.max_depth,
            'min_samples_leaf': self.min_samples_leaf,
            'trees': [(tree.to_dict(), feat_idx) for tree, feat_idx in self.trees],
        }

    def from_dict(self, d):
        self.n_trees = d['n_trees']
        self.max_depth = d['max_depth']
        self.min_samples_leaf = d['min_samples_leaf']
        self.trees = []
        for tree_dict, feat_idx in d['trees']:
            tree = SimpleTree(max_depth=self.max_depth,
                              min_samples_leaf=self.min_samples_leaf)
            tree.from_dict(tree_dict)
            self.trees.append((tree, feat_idx))
        return self


# ═══════════════════════════════════════════════════════════════
# GRADIENT BOOSTED TREES
# ═══════════════════════════════════════════════════════════════

class GradientBoostedTrees:
    """Gradient Boosted Trees for regression. Sequential residual fitting.

    Each tree fits the residuals (errors) of the current ensemble.
    Small learning rate + many shallow trees = strong regularization.
    Much better than Random Forest for small, noisy datasets.

    Parameters:
    - n_trees: number of boosting rounds
    - max_depth: depth of each individual tree (keep small: 2-3)
    - learning_rate: shrinkage factor per tree (0.05-0.2)
    - min_samples_leaf: minimum samples per leaf
    - subsample: fraction of data for each tree (stochastic GB)
    """

    def __init__(self, n_trees=50, max_depth=2, learning_rate=0.1,
                 min_samples_leaf=20, subsample=0.8, seed=42):
        self.n_trees = n_trees
        self.max_depth = max_depth
        self.learning_rate = learning_rate
        self.min_samples_leaf = min_samples_leaf
        self.subsample = subsample
        self.seed = seed
        self.base_prediction = 0.0
        self.trees = []

    def fit(self, X, y):
        rng = np.random.RandomState(self.seed)
        n = len(y)
        n_sub = max(int(n * self.subsample), 30)

        # Base prediction: mean of targets
        self.base_prediction = float(np.mean(y))
        current_pred = np.full(n, self.base_prediction)

        self.trees = []
        for i in range(self.n_trees):
            # Compute residuals (negative gradient for MSE loss)
            residuals = y - current_pred

            # Stochastic: use subsample of data
            if self.subsample < 1.0:
                idx = rng.choice(n, size=n_sub, replace=False)
            else:
                idx = np.arange(n)

            # Fit tree on residuals
            tree = SimpleTree(max_depth=self.max_depth,
                              min_samples_leaf=self.min_samples_leaf)
            tree.fit(X[idx], residuals[idx])

            # Update predictions with learning rate
            tree_pred = tree.predict(X)
            current_pred += self.learning_rate * tree_pred

            self.trees.append(tree)

        return self

    def predict(self, X):
        pred = np.full(len(X), self.base_prediction)
        for tree in self.trees:
            pred += self.learning_rate * tree.predict(X)
        return pred

    def predict_one(self, x):
        pred = self.base_prediction
        for tree in self.trees:
            pred += self.learning_rate * tree.predict_one(x)
        return float(pred)

    def to_dict(self):
        return {
            'model_type': 'gbt',
            'n_trees': self.n_trees,
            'max_depth': self.max_depth,
            'learning_rate': self.learning_rate,
            'min_samples_leaf': self.min_samples_leaf,
            'subsample': self.subsample,
            'base_prediction': self.base_prediction,
            'trees': [tree.to_dict() for tree in self.trees],
        }

    def from_dict(self, d):
        self.n_trees = d['n_trees']
        self.max_depth = d['max_depth']
        self.learning_rate = d['learning_rate']
        self.min_samples_leaf = d['min_samples_leaf']
        self.subsample = d.get('subsample', 0.8)
        self.base_prediction = d['base_prediction']
        self.trees = []
        for tree_dict in d['trees']:
            tree = SimpleTree(max_depth=self.max_depth,
                              min_samples_leaf=self.min_samples_leaf)
            tree.from_dict(tree_dict)
            self.trees.append(tree)
        return self


# ═══════════════════════════════════════════════════════════════
# HYPERPARAMETER TUNING
# ═══════════════════════════════════════════════════════════════

FOREST_CONFIGS = [
    # (n_trees, max_depth, min_samples_leaf)
    (20, 3, 30),   # shallow, conservative
    (30, 4, 20),   # medium depth
    (30, 4, 30),   # medium, larger leaves
    (50, 3, 15),   # many shallow trees
]

GBT_CONFIGS = [
    # (n_trees, max_depth, learning_rate, min_samples_leaf)
    (30, 2, 0.05, 30),   # very conservative
    (50, 2, 0.05, 20),   # conservative, more trees
    (50, 2, 0.1, 30),    # moderate lr
    (50, 3, 0.05, 20),   # slightly deeper
    (80, 2, 0.05, 20),   # many shallow stumps
    (80, 3, 0.03, 30),   # many trees, tiny lr, deeper
    (100, 2, 0.03, 20),  # lots of stumps, tiny lr
]


def _eval_predictions(y_pred, y_true, dates_arr):
    """Evaluate predictions: dir_acc, MAE, R2, rank_corr."""
    dir_acc = float(np.mean(np.sign(y_pred) == np.sign(y_true)))
    mae = float(np.mean(np.abs(y_true - y_pred)))
    ss_res = np.sum((y_true - y_pred) ** 2)
    ss_tot = np.sum((y_true - np.mean(y_true)) ** 2)
    r2 = float(1 - ss_res / ss_tot) if ss_tot > 0 else 0.0

    try:
        from src.crypto.weight_optimizer import spearman_rank_fast
        rank_corrs = []
        for d in np.unique(dates_arr):
            d_mask = dates_arr == d
            if d_mask.sum() >= 3:
                rc = spearman_rank_fast(y_pred[d_mask], y_true[d_mask])
                if not np.isnan(rc):
                    rank_corrs.append(rc)
        rank_corr = float(np.mean(rank_corrs)) if rank_corrs else 0.0
    except Exception:
        rank_corr = 0.0

    return dir_acc, mae, r2, rank_corr


def _tune_best_model_wf(X_train, y_train, dates_train):
    """Tune and select best model (RF vs GBT) via nested walk-forward CV.

    Returns (model_type, best_config, best_score) where:
    - model_type: 'forest' or 'gbt'
    - best_config: tuple of hyperparameters
    - best_score: composite metric on inner validation
    """
    unique_dates = np.sort(np.unique(dates_train))
    inner_split = int(len(unique_dates) * 0.6)
    if inner_split < 5:
        return 'gbt', (50, 2, 0.05, 20), -1

    inner_train_dates = set(unique_dates[:inner_split])
    inner_train_mask = np.array([d in inner_train_dates for d in dates_train])
    inner_val_mask = ~inner_train_mask

    X_itr = X_train[inner_train_mask]
    y_itr = y_train[inner_train_mask]
    X_ival = X_train[inner_val_mask]
    y_ival = y_train[inner_val_mask]
    dates_ival = dates_train[inner_val_mask]

    if len(y_itr) < 50 or len(y_ival) < 20:
        return 'gbt', (50, 2, 0.05, 20), -1

    best_score = -999
    best_type = 'gbt'
    best_config = (50, 2, 0.05, 20)

    # Try Random Forest configs
    for n_trees, max_depth, min_leaf in FOREST_CONFIGS:
        forest = SimpleForest(n_trees=n_trees, max_depth=max_depth,
                              min_samples_leaf=min_leaf)
        forest.fit(X_itr, y_itr)
        y_pred_val = forest.predict(X_ival)
        dir_acc, mae, r2, rank_corr = _eval_predictions(
            y_pred_val, y_ival, dates_ival)
        composite = dir_acc + 0.5 * max(rank_corr, 0.0)
        if composite > best_score:
            best_score = composite
            best_type = 'forest'
            best_config = (n_trees, max_depth, min_leaf)

    # Try GBT configs
    for n_trees, max_depth, lr, min_leaf in GBT_CONFIGS:
        gbt = GradientBoostedTrees(
            n_trees=n_trees, max_depth=max_depth,
            learning_rate=lr, min_samples_leaf=min_leaf)
        gbt.fit(X_itr, y_itr)
        y_pred_val = gbt.predict(X_ival)
        dir_acc, mae, r2, rank_corr = _eval_predictions(
            y_pred_val, y_ival, dates_ival)
        composite = dir_acc + 0.5 * max(rank_corr, 0.0)
        if composite > best_score:
            best_score = composite
            best_type = 'gbt'
            best_config = (n_trees, max_depth, lr, min_leaf)

    return best_type, best_config, best_score


# ═══════════════════════════════════════════════════════════════
# TRAINING
# ═══════════════════════════════════════════════════════════════

def train_all_groups(run_id=None):
    """Train Random Forest for each coin group using walk-forward split.

    Walk-forward: train on first 70% of dates, test on last 30%.
    Targets winsorized to [-20%, 20%].
    Hyperparams auto-tuned via nested inner CV.
    Saves models to data/crypto/forest_models/{group}.json
    """
    conn = sqlite3.connect(str(PATTERNS_DB))
    conn.row_factory = sqlite3.Row

    if run_id is None:
        row = conn.execute(
            "SELECT run_id, COUNT(*) as n FROM training_results "
            "GROUP BY run_id ORDER BY n DESC LIMIT 1"
        ).fetchone()
        if not row:
            log.error("No training results found")
            conn.close()
            return None
        run_id = row['run_id']

    rows = conn.execute(
        "SELECT * FROM training_results WHERE run_id = ? "
        "AND actual_change_pct IS NOT NULL", (run_id,)
    ).fetchall()
    conn.close()

    log.info(f"Training forest models on {len(rows)} samples (run {run_id})")
    log.info(f"  Features: {len(FEATURE_NAMES)} ({', '.join(FEATURE_NAMES[:6])} + poly)")

    # Extract features and targets
    all_features = []
    all_targets = []
    all_coins = []
    all_dates = []

    for r in rows:
        feat = extract_features_from_row(r)
        target = r['actual_change_pct']
        all_features.append(feat)
        all_targets.append(target)
        all_coins.append(r['coin'])
        all_dates.append(r['forecast_date'])

    X_all = np.array(all_features)
    y_all = np.array(all_targets)
    coins_all = np.array(all_coins)
    dates_all = np.array(all_dates)

    MODELS_DIR.mkdir(parents=True, exist_ok=True)

    # Also load Elastic Net results for comparison
    enet_results = {}
    enet_dir = Path('data/crypto/regression_models')
    for g in COIN_GROUPS:
        enet_path = enet_dir / f'{g}.json'
        if enet_path.exists():
            enet_data = json.loads(enet_path.read_text())
            enet_results[g] = {
                'dir_acc': enet_data.get('test_dir_acc', 0),
                'rank': enet_data.get('test_rank_corr', 0),
                'blend': enet_data.get('blend_weight', 0),
            }

    results = {}
    for group, group_coins in COIN_GROUPS.items():
        mask = np.array([c in group_coins for c in coins_all])
        X_group = X_all[mask]
        y_group = y_all[mask]
        dates_group = dates_all[mask]

        n = len(y_group)
        if n < 100:
            log.warning(f"  {group}: too few samples ({n}), skipping")
            continue

        # Walk-forward split: train on first 70% of dates, test on last 30%
        unique_dates = np.sort(np.unique(dates_group))
        split_idx = int(len(unique_dates) * 0.7)
        train_dates_set = set(unique_dates[:split_idx])

        train_mask = np.array([d in train_dates_set for d in dates_group])
        test_mask = ~train_mask

        X_train, y_train_raw = X_group[train_mask], y_group[train_mask]
        X_test, y_test = X_group[test_mask], y_group[test_mask]
        dates_train = dates_group[train_mask]
        dates_test = dates_group[test_mask]

        if len(y_train_raw) < 60:
            log.warning(f"  {group}: too few training samples ({len(y_train_raw)}), skipping")
            continue

        # Winsorize targets
        y_train = np.clip(y_train_raw, -20.0, 20.0)

        # Auto-tune: compare RF vs GBT via nested WF-CV
        model_type, best_config, _ = _tune_best_model_wf(X_train, y_train, dates_train)

        # Build winning model on full training set
        if model_type == 'gbt':
            n_trees, max_depth, lr, min_leaf = best_config
            model_obj = GradientBoostedTrees(
                n_trees=n_trees, max_depth=max_depth,
                learning_rate=lr, min_samples_leaf=min_leaf)
            config_str = f"GBT {n_trees}x{max_depth}d lr={lr}"
        else:
            n_trees, max_depth, min_leaf = best_config
            lr = None
            model_obj = SimpleForest(n_trees=n_trees, max_depth=max_depth,
                                     min_samples_leaf=min_leaf)
            config_str = f"RF {n_trees}x{max_depth}d"

        model_obj.fit(X_train, y_train)

        # Evaluate
        y_pred_train = model_obj.predict(X_train)
        y_pred_test = model_obj.predict(X_test)

        train_dir_acc, train_mae, _, _ = _eval_predictions(
            y_pred_train, y_train_raw, dates_train)
        test_dir_acc, test_mae, r2, test_rank_corr = _eval_predictions(
            y_pred_test, y_test, dates_test)

        # Compare with Elastic Net
        enet = enet_results.get(group, {})
        enet_dir_acc = enet.get('dir_acc', 0)
        enet_rank = enet.get('rank', 0)

        # Use forest if it beats Elastic Net on composite metric
        forest_composite = test_dir_acc + 0.5 * max(test_rank_corr, 0)
        enet_composite = enet_dir_acc + 0.5 * max(enet_rank, 0)
        wins_over_enet = forest_composite > enet_composite

        # Determine blend weight (same thresholds as Elastic Net)
        if test_dir_acc >= 0.60:
            blend_weight = 0.15
        elif test_dir_acc >= 0.52:
            blend_weight = 0.08
        else:
            blend_weight = 0.0

        # Only use forest if it beats or matches Elastic Net
        if not wins_over_enet:
            blend_weight = 0.0  # defer to Elastic Net

        # Feature importance (approximate: split frequency)
        feat_counts = np.zeros(len(FEATURE_NAMES))
        if model_type == 'gbt':
            all_feat_idx = list(range(len(FEATURE_NAMES)))
            for tree in model_obj.trees:
                _count_splits(tree.tree, all_feat_idx, feat_counts)
        else:
            for tree, feat_idx in model_obj.trees:
                _count_splits(tree.tree, feat_idx, feat_counts)
        feat_sum = feat_counts.sum()
        feat_importance = feat_counts / feat_sum if feat_sum > 0 else feat_counts

        # Save model
        model_data = {
            'model': model_obj.to_dict(),
            'model_type': model_type,
            'feature_names': FEATURE_NAMES,
            'n_trees': n_trees,
            'max_depth': max_depth,
            'min_samples_leaf': min_leaf,
            'train_mae': round(train_mae, 4),
            'test_mae': round(test_mae, 4),
            'train_dir_acc': round(train_dir_acc, 4),
            'test_dir_acc': round(test_dir_acc, 4),
            'r2': round(r2, 4),
            'test_rank_corr': round(test_rank_corr, 4),
            'blend_weight': blend_weight,
            'wins_over_enet': wins_over_enet,
            'enet_dir_acc': round(enet_dir_acc, 4),
            'enet_rank': round(enet_rank, 4),
            'feature_importance': {name: round(float(imp), 4)
                                   for name, imp in zip(FEATURE_NAMES, feat_importance)},
            'n_train': len(y_train_raw),
            'n_test': len(y_test),
            'group': group,
            'coins': group_coins,
            'run_id': run_id,
            'trained_at': datetime.now(timezone.utc).isoformat(),
        }

        model_path = MODELS_DIR / f'{group}.json'
        model_path.write_text(json.dumps(model_data, indent=2))

        vs_enet = model_type.upper() if wins_over_enet else "ENET"
        log.info(f"  {group:10s}: dir={test_dir_acc*100:.1f}%, rank={test_rank_corr:.3f}, "
                 f"R2={r2:.4f}, {config_str} "
                 f"[vs enet: {enet_dir_acc*100:.1f}%/{enet_rank:.3f}] -> {vs_enet} "
                 f"(blend={blend_weight})")

        # Top features by split frequency
        top = sorted(zip(FEATURE_NAMES, feat_importance), key=lambda x: -x[1])[:4]
        top_str = ', '.join(f'{name}={imp:.2f}' for name, imp in top)
        log.info(f"    Splits: {top_str}")

        results[group] = model_data

    return results


def train_from_features(force: bool = False) -> dict:
    """Train forest models using raw features from feature_builder.

    Eliminates circular dependency on forecast_engine category scores.
    Trees handle NaN by treating it as a separate split direction.
    """
    from src.crypto.regression_model import COIN_GROUPS, _eval_predictions
    from src.crypto.feature_builder import FeatureBuilder

    MARKET_DB = Path('data/crypto/market.db')
    log.info("Training forest models from raw features (no circular dependency)")
    fb = FeatureBuilder(db_path=str(MARKET_DB))
    df = fb.build_dataset(include_labels=True)

    target_col = 'label_7d'
    if target_col not in df.columns:
        log.error(f"No {target_col} in dataset")
        return {}

    df = df.dropna(subset=[target_col]).copy()
    df[target_col] = df[target_col].clip(-0.20, 0.20) * 100

    feature_cols = [c for c in fb.FEATURE_COLS if c in df.columns]
    log.info(f"  Dataset: {len(df)} rows, {len(feature_cols)} features")

    # Load Elastic Net results for comparison
    enet_results = {}
    enet_dir = Path('data/crypto/regression_models')
    for g in COIN_GROUPS:
        enet_path = enet_dir / f'{g}.json'
        if enet_path.exists():
            enet_data = json.loads(enet_path.read_text())
            enet_results[g] = {
                'dir_acc': enet_data.get('test_dir_acc', 0),
                'rank': enet_data.get('test_rank_corr', 0),
            }

    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    results = {}

    for group, group_coins in COIN_GROUPS.items():
        df_g = df[df['coin'].isin(group_coins)].copy()
        if len(df_g) < 200:
            log.warning(f"  {group}: too few samples ({len(df_g)}), skipping")
            continue

        dates = sorted(df_g['date'].unique())
        split_idx = int(len(dates) * 0.7)
        train_dates = set(dates[:split_idx])

        train_mask = df_g['date'].isin(train_dates)
        X_train = df_g.loc[train_mask, feature_cols].values
        y_train_raw = df_g.loc[train_mask, target_col].values
        X_test = df_g.loc[~train_mask, feature_cols].values
        y_test = df_g.loc[~train_mask, target_col].values
        dates_train = df_g.loc[train_mask, 'date'].values
        dates_test = df_g.loc[~train_mask, 'date'].values

        if len(y_train_raw) < 100:
            continue

        y_train = np.clip(y_train_raw, -20.0, 20.0)

        # Replace NaN with column median (our trees don't handle NaN natively)
        col_medians = np.nanmedian(X_train, axis=0)
        col_medians = np.where(np.isnan(col_medians), 0.0, col_medians)
        for j in range(X_train.shape[1]):
            X_train[np.isnan(X_train[:, j]), j] = col_medians[j]
            X_test[np.isnan(X_test[:, j]), j] = col_medians[j]

        # Train GBT (stronger than RF for this task)
        model_obj = GradientBoostedTrees(
            n_trees=50, max_depth=4, learning_rate=0.05, min_samples_leaf=30)
        model_obj.fit(X_train, y_train)

        y_pred_test = model_obj.predict(X_test)
        test_dir_acc, test_mae, r2, test_rank_corr = _eval_predictions(
            y_pred_test, y_test, dates_test)

        # Compare with Elastic Net
        enet = enet_results.get(group, {})
        forest_composite = test_dir_acc + 0.5 * max(test_rank_corr, 0)
        enet_composite = enet.get('dir_acc', 0) + 0.5 * max(enet.get('rank', 0), 0)
        wins_over_enet = forest_composite > enet_composite

        if test_dir_acc >= 0.60:
            blend_weight = 0.15
        elif test_dir_acc >= 0.52:
            blend_weight = 0.08
        else:
            blend_weight = 0.0
        if not wins_over_enet:
            blend_weight = 0.0

        # Feature importance
        feat_counts = np.zeros(len(feature_cols))
        all_feat_idx = list(range(len(feature_cols)))
        for tree in model_obj.trees:
            _count_splits(tree.tree, all_feat_idx, feat_counts)
        feat_sum = feat_counts.sum()
        feat_importance = feat_counts / feat_sum if feat_sum > 0 else feat_counts

        model_data = {
            'model': model_obj.to_dict(),
            'model_type': 'gbt',
            'feature_names': feature_cols,
            'col_medians': col_medians.tolist(),
            'n_trees': 50,
            'max_depth': 4,
            'min_samples_leaf': 30,
            'test_dir_acc': round(test_dir_acc, 4),
            'test_mae': round(test_mae, 4),
            'r2': round(r2, 4),
            'test_rank_corr': round(test_rank_corr, 4),
            'blend_weight': blend_weight,
            'wins_over_enet': wins_over_enet,
            'feature_importance': {name: round(float(imp), 4)
                                   for name, imp in zip(feature_cols, feat_importance)},
            'n_train': len(y_train_raw),
            'n_test': len(y_test),
            'group': group,
            'data_source': 'feature_builder',
            'trained_at': datetime.now(timezone.utc).isoformat(),
        }

        model_path = MODELS_DIR / f'{group}.json'
        model_path.write_text(json.dumps(model_data, indent=2))

        winner = "GBT" if wins_over_enet else "ENET"
        log.info(f"  {group:10s}: dir={test_dir_acc*100:.1f}%, rank={test_rank_corr:.3f}, "
                 f"R2={r2:.4f}, blend={blend_weight} [{winner}] "
                 f"(train={len(y_train_raw)}, test={len(y_test)})")

        top = sorted(zip(feature_cols, feat_importance), key=lambda x: -x[1])[:5]
        log.info(f"    Top: {', '.join(f'{n}={v:.3f}' for n, v in top)}")

        results[group] = model_data

    return results


def _count_splits(node, feat_idx, feat_counts):
    """Count how many times each feature is used for splitting (recursive)."""
    if node['leaf']:
        return
    # Map tree-local feature index to global feature index
    global_feat = feat_idx[node['feature']]
    feat_counts[global_feat] += 1
    _count_splits(node['left'], feat_idx, feat_counts)
    _count_splits(node['right'], feat_idx, feat_counts)


# ═══════════════════════════════════════════════════════════════
# PREDICTION
# ═══════════════════════════════════════════════════════════════

_forest_cache = {}
_forest_blend_cache = {}


def load_forest(group):
    """Load saved tree model for a group. Returns model object or None.

    Supports both SimpleForest and GradientBoostedTrees.
    Only loads if the model won over Elastic Net.
    """
    if group in _forest_cache:
        return _forest_cache[group]

    model_path = MODELS_DIR / f'{group}.json'
    if not model_path.exists():
        _forest_cache[group] = None
        return None

    try:
        data = json.loads(model_path.read_text())

        # Only use if it won over elastic net
        if not data.get('wins_over_enet', False):
            _forest_cache[group] = None
            _forest_blend_cache[group] = 0.0
            return None

        model_type = data.get('model_type', 'forest')
        model_dict = data.get('model', data.get('forest'))

        if model_type == 'gbt':
            model_obj = GradientBoostedTrees()
            model_obj.from_dict(model_dict)
        else:
            model_obj = SimpleForest()
            model_obj.from_dict(model_dict)

        _forest_cache[group] = model_obj
        _forest_blend_cache[group] = data.get('blend_weight', 0.0)
        return model_obj
    except Exception as e:
        log.warning(f"Failed to load model for {group}: {e}")
        _forest_cache[group] = None
        return None


def get_forest_blend_weight(group):
    """Get blend weight for forest model. 0.0 if forest doesn't beat elastic net."""
    if group not in _forest_blend_cache:
        load_forest(group)
    return _forest_blend_cache.get(group, 0.0)


def predict_change_forest(features, group):
    """Predict actual_change_pct using group's tree model (RF or GBT).

    features: numpy array of shape (12,) — raw features (same as regression_model)
    group: coin group name
    Returns: predicted change % or None
    """
    model_obj = load_forest(group)
    if model_obj is None:
        return None

    prediction = model_obj.predict_one(features)
    return max(-20.0, min(20.0, prediction))


def predict_score_forest(features, group):
    """Predict and convert to [-1, 1] score for forecast_engine integration."""
    change = predict_change_forest(features, group)
    if change is None:
        return None
    score = change / 10.0
    return max(-1.0, min(1.0, score))


def clear_cache():
    """Clear forest model cache."""
    _forest_cache.clear()
    _forest_blend_cache.clear()


# ═══════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════

if __name__ == '__main__':
    run_id = sys.argv[1] if len(sys.argv) > 1 else None

    log.info("=" * 60)
    log.info("ML CORRECTOR TRAINING (Level 6)")
    log.info("  Random Forest — non-linear correction")
    log.info("  Auto-tuned hyperparams via nested WF-CV")
    log.info("=" * 60)

    results = train_all_groups(run_id)
    if results:
        log.info(f"\n{'='*60}")
        log.info(f"Trained {len(results)} forest models")
        n_tree_wins = sum(1 for m in results.values() if m['wins_over_enet'])
        n_enet_wins = len(results) - n_tree_wins
        log.info(f"  Tree model wins: {n_tree_wins}, Elastic Net wins: {n_enet_wins}")
        for group, model in sorted(results.items()):
            winner = model.get('model_type', 'forest').upper() if model['wins_over_enet'] else "enet"
            log.info(f"  {group:10s}: dir={model['test_dir_acc']*100:.1f}%, "
                     f"rank={model['test_rank_corr']:.3f}, blend={model['blend_weight']} "
                     f"[{winner}]")
        log.info(f"Models saved to {MODELS_DIR}/")
        log.info(f"{'='*60}")
    else:
        log.error("No models trained!")
