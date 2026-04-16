"""
Forecast v3 — LightGBM Walk-Forward Training & Prediction

Walk-forward CV ONLY. Random split BANNED.
Per-group models (majors, l1_alts, defi, ai, meme).
Honest metrics — report exactly what the model achieves.
"""

import json
import logging
import pickle
import numpy as np
import pandas as pd
import lightgbm as lgb
from pathlib import Path
from datetime import datetime
from typing import Optional
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score, classification_report

from src.crypto.feature_builder import FeatureBuilder, COIN_GROUPS, COIN_TO_GROUP

logger = logging.getLogger(__name__)

MODEL_DIR = Path(__file__).parent.parent.parent / 'data' / 'crypto' / 'models_v3'
DATASET_PATH = Path(__file__).parent.parent.parent / 'data' / 'crypto' / 'training_dataset_v3.csv'

# Features that have enough data for training (>50% availability)
CORE_FEATURES = [
    # Price (13)
    'ret_1d', 'ret_3d', 'ret_7d', 'ret_14d', 'ret_30d',
    'volatility_7d', 'volatility_30d',
    'rsi_14', 'bb_position',
    'ma50_dist', 'ma200_dist',
    'volume_ratio_7d', 'high_low_range_7d',
    # Funding (3)
    'funding_rate', 'funding_rate_7d_avg', 'funding_rate_pctl_30d',
    # Fear & Greed (3)
    'fg_value', 'fg_change_7d', 'fg_percentile_30d',
    # Coinbase premium (1)
    'coinbase_premium',
    # ETF flows (2)
    'etf_flow_usd', 'etf_flow_7d_avg',
    # BTC cross-asset (3)
    'btc_ret_7d', 'btc_ret_30d', 'corr_btc_30d',
    # Macro (7)
    'vix', 'yield_curve', 'treasury_10y', 'fed_rate',
    'cpi_yoy', 'fomc_meeting', 'fomc_days_until',
    # Google Trends (3)
    'gtrend_bitcoin', 'gtrend_crypto', 'gtrend_bitcoin_change',
    # DeFi TVL (2)
    'defi_tvl_change_7d', 'defi_tvl_change_30d',
    # Cross-coin breadth (4)
    'pct_above_ma50', 'pct_above_ma200',
    'market_avg_ret_7d', 'n_coins_new_high_30d',
    # Halving cycle (2)
    'days_since_halving', 'halving_cycle_phase',
    # Derivatives (5) — NOW 83-91% available thanks to CoinGlass 5y backfill
    'oi_change_1d', 'oi_change_7d',
    'ls_long_pct', 'ls_ratio',
    'taker_ratio',
    # Calendar (2)
    'day_of_week', 'is_weekend',
]

# CQ features — only available for 8 coins, add as bonus
CQ_FEATURES = ['cq_netflow', 'cq_netflow_7d_avg', 'cq_reserve_change_7d']


class ForecastModelV3:
    """
    Walk-forward trained LightGBM model for crypto direction prediction.

    Supports:
    - Global model (all coins)
    - Per-group models (majors, l1_alts, defi, ai, meme)
    - 3d and 7d prediction horizons
    - Confidence calibration
    """

    def __init__(self):
        self.models = {}  # {group: {horizon: model}}
        self.scalers = {}  # {group: scaler}
        self.lr_models = {}  # Logistic regression baselines
        self.feature_cols = CORE_FEATURES.copy()
        self.metrics = {}  # Walk-forward results
        self.feature_importance = {}
        self.trained_at = None

    def load_dataset(self) -> pd.DataFrame:
        """Load pre-built training dataset."""
        if DATASET_PATH.exists():
            df = pd.read_csv(DATASET_PATH)
            df['date'] = pd.to_datetime(df['date'])
            return df
        else:
            logger.info("No cached dataset, building from scratch...")
            builder = FeatureBuilder()
            return builder.build_dataset(include_labels=True)

    # ── Walk-Forward CV ──────────────────────────────────────────

    def walk_forward_cv(
        self,
        df: pd.DataFrame,
        horizon: str = '7d',
        min_train_days: int = 90,
        val_days: int = 30,
        group: str = None,
    ) -> dict:
        """
        Walk-forward cross-validation with expanding window.

        Args:
            df: Dataset with features and labels
            horizon: '3d' or '7d'
            min_train_days: Minimum training window
            val_days: Validation window size
            group: Coin group to filter, or None for all

        Returns:
            Dict with metrics and fold details
        """
        label_col = f'label_dir_{horizon}'
        label_cont = f'label_{horizon}'

        # Filter by group if specified
        if group:
            group_coins = COIN_GROUPS.get(group, [])
            if group == 'l1_alts':
                group_coins = group_coins + ['ARB', 'OP', 'POL']
            df = df[df['coin'].isin(group_coins)].copy()

        # Drop rows without labels
        df = df.dropna(subset=[label_col])

        # Sort by date
        df = df.sort_values('date').reset_index(drop=True)

        # Get unique dates
        dates = sorted(df['date'].unique())
        if len(dates) < min_train_days + val_days:
            logger.warning(f"Not enough dates for CV: {len(dates)} (need {min_train_days + val_days})")
            return {'error': 'insufficient_data'}

        # Walk-forward folds
        folds = []
        fold_start = min_train_days
        while fold_start + val_days <= len(dates):
            train_end_date = dates[fold_start - 1]
            val_start_date = dates[fold_start]
            val_end_idx = min(fold_start + val_days - 1, len(dates) - 1)
            val_end_date = dates[val_end_idx]

            folds.append({
                'train_end': train_end_date,
                'val_start': val_start_date,
                'val_end': val_end_date,
            })
            fold_start += val_days

        logger.info(f"Walk-forward: {len(folds)} folds, {len(dates)} total dates")

        # Features
        feature_cols = [c for c in self.feature_cols if c in df.columns]
        X_all = df[feature_cols]
        y_all = df[label_col].astype(int)
        y_cont = df[label_cont] if label_cont in df.columns else None
        dates_all = df['date']

        # Track metrics across folds
        all_preds = []
        all_true = []
        all_proba = []
        all_dates = []
        fold_metrics = []

        for i, fold in enumerate(folds):
            train_mask = dates_all <= fold['train_end']
            val_mask = (dates_all >= fold['val_start']) & (dates_all <= fold['val_end'])

            X_train = X_all[train_mask]
            y_train = y_all[train_mask]
            X_val = X_all[val_mask]
            y_val = y_all[val_mask]

            if len(X_val) == 0 or len(X_train) < 50:
                continue

            # Sample weights (recent data matters more)
            weights = self._compute_sample_weights(dates_all[train_mask], half_life_days=180)

            # Normalize features (fit on train only!)
            scaler = StandardScaler()
            X_train_scaled = pd.DataFrame(
                scaler.fit_transform(X_train.fillna(0)),
                columns=feature_cols,
                index=X_train.index,
            )
            X_val_scaled = pd.DataFrame(
                scaler.transform(X_val.fillna(0)),
                columns=feature_cols,
                index=X_val.index,
            )

            # LightGBM — handles NaN natively, no need for fillna
            # But for consistency with LogReg, we use scaled features
            lgb_params = {
                'objective': 'multiclass',
                'num_class': 3,
                'metric': 'multi_logloss',
                'learning_rate': 0.05,
                'max_depth': 5,
                'num_leaves': 31,
                'min_child_samples': 20,
                'subsample': 0.8,
                'colsample_bytree': 0.8,
                'reg_alpha': 0.1,
                'reg_lambda': 1.0,
                'verbose': -1,
                'seed': 42,
            }

            # Map labels: -1→0, 0→1, 1→2 for LightGBM
            y_train_mapped = y_train.map({-1: 0, 0: 1, 1: 2})
            y_val_mapped = y_val.map({-1: 0, 0: 1, 1: 2})

            train_data = lgb.Dataset(X_train.fillna(np.nan), label=y_train_mapped, weight=weights)
            val_data = lgb.Dataset(X_val.fillna(np.nan), label=y_val_mapped, reference=train_data)

            model = lgb.train(
                lgb_params,
                train_data,
                num_boost_round=300,
                valid_sets=[val_data],
                callbacks=[lgb.early_stopping(30, verbose=False), lgb.log_evaluation(0)],
            )

            # Predict
            proba = model.predict(X_val.fillna(np.nan))  # shape: (n, 3)
            preds_mapped = np.argmax(proba, axis=1)
            # Map back: 0→-1, 1→0, 2→1
            preds = np.where(preds_mapped == 0, -1, np.where(preds_mapped == 2, 1, 0))

            # Confidence: max probability
            confidence = np.max(proba, axis=1)

            # Fold accuracy
            fold_acc = accuracy_score(y_val, preds)
            # Directional accuracy (BUY/SELL only)
            dir_mask = (y_val != 0)
            dir_acc = accuracy_score(y_val[dir_mask], preds[dir_mask]) if dir_mask.sum() > 0 else 0

            fold_metrics.append({
                'fold': i,
                'train_size': len(X_train),
                'val_size': len(X_val),
                'accuracy': fold_acc,
                'directional_accuracy': dir_acc,
                'n_buy_pred': (preds == 1).sum(),
                'n_sell_pred': (preds == -1).sum(),
                'n_neutral_pred': (preds == 0).sum(),
            })

            all_preds.extend(preds)
            all_true.extend(y_val.values)
            all_proba.extend(confidence)
            all_dates.extend(dates_all[val_mask].values)

        if not fold_metrics:
            return {'error': 'no_valid_folds'}

        # Aggregate metrics
        all_preds = np.array(all_preds)
        all_true = np.array(all_true)
        all_proba = np.array(all_proba)

        overall_acc = accuracy_score(all_true, all_preds)
        dir_mask = all_true != 0
        dir_acc = accuracy_score(all_true[dir_mask], all_preds[dir_mask]) if dir_mask.sum() > 0 else 0

        # Per-signal accuracy
        buy_mask = all_true == 1
        sell_mask = all_true == -1
        buy_pred_mask = all_preds == 1
        sell_pred_mask = all_preds == -1

        buy_precision = (
            (all_true[buy_pred_mask] == 1).sum() / buy_pred_mask.sum()
            if buy_pred_mask.sum() > 0 else 0
        )
        sell_precision = (
            (all_true[sell_pred_mask] == -1).sum() / sell_pred_mask.sum()
            if sell_pred_mask.sum() > 0 else 0
        )

        # Confidence calibration
        high_conf_mask = all_proba >= 0.6
        if high_conf_mask.sum() > 0:
            high_conf_acc = accuracy_score(all_true[high_conf_mask], all_preds[high_conf_mask])
        else:
            high_conf_acc = 0

        results = {
            'horizon': horizon,
            'group': group or 'all',
            'n_folds': len(fold_metrics),
            'total_predictions': len(all_preds),
            'overall_accuracy': round(overall_acc * 100, 1),
            'directional_accuracy': round(dir_acc * 100, 1),
            'buy_precision': round(buy_precision * 100, 1),
            'sell_precision': round(sell_precision * 100, 1),
            'n_buy_actual': int(buy_mask.sum()),
            'n_sell_actual': int(sell_mask.sum()),
            'n_buy_predicted': int(buy_pred_mask.sum()),
            'n_sell_predicted': int(sell_pred_mask.sum()),
            'high_conf_accuracy': round(high_conf_acc * 100, 1),
            'high_conf_count': int(high_conf_mask.sum()),
            'fold_metrics': fold_metrics,
        }

        return results

    # ── Logistic Regression Baseline ─────────────────────────────

    def logistic_baseline_cv(
        self, df: pd.DataFrame, horizon: str = '7d',
        min_train_days: int = 90, val_days: int = 30,
        group: str = None,
    ) -> dict:
        """Logistic regression baseline with same walk-forward CV."""
        label_col = f'label_dir_{horizon}'

        if group:
            group_coins = COIN_GROUPS.get(group, [])
            if group == 'l1_alts':
                group_coins = group_coins + ['ARB', 'OP', 'POL']
            df = df[df['coin'].isin(group_coins)].copy()

        df = df.dropna(subset=[label_col]).sort_values('date').reset_index(drop=True)
        dates = sorted(df['date'].unique())
        if len(dates) < min_train_days + val_days:
            return {'error': 'insufficient_data'}

        feature_cols = [c for c in self.feature_cols if c in df.columns]
        X_all = df[feature_cols]
        y_all = df[label_col].astype(int)
        dates_all = df['date']

        folds = []
        fold_start = min_train_days
        while fold_start + val_days <= len(dates):
            folds.append({
                'train_end': dates[fold_start - 1],
                'val_start': dates[fold_start],
                'val_end': dates[min(fold_start + val_days - 1, len(dates) - 1)],
            })
            fold_start += val_days

        all_preds = []
        all_true = []

        for fold in folds:
            train_mask = dates_all <= fold['train_end']
            val_mask = (dates_all >= fold['val_start']) & (dates_all <= fold['val_end'])

            X_train = X_all[train_mask].fillna(0)
            y_train = y_all[train_mask]
            X_val = X_all[val_mask].fillna(0)
            y_val = y_all[val_mask]

            if len(X_val) == 0 or len(X_train) < 50:
                continue

            scaler = StandardScaler()
            X_train_s = scaler.fit_transform(X_train)
            X_val_s = scaler.transform(X_val)

            lr = LogisticRegression(max_iter=1000, C=1.0)
            lr.fit(X_train_s, y_train)
            preds = lr.predict(X_val_s)

            all_preds.extend(preds)
            all_true.extend(y_val.values)

        all_preds = np.array(all_preds)
        all_true = np.array(all_true)

        overall_acc = accuracy_score(all_true, all_preds)
        dir_mask = all_true != 0
        dir_acc = accuracy_score(all_true[dir_mask], all_preds[dir_mask]) if dir_mask.sum() > 0 else 0

        return {
            'horizon': horizon,
            'group': group or 'all',
            'model': 'LogisticRegression',
            'overall_accuracy': round(overall_acc * 100, 1),
            'directional_accuracy': round(dir_acc * 100, 1),
            'total_predictions': len(all_preds),
        }

    # ── Binary Walk-Forward CV ─────────────────────────────────

    @staticmethod
    def _compute_sample_weights(dates: pd.Series, half_life_days: int = 365) -> np.ndarray:
        """Exponential decay weights: recent data matters more."""
        max_date = dates.max()
        days_ago = (max_date - dates).dt.days.values
        weights = np.exp(-np.log(2) * days_ago / half_life_days)
        return weights

    def walk_forward_cv_binary(
        self,
        df: pd.DataFrame,
        horizon: str = '7d',
        min_train_days: int = 90,
        val_days: int = 30,
        group: str = None,
    ) -> dict:
        """
        Walk-forward CV with BINARY models (UP vs not-UP, DOWN vs not-DOWN).
        Uses volatility-adjusted labels + sample weighting.
        """
        up_col = f'label_up_{horizon}'
        down_col = f'label_down_{horizon}'
        label_dir = f'label_dir_{horizon}'

        if group:
            group_coins = COIN_GROUPS.get(group, [])
            if group == 'l1_alts':
                group_coins = group_coins + ['ARB', 'OP', 'POL']
            df = df[df['coin'].isin(group_coins)].copy()

        # Need both binary labels
        if up_col not in df.columns or down_col not in df.columns:
            # Fallback: build from label_dir
            df = df.dropna(subset=[label_dir])
        else:
            df = df.dropna(subset=[up_col, down_col])

        df = df.sort_values('date').reset_index(drop=True)
        dates = sorted(df['date'].unique())
        if len(dates) < min_train_days + val_days:
            return {'error': 'insufficient_data'}

        folds = []
        fold_start = min_train_days
        while fold_start + val_days <= len(dates):
            folds.append({
                'train_end': dates[fold_start - 1],
                'val_start': dates[fold_start],
                'val_end': dates[min(fold_start + val_days - 1, len(dates) - 1)],
            })
            fold_start += val_days

        logger.info(f"Binary walk-forward: {len(folds)} folds, {len(dates)} dates")

        feature_cols = [c for c in self.feature_cols if c in df.columns]
        X_all = df[feature_cols]
        dates_all = df['date']

        # Use vol-adjusted labels if available, else fixed
        if up_col in df.columns:
            y_up_all = df[up_col].astype(int)
            y_down_all = df[down_col].astype(int)
        else:
            y_dir = df[label_dir].astype(int)
            y_up_all = (y_dir == 1).astype(int)
            y_down_all = (y_dir == -1).astype(int)

        # Also keep label_dir for directional accuracy measurement
        y_dir_all = df[label_dir].astype(int) if label_dir in df.columns else None

        lgb_params = {
            'objective': 'binary',
            'metric': 'binary_logloss',
            'learning_rate': 0.05,
            'max_depth': 5,
            'num_leaves': 31,
            'min_child_samples': 20,
            'subsample': 0.8,
            'colsample_bytree': 0.8,
            'reg_alpha': 0.1,
            'reg_lambda': 1.0,
            'verbose': -1,
            'seed': 42,
        }

        all_p_up = []
        all_p_down = []
        all_true_dir = []
        all_preds = []

        for fold in folds:
            train_mask = dates_all <= fold['train_end']
            val_mask = (dates_all >= fold['val_start']) & (dates_all <= fold['val_end'])

            X_train = X_all[train_mask]
            X_val = X_all[val_mask]

            if len(X_val) == 0 or len(X_train) < 50:
                continue

            # Sample weights (exponential decay)
            weights = self._compute_sample_weights(dates_all[train_mask])

            # Train UP model
            y_up_train = y_up_all[train_mask]
            train_up = lgb.Dataset(X_train.fillna(np.nan), label=y_up_train, weight=weights)
            val_up = lgb.Dataset(X_val.fillna(np.nan), label=y_up_all[val_mask], reference=train_up)
            model_up = lgb.train(
                lgb_params, train_up, num_boost_round=300,
                valid_sets=[val_up],
                callbacks=[lgb.early_stopping(30, verbose=False), lgb.log_evaluation(0)],
            )

            # Train DOWN model
            y_down_train = y_down_all[train_mask]
            train_down = lgb.Dataset(X_train.fillna(np.nan), label=y_down_train, weight=weights)
            val_down = lgb.Dataset(X_val.fillna(np.nan), label=y_down_all[val_mask], reference=train_down)
            model_down = lgb.train(
                lgb_params, train_down, num_boost_round=300,
                valid_sets=[val_down],
                callbacks=[lgb.early_stopping(30, verbose=False), lgb.log_evaluation(0)],
            )

            # Predict
            p_up = model_up.predict(X_val.fillna(np.nan))
            p_down = model_down.predict(X_val.fillna(np.nan))

            # Direction: compare P(UP) vs P(DOWN)
            preds = np.where(
                (p_up > p_down) & (p_up > 0.45), 1,
                np.where((p_down > p_up) & (p_down > 0.45), -1, 0)
            )

            all_p_up.extend(p_up)
            all_p_down.extend(p_down)
            all_preds.extend(preds)

            if y_dir_all is not None:
                all_true_dir.extend(y_dir_all[val_mask].values)

        if not all_preds:
            return {'error': 'no_valid_folds'}

        all_preds = np.array(all_preds)
        all_p_up = np.array(all_p_up)
        all_p_down = np.array(all_p_down)
        all_true_dir = np.array(all_true_dir) if all_true_dir else np.zeros_like(all_preds)

        # Metrics (using fixed-threshold label_dir for comparison)
        overall_acc = accuracy_score(all_true_dir, all_preds)
        dir_mask = all_true_dir != 0
        dir_acc = accuracy_score(all_true_dir[dir_mask], all_preds[dir_mask]) if dir_mask.sum() > 0 else 0

        buy_pred = all_preds == 1
        sell_pred = all_preds == -1
        buy_prec = (all_true_dir[buy_pred] == 1).sum() / buy_pred.sum() if buy_pred.sum() > 0 else 0
        sell_prec = (all_true_dir[sell_pred] == -1).sum() / sell_pred.sum() if sell_pred.sum() > 0 else 0

        # High confidence: max(P(UP), P(DOWN)) > 0.6
        high_conf = np.maximum(all_p_up, all_p_down) > 0.6
        high_conf_acc = accuracy_score(
            all_true_dir[high_conf], all_preds[high_conf]
        ) if high_conf.sum() > 0 else 0

        return {
            'horizon': horizon,
            'group': group or 'all',
            'model': 'BinaryLGBM',
            'n_folds': len(folds),
            'total_predictions': len(all_preds),
            'overall_accuracy': round(overall_acc * 100, 1),
            'directional_accuracy': round(dir_acc * 100, 1),
            'buy_precision': round(buy_prec * 100, 1),
            'sell_precision': round(sell_prec * 100, 1),
            'n_buy_predicted': int(buy_pred.sum()),
            'n_sell_predicted': int(sell_pred.sum()),
            'high_conf_accuracy': round(high_conf_acc * 100, 1),
            'high_conf_count': int(high_conf.sum()),
        }

    # ── Final Model Training ─────────────────────────────────────

    def train_final_binary(
        self,
        df: pd.DataFrame,
        horizon: str = '7d',
        group: str = None,
    ):
        """Train final binary UP + DOWN models for production."""
        up_col = f'label_up_{horizon}'
        down_col = f'label_down_{horizon}'
        label_dir = f'label_dir_{horizon}'

        if group:
            group_coins = COIN_GROUPS.get(group, [])
            if group == 'l1_alts':
                group_coins = group_coins + ['ARB', 'OP', 'POL']
            df = df[df['coin'].isin(group_coins)].copy()

        if up_col in df.columns:
            df = df.dropna(subset=[up_col, down_col])
            y_up = df[up_col].astype(int)
            y_down = df[down_col].astype(int)
        else:
            df = df.dropna(subset=[label_dir])
            y_dir = df[label_dir].astype(int)
            y_up = (y_dir == 1).astype(int)
            y_down = (y_dir == -1).astype(int)

        df = df.sort_values('date').reset_index(drop=True)
        feature_cols = [c for c in self.feature_cols if c in df.columns]
        X = df[feature_cols]

        # Sample weights
        weights = self._compute_sample_weights(df['date'])

        params = {
            'objective': 'binary',
            'metric': 'binary_logloss',
            'learning_rate': 0.05,
            'max_depth': 5,
            'num_leaves': 31,
            'min_child_samples': 20,
            'subsample': 0.8,
            'colsample_bytree': 0.8,
            'reg_alpha': 0.1,
            'reg_lambda': 1.0,
            'verbose': -1,
            'seed': 42,
        }

        # Train UP model
        train_up = lgb.Dataset(X.fillna(np.nan), label=y_up, weight=weights)
        model_up = lgb.train(params, train_up, num_boost_round=200)

        # Train DOWN model
        train_down = lgb.Dataset(X.fillna(np.nan), label=y_down, weight=weights)
        model_down = lgb.train(params, train_down, num_boost_round=200)

        # Feature importance (average of both models)
        imp_up = dict(zip(feature_cols, model_up.feature_importance(importance_type='gain')))
        imp_down = dict(zip(feature_cols, model_down.feature_importance(importance_type='gain')))
        combined = {f: imp_up.get(f, 0) + imp_down.get(f, 0) for f in feature_cols}
        combined = dict(sorted(combined.items(), key=lambda x: -x[1]))

        key = group or 'all'
        self.models[f'{key}_{horizon}_up'] = model_up
        self.models[f'{key}_{horizon}_down'] = model_down
        self.feature_importance[f'{key}_{horizon}'] = combined

        # Scaler
        scaler = StandardScaler()
        scaler.fit(X.fillna(0))
        self.scalers[key] = scaler

    def train_final(
        self,
        df: pd.DataFrame,
        horizon: str = '7d',
        group: str = None,
        best_params: dict = None,
    ) -> lgb.Booster:
        """
        Train final model on ALL available data (for production use).
        Call AFTER walk-forward CV validates the approach.
        """
        label_col = f'label_dir_{horizon}'

        if group:
            group_coins = COIN_GROUPS.get(group, [])
            if group == 'l1_alts':
                group_coins = group_coins + ['ARB', 'OP', 'POL']
            df = df[df['coin'].isin(group_coins)].copy()

        df = df.dropna(subset=[label_col]).sort_values('date').reset_index(drop=True)
        feature_cols = [c for c in self.feature_cols if c in df.columns]

        X = df[feature_cols]
        y = df[label_col].astype(int).map({-1: 0, 0: 1, 1: 2})

        # Scaler
        scaler = StandardScaler()
        scaler.fit(X.fillna(0))

        params = best_params or {
            'objective': 'multiclass',
            'num_class': 3,
            'metric': 'multi_logloss',
            'learning_rate': 0.05,
            'max_depth': 5,
            'num_leaves': 31,
            'min_child_samples': 20,
            'subsample': 0.8,
            'colsample_bytree': 0.8,
            'reg_alpha': 0.1,
            'reg_lambda': 1.0,
            'verbose': -1,
            'seed': 42,
        }

        # Sample weights (recent data more important)
        weights = self._compute_sample_weights(df['date'], half_life_days=180)

        train_data = lgb.Dataset(X.fillna(np.nan), label=y, weight=weights)
        model = lgb.train(params, train_data, num_boost_round=200)

        # Feature importance
        importance = dict(zip(feature_cols, model.feature_importance(importance_type='gain')))
        importance = dict(sorted(importance.items(), key=lambda x: -x[1]))

        key = group or 'all'
        self.models[f'{key}_{horizon}'] = model
        self.scalers[key] = scaler
        self.feature_importance[f'{key}_{horizon}'] = importance

        return model

    def predict(self, features: dict, coin: str, horizon: str = '7d') -> dict:
        """
        Predict direction for a single coin.

        Strategy:
        - 3-class model provides direction (UP/DOWN/NEUTRAL)
        - Binary models provide confidence boost (P(significant move))

        Returns:
            {
                'direction': 'BUY'|'SELL'|'NEUTRAL',
                'confidence': float (0-1),
                'probabilities': {'DOWN': float, 'NEUTRAL': float, 'UP': float},
            }
        """
        group = COIN_TO_GROUP.get(coin, 'l1_alts')
        X = pd.DataFrame([features])[self.feature_cols].fillna(np.nan)

        # 1. Get direction from 3-class model
        model_key = f'{group}_{horizon}'
        model = self.models.get(model_key) or self.models.get(f'all_{horizon}')
        if model is None:
            return {'direction': 'NEUTRAL', 'confidence': 0.0, 'probabilities': {}}

        proba = model.predict(X)[0]  # [P(DOWN), P(NEUTRAL), P(UP)]
        p_down, p_neutral, p_up = float(proba[0]), float(proba[1]), float(proba[2])
        spread = abs(p_up - p_down)

        if p_up > p_down and p_up > 0.40 and spread > 0.05:
            direction = 'BUY'
            confidence = p_up
        elif p_down > p_up and p_down > 0.40 and spread > 0.05:
            direction = 'SELL'
            confidence = p_down
        else:
            direction = 'NEUTRAL'
            confidence = p_neutral

        # 2. Binary confidence boost
        # If binary model agrees, boost confidence. If disagrees, reduce.
        model_up = self.models.get(f'{group}_{horizon}_up') or self.models.get(f'all_{horizon}_up')
        model_down = self.models.get(f'{group}_{horizon}_down') or self.models.get(f'all_{horizon}_down')

        if model_up is not None and model_down is not None:
            bp_up = float(model_up.predict(X)[0])
            bp_down = float(model_down.predict(X)[0])

            if direction == 'BUY' and bp_up > 0.20:
                # Binary agrees it's a significant UP → boost
                confidence = min(confidence + bp_up * 0.3, 0.95)
            elif direction == 'SELL' and bp_down > 0.20:
                # Binary agrees it's a significant DOWN → boost
                confidence = min(confidence + bp_down * 0.3, 0.95)
            elif direction == 'BUY' and bp_down > bp_up * 1.5:
                # Binary says DOWN more likely → demote to NEUTRAL
                direction = 'NEUTRAL'
                confidence = p_neutral
            elif direction == 'SELL' and bp_up > bp_down * 1.5:
                # Binary says UP more likely → demote to NEUTRAL
                direction = 'NEUTRAL'
                confidence = p_neutral

            # Add binary probs to output
            p_up = max(p_up, bp_up)
            p_down = max(p_down, bp_down)

        return {
            'direction': direction,
            'confidence': confidence,
            'probabilities': {'DOWN': p_down, 'NEUTRAL': p_neutral, 'UP': p_up},
        }

    # ── Save / Load ──────────────────────────────────────────────

    def save(self, version: str = None):
        """Save all models and metadata."""
        MODEL_DIR.mkdir(parents=True, exist_ok=True)
        version = version or datetime.now().strftime('%Y%m%d_%H%M%S')
        save_dir = MODEL_DIR / version
        save_dir.mkdir(exist_ok=True)

        for key, model in self.models.items():
            model.save_model(str(save_dir / f'{key}.lgb'))

        for key, scaler in self.scalers.items():
            with open(save_dir / f'scaler_{key}.pkl', 'wb') as f:
                pickle.dump(scaler, f)

        meta = {
            'version': version,
            'trained_at': datetime.now().isoformat(),
            'feature_cols': self.feature_cols,
            'metrics': self.metrics,
            'feature_importance': {
                k: {feat: float(val) for feat, val in v.items()}
                for k, v in self.feature_importance.items()
            },
        }
        with open(save_dir / 'meta.json', 'w') as f:
            json.dump(meta, f, indent=2, default=lambda o: int(o) if hasattr(o, 'item') else str(o))

        # Symlink to latest
        latest = MODEL_DIR / 'latest'
        if latest.exists():
            if latest.is_symlink():
                latest.unlink()
            elif latest.is_dir():
                import shutil
                shutil.rmtree(latest)
        # On Windows, just write a pointer file
        with open(MODEL_DIR / 'latest.txt', 'w') as f:
            f.write(version)

        logger.info(f"Saved model v{version} to {save_dir}")

    def load(self, version: str = None):
        """Load models from disk."""
        if version is None:
            ptr_file = MODEL_DIR / 'latest.txt'
            if ptr_file.exists():
                version = ptr_file.read_text().strip()
            else:
                raise FileNotFoundError("No saved model found")

        save_dir = MODEL_DIR / version
        if not save_dir.exists():
            raise FileNotFoundError(f"Model version {version} not found")

        # Load meta
        with open(save_dir / 'meta.json') as f:
            meta = json.load(f)
        self.feature_cols = meta['feature_cols']
        self.metrics = meta.get('metrics', {})
        self.feature_importance = meta.get('feature_importance', {})

        # Load models
        for model_file in save_dir.glob('*.lgb'):
            key = model_file.stem
            self.models[key] = lgb.Booster(model_file=str(model_file))

        # Load scalers
        for scaler_file in save_dir.glob('scaler_*.pkl'):
            key = scaler_file.stem.replace('scaler_', '')
            with open(scaler_file, 'rb') as f:
                self.scalers[key] = pickle.load(f)

        logger.info(f"Loaded model v{version}: {list(self.models.keys())}")

    # ── Hyperparameter Search ────────────────────────────────────

    def hyperparameter_search(
        self, df: pd.DataFrame, horizon: str = '7d', group: str = None,
    ) -> dict:
        """
        Simple grid search over key hyperparameters using walk-forward CV.
        Returns best params.
        """
        label_col = f'label_dir_{horizon}'

        if group:
            group_coins = COIN_GROUPS.get(group, [])
            if group == 'l1_alts':
                group_coins = group_coins + ['ARB', 'OP', 'POL']
            df = df[df['coin'].isin(group_coins)].copy()

        df = df.dropna(subset=[label_col]).sort_values('date').reset_index(drop=True)
        dates = sorted(df['date'].unique())
        feature_cols = [c for c in self.feature_cols if c in df.columns]

        # Parameter grid (focused, not exhaustive)
        param_grid = [
            {'max_depth': 3, 'num_leaves': 15, 'learning_rate': 0.05, 'min_child_samples': 30},
            {'max_depth': 5, 'num_leaves': 31, 'learning_rate': 0.05, 'min_child_samples': 20},
            {'max_depth': 5, 'num_leaves': 31, 'learning_rate': 0.03, 'min_child_samples': 20},
            {'max_depth': 7, 'num_leaves': 63, 'learning_rate': 0.05, 'min_child_samples': 15},
            {'max_depth': 5, 'num_leaves': 31, 'learning_rate': 0.1, 'min_child_samples': 25},
            {'max_depth': 4, 'num_leaves': 20, 'learning_rate': 0.05, 'min_child_samples': 30},
        ]

        best_score = 0
        best_params = None
        results_log = []

        for params in param_grid:
            full_params = {
                'objective': 'multiclass',
                'num_class': 3,
                'metric': 'multi_logloss',
                'subsample': 0.8,
                'colsample_bytree': 0.8,
                'reg_alpha': 0.1,
                'reg_lambda': 1.0,
                'verbose': -1,
                'seed': 42,
                **params,
            }

            # Quick walk-forward with larger steps
            all_preds = []
            all_true = []
            fold_start = 90

            while fold_start + 30 <= len(dates):
                train_end = dates[fold_start - 1]
                val_end = dates[min(fold_start + 29, len(dates) - 1)]
                dates_all = df['date']
                train_mask = dates_all <= train_end
                val_mask = (dates_all > train_end) & (dates_all <= val_end)

                X_train = df.loc[train_mask, feature_cols]
                y_train = df.loc[train_mask, label_col].astype(int).map({-1: 0, 0: 1, 1: 2})
                X_val = df.loc[val_mask, feature_cols]
                y_val = df.loc[val_mask, label_col].astype(int)

                if len(X_val) == 0 or len(X_train) < 50:
                    fold_start += 30
                    continue

                train_data = lgb.Dataset(X_train.fillna(np.nan), label=y_train)
                model = lgb.train(
                    full_params, train_data, num_boost_round=200,
                    callbacks=[lgb.log_evaluation(0)],
                )

                proba = model.predict(X_val.fillna(np.nan))
                preds_mapped = np.argmax(proba, axis=1)
                preds = np.where(preds_mapped == 0, -1, np.where(preds_mapped == 2, 1, 0))

                all_preds.extend(preds)
                all_true.extend(y_val.values)
                fold_start += 30

            if len(all_preds) == 0:
                continue

            all_preds = np.array(all_preds)
            all_true = np.array(all_true)
            dir_mask = all_true != 0
            dir_acc = accuracy_score(all_true[dir_mask], all_preds[dir_mask]) if dir_mask.sum() > 0 else 0
            overall_acc = accuracy_score(all_true, all_preds)

            result = {
                'params': params,
                'overall_accuracy': round(overall_acc * 100, 1),
                'directional_accuracy': round(dir_acc * 100, 1),
            }
            results_log.append(result)
            logger.info(f"  Params: depth={params['max_depth']}, lr={params['learning_rate']} → "
                       f"overall={overall_acc*100:.1f}%, dir={dir_acc*100:.1f}%")

            if dir_acc > best_score:
                best_score = dir_acc
                best_params = full_params

        return {
            'best_params': best_params,
            'best_directional_accuracy': round(best_score * 100, 1),
            'all_results': results_log,
        }


# ── CLI: Run full training pipeline ──────────────────────────

def run_training_pipeline():
    """Run the complete training pipeline and report results."""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

    model = ForecastModelV3()
    df = model.load_dataset()
    print(f"\nLoaded dataset: {len(df)} rows, {df['coin'].nunique()} coins")
    print(f"Date range: {df['date'].min().date()} to {df['date'].max().date()}")

    # ── Step 1: Global walk-forward CV ──────────────────────────
    print("\n" + "=" * 60)
    print("STEP 1: GLOBAL WALK-FORWARD CV")
    print("=" * 60)

    for horizon in ['7d', '3d']:
        print(f"\n--- Horizon: {horizon} ---")

        # LightGBM
        lgb_results = model.walk_forward_cv(df, horizon=horizon)
        if 'error' in lgb_results:
            print(f"  LightGBM: {lgb_results['error']}")
            continue

        print(f"  LightGBM:")
        print(f"    Overall accuracy:     {lgb_results['overall_accuracy']}%")
        print(f"    Directional accuracy: {lgb_results['directional_accuracy']}%")
        print(f"    BUY precision:        {lgb_results['buy_precision']}%")
        print(f"    SELL precision:       {lgb_results['sell_precision']}%")
        print(f"    Predictions: {lgb_results['total_predictions']} "
              f"(BUY={lgb_results['n_buy_predicted']}, SELL={lgb_results['n_sell_predicted']})")
        print(f"    High-conf (>60%):     {lgb_results['high_conf_accuracy']}% "
              f"({lgb_results['high_conf_count']} predictions)")

        # Logistic baseline
        lr_results = model.logistic_baseline_cv(df, horizon=horizon)
        if 'error' not in lr_results:
            print(f"  LogReg baseline:")
            print(f"    Overall accuracy:     {lr_results['overall_accuracy']}%")
            print(f"    Directional accuracy: {lr_results['directional_accuracy']}%")

        model.metrics[f'global_{horizon}'] = lgb_results
        model.metrics[f'global_{horizon}_logreg'] = lr_results

    # ── Step 2: Per-group walk-forward CV ────────────────────────
    print("\n" + "=" * 60)
    print("STEP 2: PER-GROUP WALK-FORWARD CV (7d)")
    print("=" * 60)

    for group in ['majors', 'l1_alts', 'defi', 'ai', 'meme']:
        print(f"\n--- Group: {group} ---")
        results = model.walk_forward_cv(df, horizon='7d', group=group)
        if 'error' in results:
            print(f"  {results['error']}")
            continue
        print(f"  Overall: {results['overall_accuracy']}%, "
              f"Directional: {results['directional_accuracy']}%, "
              f"BUY prec: {results['buy_precision']}%, "
              f"SELL prec: {results['sell_precision']}%")
        print(f"  Preds: {results['total_predictions']} "
              f"(BUY={results['n_buy_predicted']}, SELL={results['n_sell_predicted']})")
        model.metrics[f'{group}_7d'] = results

    # ── Step 3: Hyperparameter search (global) ──────────────────
    print("\n" + "=" * 60)
    print("STEP 3: HYPERPARAMETER SEARCH (global, 7d)")
    print("=" * 60)
    hp_results = model.hyperparameter_search(df, horizon='7d')
    print(f"\nBest directional accuracy: {hp_results['best_directional_accuracy']}%")
    if hp_results['best_params']:
        print(f"Best params: depth={hp_results['best_params'].get('max_depth')}, "
              f"lr={hp_results['best_params'].get('learning_rate')}, "
              f"leaves={hp_results['best_params'].get('num_leaves')}")

    # ── Step 4: Train final models ──────────────────────────────
    print("\n" + "=" * 60)
    print("STEP 4: TRAIN FINAL MODELS")
    print("=" * 60)

    best_params = hp_results.get('best_params')

    # Global model
    model.train_final(df, horizon='7d', best_params=best_params)
    model.train_final(df, horizon='3d', best_params=best_params)
    print("  Trained global 7d + 3d models")

    # Per-group models
    for group in ['majors', 'l1_alts', 'defi', 'ai', 'meme']:
        try:
            model.train_final(df, horizon='7d', group=group, best_params=best_params)
            print(f"  Trained {group} 7d model")
        except Exception as e:
            print(f"  {group} 7d: failed ({e})")

    # ── Step 5: Feature importance ──────────────────────────────
    print("\n" + "=" * 60)
    print("STEP 5: FEATURE IMPORTANCE (global 7d)")
    print("=" * 60)

    fi = model.feature_importance.get('all_7d', {})
    if fi:
        total = sum(fi.values())
        for feat, imp in list(fi.items())[:15]:
            pct = imp / total * 100 if total > 0 else 0
            bar = "#" * int(pct)
            print(f"  {feat:30s}: {pct:5.1f}% {bar}")

    # ── Step 6: Save ─────────────────────────────────────────────
    model.save()
    print("\nModel saved!")

    # ── Summary ──────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("SUMMARY — COMPARISON WITH CURRENT SYSTEM")
    print("=" * 60)
    print("\nCurrent system:")
    print("  BUY accuracy:  0.0% (0/12)")
    print("  SELL accuracy: 50.1%")
    print("  Overall: ~50% directional (coin flip)")
    print("\nForecast v3:")
    g7 = model.metrics.get('global_7d', {})
    if g7 and 'error' not in g7:
        print(f"  Overall accuracy:     {g7.get('overall_accuracy', '?')}%")
        print(f"  Directional accuracy: {g7.get('directional_accuracy', '?')}%")
        print(f"  BUY precision:        {g7.get('buy_precision', '?')}%")
        print(f"  SELL precision:       {g7.get('sell_precision', '?')}%")


if __name__ == '__main__':
    run_training_pipeline()
