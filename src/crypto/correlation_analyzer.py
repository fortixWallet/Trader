"""
FORTIX — Correlation Analyzer
=====================================
Cross-asset correlation analysis from existing price data.

Features:
  - Rolling Pearson correlation matrix (30/90-day)
  - BTC beta for each altcoin (volatility amplification)
  - Correlation regime detection (alt season signal)
  - Sector cohesion analysis

No external API needed — computed from prices table (365 days × 25 coins).
"""

import sys
import sqlite3
import logging
import numpy as np
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger('correlation')

DB_PATH = Path('data/crypto/market.db')


class CorrelationAnalyzer:
    """Cross-asset correlation analysis from existing price data."""

    def __init__(self, conn: sqlite3.Connection = None, coins: list = None):
        self.conn = conn or sqlite3.connect(str(DB_PATH), timeout=60)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=60000")
        self._own_conn = conn is None

        from src.crypto.data_collector import TRACKED_COINS
        self.coins = coins or TRACKED_COINS

        # Cache aligned returns
        self._returns_cache = {}

    def close(self):
        if self._own_conn:
            self.conn.close()

    def _get_daily_closes(self, coin: str, limit: int = 365) -> list:
        """Get daily close prices ordered by timestamp."""
        rows = self.conn.execute(
            "SELECT timestamp, close FROM prices "
            "WHERE coin = ? AND timeframe = '1d' ORDER BY timestamp ASC "
            "LIMIT ?", (coin, limit)
        ).fetchall()
        return rows

    def _get_aligned_returns(self, window: int = 90) -> dict:
        """Get daily returns for all coins, aligned by timestamp.

        Uses pairwise alignment — a coin is included if it shares enough
        timestamps with BTC (reference). This handles coins with different
        data ranges (e.g., SHIB offset by 1 day).

        Returns dict: {coin: np.array of returns} where arrays are aligned
        to BTC's timestamps (the most liquid, always-present reference).
        """
        cache_key = window
        if cache_key in self._returns_cache:
            return self._returns_cache[cache_key]

        # Fetch closes for all coins
        coin_data = {}
        for coin in self.coins:
            rows = self._get_daily_closes(coin, 365)
            if len(rows) >= window + 1:
                recent = rows[-(window + 1):]
                coin_data[coin] = {r[0]: r[1] for r in recent}

        if 'BTC' not in coin_data or len(coin_data) < 2:
            return {}

        # Use BTC timestamps as reference
        btc_ts = sorted(coin_data['BTC'].keys())

        # For each coin, find overlap with BTC timestamps
        aligned = {}
        for coin, data in coin_data.items():
            # Find common timestamps with BTC
            common = sorted(set(btc_ts) & set(data.keys()))
            if len(common) < window // 3:
                continue

            prices = np.array([data[ts] for ts in common], dtype=float)
            if np.any(np.isnan(prices)) or np.any(prices[:-1] == 0):
                continue
            returns = np.diff(prices) / prices[:-1]

            # Store with the common timestamps for pairwise alignment
            aligned[coin] = {'returns': returns, 'timestamps': common[1:]}

        self._returns_cache[cache_key] = aligned
        return aligned

    def _pairwise_corr(self, aligned: dict, coin_a: str, coin_b: str) -> float:
        """Compute correlation between two coins using their overlapping timestamps."""
        if coin_a not in aligned or coin_b not in aligned:
            return float('nan')

        ts_a = set(aligned[coin_a]['timestamps'])
        ts_b = set(aligned[coin_b]['timestamps'])
        common = sorted(ts_a & ts_b)

        if len(common) < 10:
            return float('nan')

        # Map timestamps to indices
        idx_a = {ts: i for i, ts in enumerate(aligned[coin_a]['timestamps'])}
        idx_b = {ts: i for i, ts in enumerate(aligned[coin_b]['timestamps'])}

        ret_a = np.array([aligned[coin_a]['returns'][idx_a[ts]] for ts in common])
        ret_b = np.array([aligned[coin_b]['returns'][idx_b[ts]] for ts in common])

        corr = np.corrcoef(ret_a, ret_b)[0, 1]
        return float(corr) if not np.isnan(corr) else 0.0

    def compute_correlation_matrix(self, window: int = 30) -> dict:
        """Rolling Pearson correlation between all coins.

        Returns:
            {
                'matrix': {(coin_a, coin_b): corr_value, ...},
                'coins': [list of coins with data],
                'avg_correlation': float,
            }
        """
        aligned = self._get_aligned_returns(window)
        if len(aligned) < 2:
            return {'matrix': {}, 'coins': [], 'avg_correlation': 0.0}

        coins_with_data = sorted(aligned.keys())
        matrix = {}
        all_corrs = []

        for i, coin_a in enumerate(coins_with_data):
            for j, coin_b in enumerate(coins_with_data):
                if j <= i:
                    continue
                corr = self._pairwise_corr(aligned, coin_a, coin_b)
                if not np.isnan(corr):
                    matrix[(coin_a, coin_b)] = round(corr, 3)
                    all_corrs.append(corr)

        avg_corr = float(np.mean(all_corrs)) if all_corrs else 0.0

        return {
            'matrix': matrix,
            'coins': coins_with_data,
            'avg_correlation': round(avg_corr, 3),
        }

    def compute_btc_beta(self, window: int = 90) -> dict:
        """Calculate beta to BTC for each altcoin.

        beta = cov(alt_returns, btc_returns) / var(btc_returns)

        Beta > 1 = amplifies BTC moves (risky)
        Beta < 1 = defensive asset
        Beta < 0 = inverse to BTC (rare)

        Returns:
            {'ETH': 1.2, 'SOL': 1.8, 'BNB': 0.9, ...}
        """
        aligned = self._get_aligned_returns(window)
        if 'BTC' not in aligned:
            return {}

        btc_data = aligned['BTC']
        btc_ts_set = set(btc_data['timestamps'])

        betas = {}
        for coin, data in aligned.items():
            if coin == 'BTC':
                betas[coin] = 1.0
                continue

            # Find overlapping timestamps
            common = sorted(btc_ts_set & set(data['timestamps']))
            if len(common) < 15:
                continue

            idx_btc = {ts: i for i, ts in enumerate(btc_data['timestamps'])}
            idx_coin = {ts: i for i, ts in enumerate(data['timestamps'])}

            btc_ret = np.array([btc_data['returns'][idx_btc[ts]] for ts in common])
            coin_ret = np.array([data['returns'][idx_coin[ts]] for ts in common])

            btc_var = np.var(btc_ret)
            if btc_var == 0:
                continue

            cov = np.cov(coin_ret, btc_ret)[0, 1]
            beta = cov / btc_var
            betas[coin] = round(float(beta), 2)

        return betas

    def detect_correlation_regime(self, short_window: int = 30,
                                   long_window: int = 90) -> dict:
        """Compare short-term vs long-term average correlations.

        Dropping correlation = potential alt season (alts moving independently)
        Rising correlation = risk-off (everything moves with BTC)

        Returns:
            {
                'avg_corr_short': 0.65,
                'avg_corr_long': 0.78,
                'trend': 'dropping' | 'rising' | 'stable',
                'delta': -0.13,
                'interpretation': 'Correlations dropping: potential alt season signal'
            }
        """
        short_data = self.compute_correlation_matrix(short_window)
        long_data = self.compute_correlation_matrix(long_window)

        avg_short = short_data.get('avg_correlation', 0)
        avg_long = long_data.get('avg_correlation', 0)
        delta = avg_short - avg_long

        if delta < -0.1:
            trend = 'dropping'
            interpretation = (
                f"Correlations dropping ({avg_short:.2f} vs {avg_long:.2f} 90D avg). "
                f"Alts decoupling from BTC — potential alt season signal."
            )
        elif delta > 0.1:
            trend = 'rising'
            interpretation = (
                f"Correlations rising ({avg_short:.2f} vs {avg_long:.2f} 90D avg). "
                f"Risk-off: everything moving with BTC."
            )
        else:
            trend = 'stable'
            interpretation = (
                f"Correlations stable ({avg_short:.2f} vs {avg_long:.2f} 90D avg). "
                f"No clear regime shift."
            )

        return {
            'avg_corr_short': round(avg_short, 3),
            'avg_corr_long': round(avg_long, 3),
            'trend': trend,
            'delta': round(float(delta), 3),
            'interpretation': interpretation,
        }

    def sector_correlation(self, window: int = 30) -> dict:
        """Measure intra-sector cohesion.

        High intra-sector correlation = sector moves as unit (narrative-driven)
        Low intra-sector correlation = sector fragmenting

        Returns:
            {
                'L1': {'cohesion': 0.82, 'interpretation': 'Strong'},
                'DeFi': {'cohesion': 0.45, 'interpretation': 'Fragmenting'},
                ...
            }
        """
        from src.crypto.forecast_engine import COIN_SECTORS

        aligned = self._get_aligned_returns(window)
        if len(aligned) < 2:
            return {}

        sector_results = {}
        for sector, coins in COIN_SECTORS.items():
            sector_coins = [c for c in coins if c in aligned]
            if len(sector_coins) < 2:
                sector_results[sector] = {
                    'cohesion': 0.0,
                    'n_coins': len(sector_coins),
                    'interpretation': 'Insufficient data',
                }
                continue

            # Compute average pairwise correlation within sector
            corrs = []
            for i, a in enumerate(sector_coins):
                for j, b in enumerate(sector_coins):
                    if j <= i:
                        continue
                    corr = self._pairwise_corr(aligned, a, b)
                    if not np.isnan(corr):
                        corrs.append(corr)

            avg_corr = float(np.mean(corrs)) if corrs else 0.0

            if avg_corr > 0.7:
                interp = 'Strong cohesion — sector moves as a unit'
            elif avg_corr > 0.4:
                interp = 'Moderate cohesion'
            elif avg_corr > 0.1:
                interp = 'Weak — sector fragmenting'
            else:
                interp = 'No cohesion — individual coin drivers'

            sector_results[sector] = {
                'cohesion': round(avg_corr, 3),
                'n_coins': len(sector_coins),
                'interpretation': interp,
            }

        return sector_results

    def detect_correlation_breaks(self, short_window: int = 7,
                                   long_window: int = 30,
                                   threshold: float = 0.3) -> list:
        """Detect coins that have broken correlation with BTC.

        Compares each coin's 7-day rolling correlation vs 30-day average
        correlation with BTC. A drop > threshold indicates decoupling.

        Args:
            short_window: recent window (default 7 days)
            long_window: baseline window (default 30 days)
            threshold: minimum correlation drop to trigger alert (default 0.3)

        Returns:
            List of dicts: [{coin, corr_7d, corr_30d, drop, historical_context}, ...]
        """
        aligned_short = self._get_aligned_returns(short_window)
        aligned_long = self._get_aligned_returns(long_window)

        if 'BTC' not in aligned_short or 'BTC' not in aligned_long:
            return []

        breaks = []
        for coin in self.coins:
            if coin == 'BTC':
                continue

            corr_short = self._pairwise_corr(aligned_short, coin, 'BTC')
            corr_long = self._pairwise_corr(aligned_long, coin, 'BTC')

            if np.isnan(corr_short) or np.isnan(corr_long):
                continue

            drop = corr_short - corr_long
            if drop < -threshold:
                # Look for historical precedent of correlation breaks
                historical_ctx = self._find_historical_break_context(coin, corr_short)

                breaks.append({
                    'coin': coin,
                    'corr_7d': round(float(corr_short), 2),
                    'corr_30d': round(float(corr_long), 2),
                    'drop': round(float(drop), 2),
                    'historical_context': historical_ctx,
                })

        # Sort by magnitude of drop (most dramatic first)
        breaks.sort(key=lambda x: x['drop'])
        return breaks

    def _find_historical_break_context(self, coin: str, current_corr: float) -> str:
        """Look up past correlation breaks for this coin and what happened after.

        Searches price history for periods where correlation with BTC was
        similarly low, then checks what the coin did in the following 14 days.
        """
        try:
            # Get 180 days of daily closes for both coin and BTC
            coin_rows = self._get_daily_closes(coin, 365)
            btc_rows = self._get_daily_closes('BTC', 365)

            if len(coin_rows) < 60 or len(btc_rows) < 60:
                return ""

            # Build aligned data
            coin_map = {r[0]: r[1] for r in coin_rows}
            btc_map = {r[0]: r[1] for r in btc_rows}
            common_ts = sorted(set(coin_map.keys()) & set(btc_map.keys()))

            if len(common_ts) < 60:
                return ""

            coin_prices = np.array([coin_map[ts] for ts in common_ts], dtype=float)
            btc_prices = np.array([btc_map[ts] for ts in common_ts], dtype=float)

            coin_returns = np.diff(coin_prices) / coin_prices[:-1]
            btc_returns = np.diff(btc_prices) / btc_prices[:-1]

            # Rolling 7-day correlation across history
            # Look for past periods with similarly low correlation
            window = 7
            past_breaks = []
            for i in range(window, len(coin_returns) - 14):
                cr = coin_returns[i - window:i]
                br = btc_returns[i - window:i]
                if len(cr) >= window and len(br) >= window:
                    c = np.corrcoef(cr, br)[0, 1]
                    if not np.isnan(c) and c <= current_corr + 0.05:
                        # What happened to coin in next 14 days?
                        if i + 14 < len(coin_prices):
                            future_return = (coin_prices[i + 14] - coin_prices[i]) / coin_prices[i] * 100
                            past_breaks.append({
                                'index': i,
                                'corr': c,
                                'future_14d': future_return,
                            })

            if not past_breaks:
                return ""

            # Summarize: average outcome after similar breaks
            avg_return = np.mean([pb['future_14d'] for pb in past_breaks])
            positive_pct = sum(1 for pb in past_breaks if pb['future_14d'] > 0) / len(past_breaks) * 100

            if avg_return > 5:
                return (f"Past similar breaks ({len(past_breaks)} instances): "
                        f"avg {avg_return:+.1f}% in 14 days ({positive_pct:.0f}% positive). "
                        f"Decoupling historically led to independent rally.")
            elif avg_return < -5:
                return (f"Past similar breaks ({len(past_breaks)} instances): "
                        f"avg {avg_return:+.1f}% in 14 days ({positive_pct:.0f}% positive). "
                        f"Decoupling historically preceded independent sell-off.")
            else:
                return (f"Past similar breaks ({len(past_breaks)} instances): "
                        f"avg {avg_return:+.1f}% in 14 days ({positive_pct:.0f}% positive). "
                        f"Mixed outcomes after decoupling.")

        except Exception as e:
            log.debug(f"Historical break context failed for {coin}: {e}")
            return ""

    def compute_btc_correlations(self, window: int = 7) -> dict:
        """Compute each coin's rolling correlation with BTC for a given window.

        Returns:
            {coin: correlation_value, ...}
        """
        aligned = self._get_aligned_returns(window)
        if 'BTC' not in aligned:
            return {}

        correlations = {}
        for coin in self.coins:
            if coin == 'BTC':
                correlations['BTC'] = 1.0
                continue
            corr = self._pairwise_corr(aligned, coin, 'BTC')
            if not np.isnan(corr):
                correlations[coin] = round(float(corr), 3)

        return correlations

    def analyze_all(self) -> dict:
        """Run full correlation analysis. Returns combined results."""
        log.info("Running correlation analysis...")

        matrix = self.compute_correlation_matrix(30)
        betas = self.compute_btc_beta(90)
        regime = self.detect_correlation_regime(30, 90)
        sectors = self.sector_correlation(30)

        # Find most/least correlated pairs
        sorted_pairs = sorted(matrix['matrix'].items(), key=lambda x: x[1], reverse=True)
        most_correlated = sorted_pairs[:5] if sorted_pairs else []
        least_correlated = sorted_pairs[-5:] if len(sorted_pairs) >= 5 else []

        # Find highest/lowest beta coins
        sorted_betas = sorted(betas.items(), key=lambda x: x[1], reverse=True)
        highest_beta = sorted_betas[:3] if sorted_betas else []
        lowest_beta = [b for b in sorted_betas if b[0] != 'BTC'][-3:] if len(sorted_betas) > 3 else []

        # NEW: Correlation break detection (7d vs 30d)
        correlation_breaks = self.detect_correlation_breaks(7, 30, threshold=0.3)
        if correlation_breaks:
            log.info(f"  Correlation breaks detected: {', '.join(b['coin'] for b in correlation_breaks)}")

        # NEW: Current 7d BTC correlations for ranking
        btc_correlations_7d = self.compute_btc_correlations(7)

        result = {
            'matrix': matrix,
            'btc_betas': betas,
            'regime': regime,
            'sector_cohesion': sectors,
            'most_correlated': most_correlated,
            'least_correlated': least_correlated,
            'highest_beta': highest_beta,
            'lowest_beta': lowest_beta,
            'correlation_breaks': correlation_breaks,
            'btc_correlations_7d': btc_correlations_7d,
        }

        log.info(f"  Correlation regime: {regime['trend']} (30D: {regime['avg_corr_short']}, 90D: {regime['avg_corr_long']})")
        log.info(f"  BTC betas computed for {len(betas)} coins")
        log.info(f"  Sector cohesion computed for {len(sectors)} sectors")

        return result


if __name__ == '__main__':
    log.info("=" * 60)
    log.info("ALPHA SIGNAL — Correlation Analysis")
    log.info("=" * 60)

    analyzer = CorrelationAnalyzer()
    result = analyzer.analyze_all()

    # Display BTC betas
    print(f"\n{'Coin':<8} {'Beta':>6}  Interpretation")
    print("-" * 40)
    for coin, beta in sorted(result['btc_betas'].items(), key=lambda x: x[1], reverse=True):
        if beta > 1.5:
            interp = "HIGH RISK — amplifies BTC moves"
        elif beta > 1.0:
            interp = "Moderate amplification"
        elif beta > 0.5:
            interp = "Defensive"
        else:
            interp = "Very defensive / uncorrelated"
        print(f"{coin:<8} {beta:>5.2f}x  {interp}")

    # Regime
    print(f"\nCorrelation Regime: {result['regime']['interpretation']}")

    # Sector cohesion
    print(f"\nSector Cohesion (30D):")
    for sector, data in result['sector_cohesion'].items():
        print(f"  {sector:<8} {data['cohesion']:.3f}  ({data['interpretation']})")

    # Most/least correlated pairs
    if result['most_correlated']:
        print(f"\nMost Correlated Pairs:")
        for (a, b), corr in result['most_correlated']:
            print(f"  {a}-{b}: {corr:.3f}")

    if result['least_correlated']:
        print(f"\nLeast Correlated Pairs:")
        for (a, b), corr in result['least_correlated']:
            print(f"  {a}-{b}: {corr:.3f}")

    analyzer.close()
