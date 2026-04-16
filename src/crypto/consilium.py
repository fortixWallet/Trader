"""
CONSILIUM — Independent Multi-Expert Voting System
===================================================
Each expert votes INDEPENDENTLY (sealed envelope).
Final decision based on consensus.

Experts:
1. LightGBM (numbers)
2. XGBoost (numbers, different algorithm)
3. Neural Net (numbers, different architecture)
4. Market Breadth (market reality)
5. News Reactor (real-time events)
6. Profi (Claude Opus — charts + context + knowledge)

Rules:
- Each expert gives: LONG/SHORT/WAIT + confidence
- Trade ONLY if Profi agrees AND at least 2/3 ML models agree
- Profi has VETO power (if Profi says WAIT → no trade regardless)
- Position size proportional to consensus strength
"""

import logging
from typing import Dict, List, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class Vote:
    expert: str
    direction: str  # LONG, SHORT, WAIT
    confidence: float  # 0.0 - 1.0
    reason: str = ""
    details: dict = None


@dataclass
class Decision:
    action: str  # LONG, SHORT, SKIP
    coin: str
    confidence: float
    votes: List[Vote]
    n_agree: int
    n_total: int
    profi_agrees: bool
    entry: float = 0
    tp: float = 0
    sl: float = 0
    size_factor: float = 1.0  # 0.0-1.0 based on consensus


class Consilium:
    """Collects independent votes and makes final decision."""

    def decide(self, coin: str, votes: List[Vote]) -> Decision:
        """
        Collect sealed envelopes, open, compare, decide.

        Rules:
        1. Profi has VETO — if Profi says WAIT → SKIP
        2. Need at least 3/n_total votes in same direction
        3. Profi + 2 ML models must agree
        4. Size proportional to consensus
        """
        if not votes:
            return Decision('SKIP', coin, 0, [], 0, 0, False)

        # Separate votes
        profi_vote = None
        ml_votes = []
        other_votes = []

        for v in votes:
            if v.expert == 'profi':
                profi_vote = v
            elif v.expert in ('lightgbm', 'xgboost', 'neural_net'):
                ml_votes.append(v)
            else:
                other_votes.append(v)

        all_votes = votes
        n_total = len(all_votes)

        # Rule 1: Profi VETO
        if profi_vote and profi_vote.direction == 'WAIT':
            return Decision('SKIP', coin, 0, all_votes, 0, n_total, False,
                          size_factor=0)

        # Count directions (excluding WAIT)
        long_votes = [v for v in all_votes if v.direction == 'LONG']
        short_votes = [v for v in all_votes if v.direction == 'SHORT']
        wait_votes = [v for v in all_votes if v.direction == 'WAIT']

        # Determine majority
        if len(long_votes) > len(short_votes):
            direction = 'LONG'
            agree_votes = long_votes
        elif len(short_votes) > len(long_votes):
            direction = 'SHORT'
            agree_votes = short_votes
        else:
            return Decision('SKIP', coin, 0, all_votes, 0, n_total, False)

        n_agree = len(agree_votes)

        # Rule 2: Need at least 3 votes in same direction
        if n_agree < 3:
            return Decision('SKIP', coin, 0, all_votes, n_agree, n_total, False)

        # Rule 3: Profi must agree with majority
        profi_agrees = profi_vote and profi_vote.direction == direction
        if not profi_agrees:
            return Decision('SKIP', coin, 0, all_votes, n_agree, n_total, False)

        # Rule 4: At least 2/3 ML models must agree
        ml_agree = sum(1 for v in ml_votes if v.direction == direction)
        if ml_agree < 2:
            return Decision('SKIP', coin, 0, all_votes, n_agree, n_total, profi_agrees)

        # Calculate confidence
        avg_confidence = sum(v.confidence for v in agree_votes) / len(agree_votes)
        consensus_strength = n_agree / n_total

        # Size factor: more consensus → bigger position
        size_factor = min(1.0, consensus_strength * avg_confidence * 2)

        # Get Profi's entry/tp/sl if available
        entry = tp = sl = 0
        if profi_vote and profi_vote.details:
            entry = profi_vote.details.get('entry', 0)
            tp = profi_vote.details.get('tp', 0)
            sl = profi_vote.details.get('sl', 0)

        logger.info(f"CONSILIUM {coin}: {direction} ({n_agree}/{n_total} agree, "
                    f"confidence={avg_confidence:.0%}, size={size_factor:.0%})")

        return Decision(
            action=direction,
            coin=coin,
            confidence=avg_confidence,
            votes=all_votes,
            n_agree=n_agree,
            n_total=n_total,
            profi_agrees=True,
            entry=entry, tp=tp, sl=sl,
            size_factor=size_factor
        )

    def format_report(self, decision: Decision) -> str:
        """Human-readable consilium report."""
        lines = [f"CONSILIUM: {decision.coin} → {decision.action}"]
        lines.append(f"Consensus: {decision.n_agree}/{decision.n_total} "
                    f"({decision.confidence:.0%} confidence)")
        lines.append(f"Size factor: {decision.size_factor:.0%}")

        for v in decision.votes:
            emoji = '✅' if v.direction == decision.action else ('⏸️' if v.direction == 'WAIT' else '❌')
            lines.append(f"  {emoji} {v.expert:12s}: {v.direction:5s} ({v.confidence:.0%}) — {v.reason[:40]}")

        if decision.entry:
            lines.append(f"\nEntry: ${decision.entry:.4f}")
            lines.append(f"TP:    ${decision.tp:.4f}")
            lines.append(f"SL:    ${decision.sl:.4f}")

        return '\n'.join(lines)
