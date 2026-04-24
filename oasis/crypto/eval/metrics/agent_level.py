"""Tier F -- Agent-level metrics.

Per-archetype action distribution, Gini coefficient, conservation check.
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd

from oasis.crypto.eval import MetricResult


def action_distribution_per_archetype(
    actions_df: pd.DataFrame,
) -> dict[str, MetricResult]:
    """Per-archetype distribution of action types.

    Returns one MetricResult per archetype. The 'value' is the entropy
    of the action-type distribution (higher = more diverse actions).
    """
    if actions_df.empty:
        return {}
    needed = {"archetype", "action_type"}
    if not needed.issubset(actions_df.columns):
        return {}
    results: dict[str, MetricResult] = {}
    for arch, grp in actions_df.groupby("archetype"):
        counts = grp["action_type"].value_counts()
        probs = counts.values / counts.values.sum()
        entropy = float(-np.sum(probs * np.log2(probs + 1e-12)))
        results[str(arch)] = MetricResult(
            name=f"action_dist_{arch}", value=entropy,
            unit="bits", direction="match_target",
            notes=f"counts: {dict(counts)}",
        )
    return results


def gini_wealth(balances_df: pd.DataFrame) -> MetricResult:
    """Gini coefficient of total wealth across agents.

    balances_df needs columns: user_id, amount (total USD-equivalent wealth).
    Target: [0.55, 0.85] for crypto-like concentration.
    """
    if balances_df.empty or "amount" not in balances_df.columns:
        return MetricResult(
            name="gini_wealth", value=float("nan"),
            unit="ratio", direction="match_target", notes="no data",
        )
    # Aggregate per user
    if "user_id" in balances_df.columns:
        wealth = balances_df.groupby("user_id")["amount"].sum().values
    else:
        wealth = balances_df["amount"].values
    wealth = np.sort(wealth.astype(float))
    n = len(wealth)
    if n == 0 or wealth.sum() == 0:
        return MetricResult(
            name="gini_wealth", value=float("nan"),
            unit="ratio", direction="match_target", notes="zero wealth",
        )
    index = np.arange(1, n + 1)
    val = float((2 * np.sum(index * wealth) / (n * np.sum(wealth))) - (n + 1) / n)
    val = max(0.0, min(1.0, val))  # clamp
    passed = 0.55 <= val <= 0.85
    return MetricResult(
        name="gini_wealth", value=val, unit="ratio",
        direction="match_target", threshold=0.55,
        passed=passed, notes="target [0.55, 0.85]",
    )


def conservation_check(conservation_df: pd.DataFrame) -> MetricResult:
    """Check that total supply is conserved across all instruments.

    conservation_df from conservation.parquet with columns:
    step, instrument, total_amount, total_locked, total_supply.
    """
    if conservation_df.empty:
        return MetricResult(
            name="conservation_check", value=float("nan"),
            unit="ratio", direction="lower_better", notes="no data",
        )
    needed = {"total_amount", "total_supply"}
    if not needed.issubset(conservation_df.columns):
        return MetricResult(
            name="conservation_check", value=float("nan"),
            unit="ratio", direction="lower_better", notes="missing cols",
        )
    drifts = np.abs(
        conservation_df["total_amount"].values
        - conservation_df["total_supply"].values
    )
    max_drift = float(np.nanmax(drifts)) if len(drifts) > 0 else 0.0
    if math.isnan(max_drift):
        max_drift = 0.0
    passed = max_drift <= 1e-6
    return MetricResult(
        name="conservation_check", value=max_drift, unit="ratio",
        direction="lower_better", threshold=1e-6,
        passed=passed,
    )
