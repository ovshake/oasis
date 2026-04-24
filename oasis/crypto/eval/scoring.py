"""Scoring: per-tier composite and overall score vector."""

from __future__ import annotations

import math

from oasis.crypto.eval import MetricResult

# Tier name constants
TIERS = [
    "price_path", "style_facts", "microstructure",
    "cross_asset", "social", "agent_level",
]

# Metric-to-tier mapping
_TIER_MAP: dict[str, str] = {
    "direction_match_pct": "price_path",
    "peak_drawdown_error": "price_path",
    "drawdown_timing_error": "price_path",
    "path_correlation": "price_path",
    "terminal_price_error": "price_path",
    "return_kurtosis": "style_facts",
    "vol_clustering_acf_lag1": "style_facts",
    "realized_vol": "style_facts",
    "green_red_ratio": "style_facts",
    "active_agent_rate": "microstructure",
    "trade_size_distribution": "microstructure",
    "correlation_frobenius_distance": "cross_asset",
    "post_volume_around_news": "social",
    "sentiment_price_correlation": "social",
    "gini_wealth": "agent_level",
    "conservation_check": "agent_level",
}


def score_tier(metrics: list[MetricResult]) -> float:
    """Composite 0-1 score for a tier.

    Scoring rules:
    - Metrics with passed=True contribute 1.0
    - Metrics with passed=False contribute a partial score based on
      how close the value is to the threshold
    - Metrics with passed=None (no threshold) contribute 0.5
    - NaN values contribute 0.0

    Returns the average across all metrics in the tier.
    """
    if not metrics:
        return 0.0

    scores: list[float] = []
    for m in metrics:
        if math.isnan(m.value):
            scores.append(0.0)
            continue
        if m.passed is True:
            scores.append(1.0)
        elif m.passed is False:
            # Partial credit based on distance to threshold
            scores.append(_partial_score(m))
        else:
            # No threshold defined -- neutral
            scores.append(0.5)

    return sum(scores) / len(scores) if scores else 0.0


def _partial_score(m: MetricResult) -> float:
    """Compute partial credit for a metric that didn't pass."""
    if m.threshold is None or math.isnan(m.value):
        return 0.0

    if m.direction == "higher_better":
        # value < threshold; credit = value/threshold (clamped)
        if m.threshold == 0:
            return 0.0
        return max(0.0, min(0.9, m.value / m.threshold))

    if m.direction == "lower_better":
        # value > threshold; credit = threshold/value (clamped)
        if m.value == 0:
            return 0.9
        return max(0.0, min(0.9, m.threshold / m.value))

    # match_target: no partial credit scheme
    return 0.25


def score_vector(all_metrics: list[MetricResult]) -> dict[str, float]:
    """Compute the full score vector from all metrics.

    Returns a dict with keys matching TIERS + baseline comparisons.
    """
    # Group by tier
    by_tier: dict[str, list[MetricResult]] = {t: [] for t in TIERS}
    for m in all_metrics:
        tier = _TIER_MAP.get(m.name)
        if tier and tier in by_tier:
            by_tier[tier].append(m)
        else:
            # Check for archetype-specific action_dist metrics
            if m.name.startswith("action_dist_"):
                by_tier["agent_level"].append(m)

    scores: dict[str, float] = {}
    for tier_name in TIERS:
        scores[tier_name] = score_tier(by_tier[tier_name])

    # Placeholder for baseline comparison scores
    # These would be computed by comparing sim metrics vs baseline metrics
    scores["vs_random_walk"] = 0.0
    scores["vs_no_news"] = 0.0

    return scores


def update_baseline_scores(
    scores: dict[str, float],
    sim_metrics: list[MetricResult],
    baseline_metrics: list[MetricResult],
    baseline_name: str,
) -> None:
    """Update score vector with baseline comparison.

    Computes the fraction of metrics where sim beats the baseline.
    """
    key = f"vs_{baseline_name}"
    if key not in scores:
        return

    wins = 0
    total = 0
    for sm in sim_metrics:
        bm = next((b for b in baseline_metrics if b.name == sm.name), None)
        if bm is None or math.isnan(sm.value) or math.isnan(bm.value):
            continue
        total += 1
        if sm.direction == "higher_better":
            if sm.value > bm.value:
                wins += 1
        elif sm.direction == "lower_better":
            if sm.value < bm.value:
                wins += 1

    scores[key] = wins / total if total > 0 else 0.0
