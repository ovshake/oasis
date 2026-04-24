"""Evaluation framework for crypto exchange simulations.

Scores sim output against ground truth and baselines across 6 metric tiers.
"""

from __future__ import annotations

from pydantic import BaseModel


class MetricResult(BaseModel):
    """Universal return type for every metric function."""

    name: str
    value: float  # the metric value (may be NaN)
    unit: str  # "ratio", "bps", "count", "steps", etc.
    direction: str  # "higher_better" | "lower_better" | "match_target"
    baseline_value: float | None = None
    passed: bool | None = None
    threshold: float | None = None
    notes: str | None = None
