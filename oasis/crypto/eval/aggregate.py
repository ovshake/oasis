"""Multi-seed aggregation with bootstrap confidence intervals."""

from __future__ import annotations

import math

import numpy as np

from oasis.crypto.eval import MetricResult


def bootstrap_ci(
    values: list[float] | np.ndarray,
    alpha: float = 0.05,
    n_resamples: int = 1000,
    seed: int = 42,
) -> tuple[float, float]:
    """Non-parametric bootstrap confidence interval.

    Returns (lower, upper) bounds at (1-alpha)*100% confidence.
    """
    arr = np.array([v for v in values if not math.isnan(v)], dtype=float)
    if len(arr) == 0:
        return (float("nan"), float("nan"))
    if len(arr) == 1:
        return (float(arr[0]), float(arr[0]))

    rng = np.random.default_rng(seed)
    boot_means = np.empty(n_resamples)
    for i in range(n_resamples):
        sample = rng.choice(arr, size=len(arr), replace=True)
        boot_means[i] = sample.mean()

    lo = float(np.percentile(boot_means, 100 * alpha / 2))
    hi = float(np.percentile(boot_means, 100 * (1 - alpha / 2)))
    return (lo, hi)


def aggregate_metrics(
    metric_results: list[list[MetricResult]],
) -> list[MetricResult]:
    """Aggregate metric results across N seeds.

    *metric_results*: list of N lists, each containing the same metrics
    (same names, same order). Returns one list with mean +/- 95% CI.

    Metrics are matched by name across seeds.
    """
    if not metric_results:
        return []

    # Group by metric name across seeds
    by_name: dict[str, list[MetricResult]] = {}
    for seed_results in metric_results:
        for m in seed_results:
            by_name.setdefault(m.name, []).append(m)

    aggregated: list[MetricResult] = []
    for name, ms in by_name.items():
        values = [m.value for m in ms if not math.isnan(m.value)]
        if not values:
            aggregated.append(MetricResult(
                name=name, value=float("nan"),
                unit=ms[0].unit, direction=ms[0].direction,
                threshold=ms[0].threshold,
                notes=f"all {len(ms)} seeds NaN",
            ))
            continue

        mean_val = float(np.mean(values))
        std_val = float(np.std(values))
        lo, hi = bootstrap_ci(values)

        # Determine pass/fail using mean against threshold
        passed = None
        if ms[0].threshold is not None and not math.isnan(mean_val):
            if ms[0].direction == "higher_better":
                passed = mean_val >= ms[0].threshold
            elif ms[0].direction == "lower_better":
                passed = mean_val <= ms[0].threshold
            elif ms[0].direction == "match_target":
                passed = ms[0].passed  # defer to individual

        aggregated.append(MetricResult(
            name=name, value=mean_val,
            unit=ms[0].unit, direction=ms[0].direction,
            threshold=ms[0].threshold,
            passed=passed,
            baseline_value=ms[0].baseline_value,
            notes=f"N={len(values)}, std={std_val:.4f}, 95%CI=[{lo:.4f}, {hi:.4f}]",
        ))

    return aggregated
