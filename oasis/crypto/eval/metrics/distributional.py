"""Tier B -- Distributional / style-fact metrics.

Check that the sim produces realistic statistical properties:
fat tails, volatility clustering, green/red ratio.
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd

from oasis.crypto.eval import MetricResult


def _safe_returns(prices: pd.Series) -> np.ndarray:
    r = prices.pct_change().dropna().values
    return r if len(r) > 0 else np.array([0.0])


def return_kurtosis(sim_prices: pd.Series) -> MetricResult:
    """Excess kurtosis of sim returns (should be in [4, 20] for crypto)."""
    r = _safe_returns(sim_prices)
    if len(r) < 4:
        return MetricResult(
            name="return_kurtosis", value=float("nan"),
            unit="ratio", direction="match_target", notes="too few returns",
        )
    from scipy.stats import kurtosis as _kurt

    val = float(_kurt(r, fisher=True))
    if math.isnan(val):
        val = 0.0
    passed = 4.0 <= val <= 20.0
    return MetricResult(
        name="return_kurtosis", value=val, unit="ratio",
        direction="match_target", threshold=4.0,
        passed=passed, notes="target [4, 20]",
    )


def vol_clustering_acf(
    sim_prices: pd.Series, lag: int = 1,
) -> MetricResult:
    """ACF of |returns| at the given lag. Positive = volatility clustering."""
    r = _safe_returns(sim_prices)
    abs_r = np.abs(r)
    if len(abs_r) <= lag + 1:
        return MetricResult(
            name=f"vol_clustering_acf_lag{lag}", value=float("nan"),
            unit="ratio", direction="higher_better", notes="too short",
        )
    mean = abs_r.mean()
    var = abs_r.var()
    if var == 0:
        return MetricResult(
            name=f"vol_clustering_acf_lag{lag}", value=0.0,
            unit="ratio", direction="higher_better", notes="zero variance",
        )
    n = len(abs_r)
    cov = np.sum((abs_r[: n - lag] - mean) * (abs_r[lag:] - mean)) / n
    val = float(cov / var)
    threshold = 0.15 if lag == 1 else 0.05
    return MetricResult(
        name=f"vol_clustering_acf_lag{lag}", value=val, unit="ratio",
        direction="higher_better", threshold=threshold,
        passed=val >= threshold,
    )


def realized_vol(sim_prices: pd.Series) -> MetricResult:
    """Annualized realized volatility of sim returns.

    Returns the value; comparison to real vol is done at the scoring layer.
    """
    r = _safe_returns(sim_prices)
    if len(r) < 2:
        return MetricResult(
            name="realized_vol", value=float("nan"),
            unit="ratio", direction="match_target", notes="too short",
        )
    # Annualize assuming 1-min steps -> 525600 per year
    val = float(np.std(r) * np.sqrt(525_600))
    return MetricResult(
        name="realized_vol", value=val, unit="ratio",
        direction="match_target",
    )


def green_red_ratio(sim_prices: pd.Series) -> MetricResult:
    """Fraction of positive-return steps (green candles).

    Crypto markets historically ~0.52.
    """
    r = _safe_returns(sim_prices)
    if len(r) == 0:
        return MetricResult(
            name="green_red_ratio", value=float("nan"),
            unit="ratio", direction="match_target", notes="no returns",
        )
    greens = (r > 0).sum()
    val = float(greens / len(r))
    passed = abs(val - 0.52) <= 0.05
    return MetricResult(
        name="green_red_ratio", value=val, unit="ratio",
        direction="match_target", threshold=0.52,
        passed=passed, notes="target ~0.52 +/- 0.05",
    )
