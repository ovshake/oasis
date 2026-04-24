"""Tier D -- Cross-asset metrics.

Correlation matrices, risk-on/off sign agreement.
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd

from oasis.crypto.eval import MetricResult


def correlation_matrix(
    prices_df: pd.DataFrame, assets: list[str],
) -> pd.DataFrame:
    """Compute pairwise Pearson correlation of returns for *assets*.

    *prices_df* must have columns named after each asset (price series).
    Returns an NxN DataFrame indexed and columned by asset.
    """
    returns = prices_df[assets].pct_change().dropna()
    if returns.empty:
        return pd.DataFrame(
            np.nan, index=assets, columns=assets,
        )
    return returns.corr()


def correlation_frobenius_distance(
    sim_corr: pd.DataFrame, real_corr: pd.DataFrame,
) -> MetricResult:
    """Frobenius norm of the difference between two correlation matrices."""
    if sim_corr.empty or real_corr.empty:
        return MetricResult(
            name="correlation_frobenius_distance", value=float("nan"),
            unit="ratio", direction="lower_better", notes="empty matrix",
        )
    # Align to the same assets
    common = sorted(set(sim_corr.columns) & set(real_corr.columns))
    if len(common) < 2:
        return MetricResult(
            name="correlation_frobenius_distance", value=float("nan"),
            unit="ratio", direction="lower_better",
            notes="<2 common assets",
        )
    s = sim_corr.loc[common, common].values.astype(float)
    r = real_corr.loc[common, common].values.astype(float)
    # Replace NaN with 0 for distance computation
    s = np.nan_to_num(s, nan=0.0)
    r = np.nan_to_num(r, nan=0.0)
    val = float(np.linalg.norm(s - r, "fro"))
    return MetricResult(
        name="correlation_frobenius_distance", value=val, unit="ratio",
        direction="lower_better", threshold=1.0,
        passed=val <= 1.0,
    )
