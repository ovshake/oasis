"""Tier C -- Microstructure metrics.

Active-agent rates, trade size distribution.
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd

from oasis.crypto.eval import MetricResult


def active_agent_rate(actions_df: pd.DataFrame) -> MetricResult:
    """Non-silent fraction of agents per step, averaged across steps.

    Expects columns: step, tier (with 'silent' for idle agents).
    """
    if actions_df.empty or "step" not in actions_df.columns:
        return MetricResult(
            name="active_agent_rate", value=float("nan"),
            unit="ratio", direction="match_target", notes="no actions data",
        )
    if "tier" not in actions_df.columns:
        return MetricResult(
            name="active_agent_rate", value=float("nan"),
            unit="ratio", direction="match_target", notes="no tier column",
        )
    grouped = actions_df.groupby("step")
    rates: list[float] = []
    for _step, grp in grouped:
        total = len(grp)
        active = (grp["tier"] != "silent").sum()
        rates.append(active / total if total > 0 else 0.0)
    val = float(np.mean(rates)) if rates else float("nan")
    return MetricResult(
        name="active_agent_rate", value=val, unit="ratio",
        direction="match_target",
    )


def trade_size_distribution(trades_df: pd.DataFrame) -> MetricResult:
    """Skewness + kurtosis summary of trade sizes (qty column).

    Returns kurtosis as the metric value (expect positive = fat-tailed).
    """
    if trades_df.empty or "qty" not in trades_df.columns:
        return MetricResult(
            name="trade_size_distribution", value=float("nan"),
            unit="ratio", direction="match_target", notes="no trades",
        )
    sizes = trades_df["qty"].dropna().values.astype(float)
    if len(sizes) < 4:
        return MetricResult(
            name="trade_size_distribution", value=float("nan"),
            unit="ratio", direction="match_target", notes="<4 trades",
        )
    from scipy.stats import kurtosis as _kurt, skew as _skew

    k = float(_kurt(sizes, fisher=True))
    s = float(_skew(sizes))
    if math.isnan(k):
        k = 0.0
    return MetricResult(
        name="trade_size_distribution", value=k, unit="ratio",
        direction="match_target",
        notes=f"skew={s:.3f}, kurtosis={k:.3f}",
    )
