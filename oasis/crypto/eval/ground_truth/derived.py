"""Derived ground-truth metrics computed from price data.

Rolling correlations, style facts (vol, kurtosis).
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def rolling_correlations(
    prices_df: pd.DataFrame,
    assets: list[str],
    window: int = 60,
) -> pd.DataFrame:
    """Compute rolling pairwise correlation of returns.

    Returns a long-form DataFrame with columns:
    step, asset_a, asset_b, correlation.
    """
    returns = prices_df[assets].pct_change().dropna()
    if returns.empty or len(returns) < window:
        return pd.DataFrame(columns=["step", "asset_a", "asset_b", "correlation"])

    rows: list[dict] = []
    for i in range(window, len(returns)):
        window_ret = returns.iloc[i - window : i]
        corr = window_ret.corr()
        for ai, a in enumerate(assets):
            for b in assets[ai + 1 :]:
                rows.append({
                    "step": i,
                    "asset_a": a,
                    "asset_b": b,
                    "correlation": float(corr.loc[a, b]),
                })
    return pd.DataFrame(rows)


def compute_style_facts(prices: pd.Series) -> dict[str, float]:
    """Compute standard style facts from a single price series.

    Returns dict with keys: annualized_vol, kurtosis, skew, acf1_abs_ret.
    """
    r = prices.pct_change().dropna().values
    if len(r) < 4:
        return {
            "annualized_vol": float("nan"),
            "kurtosis": float("nan"),
            "skew": float("nan"),
            "acf1_abs_ret": float("nan"),
        }
    from scipy.stats import kurtosis, skew

    vol = float(np.std(r) * np.sqrt(525_600))  # 1-min annualized
    kurt = float(kurtosis(r, fisher=True))
    sk = float(skew(r))

    # ACF(|r|, lag=1)
    abs_r = np.abs(r)
    mean = abs_r.mean()
    var = abs_r.var()
    if var > 0 and len(abs_r) > 1:
        cov = np.sum((abs_r[:-1] - mean) * (abs_r[1:] - mean)) / len(abs_r)
        acf1 = float(cov / var)
    else:
        acf1 = 0.0

    return {
        "annualized_vol": vol,
        "kurtosis": kurt,
        "skew": sk,
        "acf1_abs_ret": acf1,
    }
