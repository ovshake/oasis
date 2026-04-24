"""Tier E -- Social metrics.

Post volume around news, sentiment-price correlation.
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd

from oasis.crypto.eval import MetricResult


def post_volume_around_news(
    posts_df: pd.DataFrame,
    news_df: pd.DataFrame,
    window: int = 30,
) -> MetricResult:
    """Ratio of post volume in +-window steps around news vs baseline.

    Expects posts_df with column 'step', news_df with column 'step'.
    """
    if posts_df.empty or news_df.empty:
        return MetricResult(
            name="post_volume_around_news", value=float("nan"),
            unit="ratio", direction="higher_better", notes="no data",
        )
    if "step" not in posts_df.columns or "step" not in news_df.columns:
        return MetricResult(
            name="post_volume_around_news", value=float("nan"),
            unit="ratio", direction="higher_better", notes="missing step col",
        )
    news_steps = set(news_df["step"].unique())
    total_steps = posts_df["step"].nunique()
    if total_steps == 0:
        return MetricResult(
            name="post_volume_around_news", value=float("nan"),
            unit="ratio", direction="higher_better", notes="zero steps",
        )

    # Build mask of steps within window of any news step
    all_steps = posts_df["step"].values
    near_news = np.zeros(len(all_steps), dtype=bool)
    for ns in news_steps:
        near_news |= (np.abs(all_steps - ns) <= window)

    vol_near = near_news.sum()
    vol_far = (~near_news).sum()

    # Normalize by step count
    near_step_count = 0
    far_step_count = 0
    for s in posts_df["step"].unique():
        is_near = any(abs(s - ns) <= window for ns in news_steps)
        if is_near:
            near_step_count += 1
        else:
            far_step_count += 1

    if far_step_count == 0 or vol_far == 0:
        return MetricResult(
            name="post_volume_around_news", value=float("nan"),
            unit="ratio", direction="higher_better",
            notes="all steps near news",
        )
    rate_near = vol_near / max(near_step_count, 1)
    rate_far = vol_far / max(far_step_count, 1)
    val = rate_near / rate_far if rate_far > 0 else float("nan")
    return MetricResult(
        name="post_volume_around_news", value=float(val), unit="ratio",
        direction="higher_better", threshold=3.0,
        passed=None if math.isnan(val) else val >= 3.0,
    )


def sentiment_price_correlation(
    sentiment_df: pd.DataFrame,
    prices_df: pd.DataFrame,
) -> MetricResult:
    """Pearson correlation between step-level sentiment and price returns.

    sentiment_df needs columns: step, sentiment.
    prices_df needs columns: step, price (or last_price).
    """
    if sentiment_df.empty or prices_df.empty:
        return MetricResult(
            name="sentiment_price_correlation", value=float("nan"),
            unit="ratio", direction="higher_better", notes="no data",
        )
    price_col = "price" if "price" in prices_df.columns else "last_price"
    if price_col not in prices_df.columns or "sentiment" not in sentiment_df.columns:
        return MetricResult(
            name="sentiment_price_correlation", value=float("nan"),
            unit="ratio", direction="higher_better", notes="missing cols",
        )
    # Merge on step
    merged = pd.merge(
        sentiment_df[["step", "sentiment"]],
        prices_df[["step", price_col]],
        on="step", how="inner",
    )
    if len(merged) < 3:
        return MetricResult(
            name="sentiment_price_correlation", value=float("nan"),
            unit="ratio", direction="higher_better", notes="<3 merged rows",
        )
    returns = merged[price_col].pct_change().dropna()
    sent = merged["sentiment"].iloc[1:]  # align after pct_change drop
    n = min(len(returns), len(sent))
    if n < 3:
        return MetricResult(
            name="sentiment_price_correlation", value=float("nan"),
            unit="ratio", direction="higher_better", notes="too short after align",
        )
    r_vals = returns.values[:n].astype(float)
    s_vals = sent.values[:n].astype(float)
    if np.std(r_vals) == 0 or np.std(s_vals) == 0:
        val = 0.0
    else:
        val = float(np.corrcoef(r_vals, s_vals)[0, 1])
    if math.isnan(val):
        val = 0.0
    return MetricResult(
        name="sentiment_price_correlation", value=val, unit="ratio",
        direction="higher_better",
    )
