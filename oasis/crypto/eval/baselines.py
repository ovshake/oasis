"""Baseline generators for eval comparison.

Three fully functional (derivable from data):
  random_walk, constant, replay.
Four sim-based (stubs for MVP -- require re-running simulation):
  no_news, shuffled_news, uniform_persona, no_agent.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from oasis.crypto.eval import MetricResult


# ---------------------------------------------------------------------------
# Fully functional baselines
# ---------------------------------------------------------------------------


def random_walk_prices(
    real_prices: pd.DataFrame,
    seed: int = 42,
    price_col: str | None = None,
) -> pd.DataFrame:
    """Geometric Brownian motion calibrated to real volatility.

    *real_prices*: DataFrame with at least one price column.
    Returns same-shape DataFrame with simulated prices.
    Reproducible given *seed*.
    """
    rng = np.random.default_rng(seed)
    result = real_prices.copy()

    cols = _price_columns(real_prices, price_col)
    for col in cols:
        series = real_prices[col].dropna().values.astype(float)
        if len(series) < 2:
            continue
        returns = np.diff(series) / series[:-1]
        mu = float(np.mean(returns))
        sigma = float(np.std(returns))
        if sigma == 0:
            sigma = 1e-6

        n = len(series)
        sim = np.zeros(n)
        sim[0] = series[0]
        shocks = rng.normal(mu, sigma, n - 1)
        for i in range(1, n):
            sim[i] = sim[i - 1] * (1 + shocks[i - 1])
            sim[i] = max(sim[i], 1e-8)  # prevent negative
        result[col] = sim[: len(result)]
    return result


def constant_prices(
    real_prices: pd.DataFrame,
    price_col: str | None = None,
) -> pd.DataFrame:
    """Flat prices at the opening value (absolute floor baseline)."""
    result = real_prices.copy()
    cols = _price_columns(real_prices, price_col)
    for col in cols:
        vals = real_prices[col].dropna()
        if len(vals) == 0:
            continue
        result[col] = vals.iloc[0]
    return result


def replay_prices(real_prices: pd.DataFrame) -> pd.DataFrame:
    """Return real prices as-is (the ceiling / oracle baseline)."""
    return real_prices.copy()


# ---------------------------------------------------------------------------
# Sim-based baselines (stubs -- require re-running the simulation)
# ---------------------------------------------------------------------------


def no_news_prices(**kwargs) -> MetricResult:
    """Stub: same scenario minus news events. Requires sim re-run."""
    return MetricResult(
        name="no_news_baseline", value=float("nan"),
        unit="ratio", direction="match_target",
        notes="not_implemented: requires sim re-run (Phase 11 integration)",
    )


def shuffled_news_prices(**kwargs) -> MetricResult:
    """Stub: news events at randomized steps. Requires sim re-run."""
    return MetricResult(
        name="shuffled_news_baseline", value=float("nan"),
        unit="ratio", direction="match_target",
        notes="not_implemented: requires sim re-run",
    )


def uniform_persona_prices(**kwargs) -> MetricResult:
    """Stub: all agents are a single archetype. Requires sim re-run."""
    return MetricResult(
        name="uniform_persona_baseline", value=float("nan"),
        unit="ratio", direction="match_target",
        notes="not_implemented: requires sim re-run",
    )


def no_agent_prices(**kwargs) -> MetricResult:
    """Stub: only a single MM, no social agents. Requires sim re-run."""
    return MetricResult(
        name="no_agent_baseline", value=float("nan"),
        unit="ratio", direction="match_target",
        notes="not_implemented: requires sim re-run",
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _price_columns(df: pd.DataFrame, explicit: str | None = None) -> list[str]:
    """Determine which columns are price data."""
    if explicit:
        return [explicit] if explicit in df.columns else []
    # Heuristic: skip 'step', 'datetime', 'date' columns
    skip = {"step", "datetime", "date", "pair_id", "base_symbol", "quote_symbol"}
    return [c for c in df.columns if c not in skip and df[c].dtype.kind == "f"]
