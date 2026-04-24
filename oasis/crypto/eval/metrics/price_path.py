"""Tier A -- Price path metrics.

Compare sim price series against real (ground truth) price series.
All functions accept pd.DataFrame with columns: step, price (or similar).
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd

from oasis.crypto.eval import MetricResult


def _safe_returns(prices: pd.Series) -> np.ndarray:
    """Pct returns, dropping leading NaN."""
    r = prices.pct_change().dropna().values
    return r if len(r) > 0 else np.array([0.0])


def direction_match_pct(
    sim_prices: pd.Series,
    real_prices: pd.Series,
    min_bps: float = 10.0,
) -> MetricResult:
    """Fraction of steps where sim and real price move in the same direction.

    Only counts steps where |real_return| > min_bps / 10_000.
    """
    sim_r = _safe_returns(sim_prices)
    real_r = _safe_returns(real_prices)
    n = min(len(sim_r), len(real_r))
    if n == 0:
        return MetricResult(
            name="direction_match_pct", value=float("nan"),
            unit="ratio", direction="higher_better", notes="no data",
        )
    sim_r, real_r = sim_r[:n], real_r[:n]
    threshold = min_bps / 10_000
    mask = np.abs(real_r) > threshold
    if mask.sum() == 0:
        return MetricResult(
            name="direction_match_pct", value=float("nan"),
            unit="ratio", direction="higher_better",
            notes="no steps above min_bps threshold",
        )
    matches = np.sign(sim_r[mask]) == np.sign(real_r[mask])
    val = float(matches.mean())
    return MetricResult(
        name="direction_match_pct", value=val, unit="ratio",
        direction="higher_better", threshold=0.55,
        passed=val >= 0.55,
    )


def peak_drawdown_error(
    sim_prices: pd.Series, real_prices: pd.Series,
) -> MetricResult:
    """Relative error between sim and real peak drawdown."""
    sim_dd = _max_drawdown(sim_prices)
    real_dd = _max_drawdown(real_prices)
    if real_dd == 0 or math.isnan(real_dd):
        return MetricResult(
            name="peak_drawdown_error", value=float("nan"),
            unit="ratio", direction="lower_better", notes="real dd is zero",
        )
    val = abs(sim_dd - real_dd) / abs(real_dd)
    return MetricResult(
        name="peak_drawdown_error", value=val, unit="ratio",
        direction="lower_better", threshold=0.30,
        passed=val <= 0.30,
    )


def drawdown_timing_error(
    sim_prices: pd.Series, real_prices: pd.Series,
) -> MetricResult:
    """Timing error of the drawdown trough as fraction of duration."""
    n = min(len(sim_prices), len(real_prices))
    if n < 2:
        return MetricResult(
            name="drawdown_timing_error", value=float("nan"),
            unit="ratio", direction="lower_better", notes="too short",
        )
    sim_min_step = int(np.argmin(sim_prices.values[:n]))
    real_min_step = int(np.argmin(real_prices.values[:n]))
    val = abs(sim_min_step - real_min_step) / n
    return MetricResult(
        name="drawdown_timing_error", value=val, unit="ratio",
        direction="lower_better", threshold=0.10,
        passed=val <= 0.10,
    )


def path_correlation(
    sim_prices: pd.Series, real_prices: pd.Series,
) -> MetricResult:
    """Pearson correlation of the two price series."""
    n = min(len(sim_prices), len(real_prices))
    if n < 3:
        return MetricResult(
            name="path_correlation", value=float("nan"),
            unit="ratio", direction="higher_better", notes="too short",
        )
    s = sim_prices.values[:n].astype(float)
    r = real_prices.values[:n].astype(float)
    if np.std(s) == 0 or np.std(r) == 0:
        return MetricResult(
            name="path_correlation", value=0.0,
            unit="ratio", direction="higher_better", notes="zero variance",
        )
    val = float(np.corrcoef(s, r)[0, 1])
    if math.isnan(val):
        val = 0.0
    return MetricResult(
        name="path_correlation", value=val, unit="ratio",
        direction="higher_better", threshold=0.40,
        passed=val >= 0.40,
    )


def terminal_price_error(
    sim_prices: pd.Series, real_prices: pd.Series,
) -> MetricResult:
    """Relative error of terminal (last) price."""
    if len(sim_prices) == 0 or len(real_prices) == 0:
        return MetricResult(
            name="terminal_price_error", value=float("nan"),
            unit="ratio", direction="lower_better", notes="empty",
        )
    sim_last = float(sim_prices.iloc[-1])
    real_last = float(real_prices.iloc[-1])
    if real_last == 0:
        return MetricResult(
            name="terminal_price_error", value=float("nan"),
            unit="ratio", direction="lower_better", notes="real terminal=0",
        )
    val = abs(sim_last - real_last) / abs(real_last)
    return MetricResult(
        name="terminal_price_error", value=val, unit="ratio",
        direction="lower_better", threshold=0.15,
        passed=val <= 0.15,
    )


# ---- helpers ----

def _max_drawdown(prices: pd.Series) -> float:
    """Maximum drawdown as a positive fraction (e.g. 0.30 = 30% drop)."""
    vals = prices.values.astype(float)
    if len(vals) < 2:
        return 0.0
    peak = vals[0]
    max_dd = 0.0
    for v in vals[1:]:
        if v > peak:
            peak = v
        dd = (peak - v) / peak if peak > 0 else 0.0
        if dd > max_dd:
            max_dd = dd
    return max_dd
