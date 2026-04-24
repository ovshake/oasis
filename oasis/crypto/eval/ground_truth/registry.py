"""GroundTruth registry -- unified facade for all ground-truth sources."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from oasis.crypto.eval.ground_truth.derived import (
    compute_style_facts,
    rolling_correlations,
)
from oasis.crypto.eval.ground_truth.price import fetch_real_prices
from oasis.crypto.eval.ground_truth.sentiment import fetch_fear_greed

logger = logging.getLogger(__name__)


@dataclass
class GroundTruth:
    """Unified access to all ground-truth data for a date range + asset set.

    Usage::

        gt = GroundTruth(start="2022-05-07", end="2022-05-11",
                         assets=["BTC", "ETH", "USDT"])
        prices = gt.prices(resolution="1m")
        fng = gt.sentiment("fear_greed")
        corr = gt.correlations(window=60)
    """

    start: str
    end: str
    assets: list[str] = field(default_factory=lambda: ["BTC", "ETH"])
    cache_dir: Path | None = None

    # Lazy caches
    _price_cache: dict[str, pd.DataFrame] = field(
        default_factory=dict, repr=False,
    )

    def prices(self, resolution: str = "1m") -> pd.DataFrame:
        """Fetch (or cache-hit) ground-truth prices."""
        key = f"{resolution}"
        if key not in self._price_cache:
            self._price_cache[key] = fetch_real_prices(
                self.assets, self.start, self.end,
                resolution=resolution, cache_dir=self.cache_dir,
            )
        return self._price_cache[key]

    def sentiment(self, metric: str = "fear_greed") -> pd.DataFrame:
        """Fetch sentiment ground truth."""
        if metric == "fear_greed":
            return fetch_fear_greed(self.start, self.end)
        logger.warning("Unknown sentiment metric: %s", metric)
        return pd.DataFrame()

    def correlations(self, window: int = 60) -> pd.DataFrame:
        """Rolling pairwise correlations derived from prices."""
        p = self.prices()
        if p.empty:
            return pd.DataFrame()
        available = [a for a in self.assets if a in p.columns]
        if len(available) < 2:
            return pd.DataFrame()
        return rolling_correlations(p, available, window=window)

    def style_facts(self, asset: str) -> dict[str, float]:
        """Style facts (vol, kurtosis, etc.) for a single asset."""
        p = self.prices()
        if p.empty or asset not in p.columns:
            return {}
        return compute_style_facts(p[asset])
