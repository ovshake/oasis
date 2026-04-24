"""Tests for the eval subpackage (Phase 11).

Uses synthetic data throughout -- no real network calls.
"""

from __future__ import annotations

import json
import math
import subprocess
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def synthetic_prices() -> pd.Series:
    """100-step price series with a dip-and-recovery pattern."""
    rng = np.random.default_rng(42)
    base = 100.0
    prices = [base]
    for i in range(99):
        if i < 30:
            drift = -0.002  # drawdown phase
        elif i < 60:
            drift = 0.001  # recovery
        else:
            drift = 0.0005
        prices.append(prices[-1] * (1 + drift + rng.normal(0, 0.005)))
    return pd.Series(prices)


@pytest.fixture()
def flat_prices() -> pd.Series:
    return pd.Series([100.0] * 100)


@pytest.fixture()
def synthetic_run_dir(tmp_path: Path, synthetic_prices: pd.Series) -> Path:
    """Create a minimal run directory with parquet files."""
    run_dir = tmp_path / "test_run"
    run_dir.mkdir()

    # prices.parquet
    n = len(synthetic_prices)
    prices_data = {
        "step": list(range(n)),
        "pair_id": [1] * n,
        "base_symbol": ["BTC"] * n,
        "quote_symbol": ["USD"] * n,
        "last_price": synthetic_prices.values.tolist(),
        "prev_close_price": [100.0] + synthetic_prices.values[:-1].tolist(),
        "volume_step": [float(np.random.default_rng(42).uniform(0.1, 5.0))] * n,
    }
    _write_parquet(run_dir / "prices.parquet", prices_data)

    # trades.parquet
    rng = np.random.default_rng(42)
    n_trades = 50
    trades_data = {
        "trade_id": list(range(n_trades)),
        "step": rng.integers(0, n, n_trades).tolist(),
        "pair_id": [1] * n_trades,
        "price": (100.0 + rng.normal(0, 2, n_trades)).tolist(),
        "qty": rng.lognormal(0, 1, n_trades).tolist(),
        "buyer_id": rng.integers(1, 100, n_trades).tolist(),
        "seller_id": rng.integers(1, 100, n_trades).tolist(),
    }
    _write_parquet(run_dir / "trades.parquet", trades_data)

    # actions.parquet
    steps_act = []
    user_ids = []
    archetypes = []
    tiers = []
    action_types = []
    arch_pool = ["hodler", "fomo_degen", "market_maker", "lurker"]
    tier_pool = ["silent", "react", "trade", "post"]
    at_pool = ["DO_NOTHING", "LIKE_POST", "PLACE_ORDER", "CREATE_POST"]
    for s in range(n):
        for uid in range(1, 21):
            steps_act.append(s)
            user_ids.append(uid)
            archetypes.append(arch_pool[uid % len(arch_pool)])
            t_idx = rng.integers(0, len(tier_pool))
            tiers.append(tier_pool[t_idx])
            action_types.append(at_pool[t_idx])
    actions_data = {
        "step": steps_act,
        "user_id": user_ids,
        "archetype": archetypes,
        "tier": tiers,
        "action_type": action_types,
    }
    _write_parquet(run_dir / "actions.parquet", actions_data)

    # posts.parquet
    n_posts = 30
    posts_data = {
        "post_id": list(range(n_posts)),
        "step": sorted(rng.integers(0, n, n_posts).tolist()),
        "author_user_id": rng.integers(1, 100, n_posts).tolist(),
        "content": [f"post {i}" for i in range(n_posts)],
        "sentiment": rng.uniform(-1, 1, n_posts).tolist(),
    }
    _write_parquet(run_dir / "posts.parquet", posts_data)

    # news.parquet
    news_data = {
        "step": [10, 40, 70],
        "source": ["manual", "manual", "manual"],
        "title": ["Fed hikes rates", "Whale sells", "Recovery signal"],
        "sentiment_valence": [-0.8, -0.5, 0.3],
        "audience": ["all", "all", "all"],
        "affected_assets": ["BTC,ETH", "BTC", "ETH"],
    }
    _write_parquet(run_dir / "news.parquet", news_data)

    # conservation.parquet
    cons_data = {
        "step": [0, 60],
        "instrument": ["USD", "USD"],
        "total_amount": [1000000.0, 1000000.0],
        "total_locked": [0.0, 0.0],
        "total_supply": [1000000.0, 1000000.0],
    }
    _write_parquet(run_dir / "conservation.parquet", cons_data)

    return run_dir


def _write_parquet(path: Path, data: dict) -> None:
    """Write a dict-of-lists to parquet."""
    df = pd.DataFrame(data)
    df.to_parquet(path, index=False)


# ---------------------------------------------------------------------------
# Tier A tests
# ---------------------------------------------------------------------------


class TestTierA:
    def test_direction_match_pct_perfect(self, synthetic_prices: pd.Series):
        from oasis.crypto.eval.metrics.price_path import direction_match_pct

        result = direction_match_pct(synthetic_prices, synthetic_prices, min_bps=0.1)
        assert result.name == "direction_match_pct"
        assert result.value == pytest.approx(1.0)

    def test_direction_match_pct_inverse(self, synthetic_prices: pd.Series):
        from oasis.crypto.eval.metrics.price_path import direction_match_pct

        inverted = 200 - synthetic_prices  # inverse movement
        result = direction_match_pct(inverted, synthetic_prices, min_bps=0.1)
        assert result.value == pytest.approx(0.0, abs=0.05)

    def test_path_correlation_perfect(self, synthetic_prices: pd.Series):
        from oasis.crypto.eval.metrics.price_path import path_correlation

        result = path_correlation(synthetic_prices, synthetic_prices)
        assert result.value == pytest.approx(1.0)

    def test_path_correlation_returns_metric_result(self, synthetic_prices: pd.Series):
        from oasis.crypto.eval.metrics.price_path import path_correlation

        result = path_correlation(synthetic_prices, synthetic_prices)
        assert result.name == "path_correlation"
        assert result.unit == "ratio"

    def test_peak_drawdown_error_self(self, synthetic_prices: pd.Series):
        from oasis.crypto.eval.metrics.price_path import peak_drawdown_error

        result = peak_drawdown_error(synthetic_prices, synthetic_prices)
        assert result.value == pytest.approx(0.0)

    def test_drawdown_timing_error_self(self, synthetic_prices: pd.Series):
        from oasis.crypto.eval.metrics.price_path import drawdown_timing_error

        result = drawdown_timing_error(synthetic_prices, synthetic_prices)
        assert result.value == pytest.approx(0.0)

    def test_terminal_price_error_self(self, synthetic_prices: pd.Series):
        from oasis.crypto.eval.metrics.price_path import terminal_price_error

        result = terminal_price_error(synthetic_prices, synthetic_prices)
        assert result.value == pytest.approx(0.0)

    def test_all_return_metric_result(self, synthetic_prices: pd.Series):
        from oasis.crypto.eval import MetricResult
        from oasis.crypto.eval.metrics.price_path import (
            direction_match_pct,
            drawdown_timing_error,
            path_correlation,
            peak_drawdown_error,
            terminal_price_error,
        )

        fns = [
            lambda: direction_match_pct(synthetic_prices, synthetic_prices),
            lambda: peak_drawdown_error(synthetic_prices, synthetic_prices),
            lambda: drawdown_timing_error(synthetic_prices, synthetic_prices),
            lambda: path_correlation(synthetic_prices, synthetic_prices),
            lambda: terminal_price_error(synthetic_prices, synthetic_prices),
        ]
        for fn in fns:
            r = fn()
            assert isinstance(r, MetricResult)
            assert isinstance(r.value, float)


# ---------------------------------------------------------------------------
# Tier B tests
# ---------------------------------------------------------------------------


class TestTierB:
    def test_return_kurtosis(self, synthetic_prices: pd.Series):
        from oasis.crypto.eval.metrics.distributional import return_kurtosis

        result = return_kurtosis(synthetic_prices)
        assert result.name == "return_kurtosis"
        assert not math.isnan(result.value)

    def test_vol_clustering_acf(self, synthetic_prices: pd.Series):
        from oasis.crypto.eval.metrics.distributional import vol_clustering_acf

        result = vol_clustering_acf(synthetic_prices, lag=1)
        assert "vol_clustering" in result.name
        assert isinstance(result.value, float)

    def test_realized_vol(self, synthetic_prices: pd.Series):
        from oasis.crypto.eval.metrics.distributional import realized_vol

        result = realized_vol(synthetic_prices)
        assert result.value > 0

    def test_green_red_ratio(self, synthetic_prices: pd.Series):
        from oasis.crypto.eval.metrics.distributional import green_red_ratio

        result = green_red_ratio(synthetic_prices)
        assert 0 <= result.value <= 1

    def test_flat_prices_zero_vol(self, flat_prices: pd.Series):
        from oasis.crypto.eval.metrics.distributional import realized_vol

        result = realized_vol(flat_prices)
        assert result.value == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# Tier C tests
# ---------------------------------------------------------------------------


class TestTierC:
    def test_active_agent_rate(self):
        from oasis.crypto.eval.metrics.microstructure import active_agent_rate

        df = pd.DataFrame({
            "step": [0, 0, 0, 1, 1, 1],
            "tier": ["silent", "trade", "post", "silent", "silent", "react"],
        })
        result = active_agent_rate(df)
        # Step 0: 2/3 active, Step 1: 1/3 active -> mean = 0.5
        assert result.value == pytest.approx(0.5, abs=0.01)

    def test_trade_size_distribution(self):
        from oasis.crypto.eval.metrics.microstructure import trade_size_distribution

        rng = np.random.default_rng(42)
        df = pd.DataFrame({"qty": rng.lognormal(0, 1, 100)})
        result = trade_size_distribution(df)
        assert not math.isnan(result.value)

    def test_empty_actions(self):
        from oasis.crypto.eval.metrics.microstructure import active_agent_rate

        result = active_agent_rate(pd.DataFrame())
        assert math.isnan(result.value)


# ---------------------------------------------------------------------------
# Tier D tests
# ---------------------------------------------------------------------------


class TestTierD:
    def test_correlation_matrix(self):
        from oasis.crypto.eval.metrics.cross_asset import correlation_matrix

        rng = np.random.default_rng(42)
        df = pd.DataFrame({
            "BTC": 100 + np.cumsum(rng.normal(0, 1, 100)),
            "ETH": 50 + np.cumsum(rng.normal(0, 1, 100)),
        })
        corr = correlation_matrix(df, ["BTC", "ETH"])
        assert corr.shape == (2, 2)
        assert corr.loc["BTC", "BTC"] == pytest.approx(1.0)

    def test_frobenius_distance_zero(self):
        from oasis.crypto.eval.metrics.cross_asset import correlation_frobenius_distance

        mat = pd.DataFrame(
            [[1.0, 0.5], [0.5, 1.0]],
            index=["A", "B"], columns=["A", "B"],
        )
        result = correlation_frobenius_distance(mat, mat)
        assert result.value == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# Tier E tests
# ---------------------------------------------------------------------------


class TestTierE:
    def test_post_volume_around_news(self):
        from oasis.crypto.eval.metrics.social import post_volume_around_news

        posts = pd.DataFrame({"step": list(range(100))})
        news = pd.DataFrame({"step": [50]})
        result = post_volume_around_news(posts, news, window=10)
        assert isinstance(result.value, float)

    def test_sentiment_price_correlation(self):
        from oasis.crypto.eval.metrics.social import sentiment_price_correlation

        sent = pd.DataFrame({"step": range(50), "sentiment": np.linspace(-1, 1, 50)})
        prices = pd.DataFrame({"step": range(50), "price": np.linspace(100, 110, 50)})
        result = sentiment_price_correlation(sent, prices)
        assert isinstance(result.value, float)


# ---------------------------------------------------------------------------
# Tier F tests
# ---------------------------------------------------------------------------


class TestTierF:
    def test_gini_wealth(self):
        from oasis.crypto.eval.metrics.agent_level import gini_wealth

        # Perfectly equal
        df = pd.DataFrame({"user_id": range(10), "amount": [100.0] * 10})
        result = gini_wealth(df)
        assert result.value == pytest.approx(0.0, abs=0.05)

    def test_gini_wealth_unequal(self):
        from oasis.crypto.eval.metrics.agent_level import gini_wealth

        # One person has everything
        amounts = [0.0] * 99 + [10000.0]
        df = pd.DataFrame({"user_id": range(100), "amount": amounts})
        result = gini_wealth(df)
        assert result.value > 0.9

    def test_conservation_check_pass(self):
        from oasis.crypto.eval.metrics.agent_level import conservation_check

        df = pd.DataFrame({
            "step": [0, 60],
            "instrument": ["USD", "USD"],
            "total_amount": [1000.0, 1000.0],
            "total_supply": [1000.0, 1000.0],
        })
        result = conservation_check(df)
        assert result.passed is True

    def test_conservation_check_drift(self):
        from oasis.crypto.eval.metrics.agent_level import conservation_check

        df = pd.DataFrame({
            "step": [0, 60],
            "instrument": ["USD", "USD"],
            "total_amount": [1000.0, 999.0],
            "total_supply": [1000.0, 1000.0],
        })
        result = conservation_check(df)
        assert result.passed is False

    def test_action_distribution_per_archetype(self):
        from oasis.crypto.eval.metrics.agent_level import action_distribution_per_archetype

        df = pd.DataFrame({
            "archetype": ["hodler"] * 10 + ["fomo"] * 10,
            "action_type": (["HOLD"] * 5 + ["SELL"] * 5 +
                           ["BUY"] * 3 + ["SELL"] * 7),
        })
        results = action_distribution_per_archetype(df)
        assert "hodler" in results
        assert "fomo" in results
        assert results["hodler"].value > 0  # entropy > 0


# ---------------------------------------------------------------------------
# Baselines tests
# ---------------------------------------------------------------------------


class TestBaselines:
    def test_random_walk_reproducible(self):
        from oasis.crypto.eval.baselines import random_walk_prices

        real = pd.DataFrame({"price": np.linspace(100, 110, 100)})
        r1 = random_walk_prices(real, seed=42, price_col="price")
        r2 = random_walk_prices(real, seed=42, price_col="price")
        pd.testing.assert_frame_equal(r1, r2)

    def test_random_walk_same_length(self):
        from oasis.crypto.eval.baselines import random_walk_prices

        real = pd.DataFrame({"price": np.linspace(100, 110, 50)})
        result = random_walk_prices(real, seed=1, price_col="price")
        assert len(result) == len(real)

    def test_constant_prices(self):
        from oasis.crypto.eval.baselines import constant_prices

        real = pd.DataFrame({"price": [100.0, 105.0, 110.0]})
        result = constant_prices(real, price_col="price")
        assert all(result["price"] == 100.0)

    def test_replay_prices(self):
        from oasis.crypto.eval.baselines import replay_prices

        real = pd.DataFrame({"price": [100.0, 105.0, 110.0]})
        result = replay_prices(real)
        pd.testing.assert_frame_equal(result, real)

    def test_stubs_return_metric_result(self):
        from oasis.crypto.eval import MetricResult
        from oasis.crypto.eval.baselines import (
            no_agent_prices,
            no_news_prices,
            shuffled_news_prices,
            uniform_persona_prices,
        )

        for fn in [no_news_prices, shuffled_news_prices,
                    uniform_persona_prices, no_agent_prices]:
            r = fn()
            assert isinstance(r, MetricResult)
            assert "not_implemented" in (r.notes or "")


# ---------------------------------------------------------------------------
# Aggregate tests
# ---------------------------------------------------------------------------


class TestAggregate:
    def test_bootstrap_ci_normal(self):
        from oasis.crypto.eval.aggregate import bootstrap_ci

        rng = np.random.default_rng(42)
        values = rng.normal(10.0, 1.0, 1000).tolist()
        lo, hi = bootstrap_ci(values, alpha=0.05)
        # True mean is 10.0; CI should contain it
        assert lo < 10.0 < hi
        # Width should be roughly 2 * 1.96 * 1/sqrt(1000) ~ 0.12
        assert (hi - lo) < 0.5

    def test_bootstrap_ci_single_value(self):
        from oasis.crypto.eval.aggregate import bootstrap_ci

        lo, hi = bootstrap_ci([5.0])
        assert lo == 5.0
        assert hi == 5.0

    def test_aggregate_metrics(self):
        from oasis.crypto.eval import MetricResult
        from oasis.crypto.eval.aggregate import aggregate_metrics

        seed1 = [
            MetricResult(name="test_m", value=0.8, unit="ratio",
                        direction="higher_better", threshold=0.5),
        ]
        seed2 = [
            MetricResult(name="test_m", value=0.9, unit="ratio",
                        direction="higher_better", threshold=0.5),
        ]
        agg = aggregate_metrics([seed1, seed2])
        assert len(agg) == 1
        assert agg[0].value == pytest.approx(0.85)
        assert agg[0].passed is True


# ---------------------------------------------------------------------------
# Scoring tests
# ---------------------------------------------------------------------------


class TestScoring:
    def test_score_tier_all_pass(self):
        from oasis.crypto.eval import MetricResult
        from oasis.crypto.eval.scoring import score_tier

        metrics = [
            MetricResult(name="m1", value=0.9, unit="ratio",
                        direction="higher_better", passed=True),
            MetricResult(name="m2", value=0.8, unit="ratio",
                        direction="higher_better", passed=True),
            MetricResult(name="m3", value=0.7, unit="ratio",
                        direction="higher_better", passed=True),
        ]
        score = score_tier(metrics)
        assert score == 1.0

    def test_score_tier_all_fail(self):
        from oasis.crypto.eval import MetricResult
        from oasis.crypto.eval.scoring import score_tier

        metrics = [
            MetricResult(name="m1", value=0.1, unit="ratio",
                        direction="higher_better", passed=False,
                        threshold=0.5),
            MetricResult(name="m2", value=0.05, unit="ratio",
                        direction="higher_better", passed=False,
                        threshold=0.5),
        ]
        score = score_tier(metrics)
        assert score < 0.5

    def test_score_vector_structure(self):
        from oasis.crypto.eval import MetricResult
        from oasis.crypto.eval.scoring import TIERS, score_vector

        metrics = [
            MetricResult(name="direction_match_pct", value=0.6, unit="ratio",
                        direction="higher_better", passed=True),
        ]
        sv = score_vector(metrics)
        for tier in TIERS:
            assert tier in sv
        assert "vs_random_walk" in sv


# ---------------------------------------------------------------------------
# Ground truth tests
# ---------------------------------------------------------------------------


class TestGroundTruth:
    def test_fetch_fear_greed_monkey_patched(self):
        from oasis.crypto.eval.ground_truth.sentiment import parse_fng_response

        sample_response = {
            "data": [
                {
                    "value": "25",
                    "value_classification": "Extreme Fear",
                    "timestamp": "1651968000",
                },
                {
                    "value": "50",
                    "value_classification": "Neutral",
                    "timestamp": "1651881600",
                },
            ],
        }
        df = parse_fng_response(sample_response)
        assert len(df) == 2
        assert "value" in df.columns
        assert "classification" in df.columns
        assert df["value"].iloc[0] == 25

    def test_ground_truth_registry_init(self):
        from oasis.crypto.eval.ground_truth.registry import GroundTruth

        gt = GroundTruth(start="2022-05-07", end="2022-05-11", assets=["BTC"])
        assert gt.start == "2022-05-07"
        assert gt.assets == ["BTC"]


# ---------------------------------------------------------------------------
# Report tests
# ---------------------------------------------------------------------------


class TestReport:
    def test_generate_report_produces_md(self, synthetic_run_dir: Path):
        from oasis.crypto.eval.report import generate_report

        report_data = generate_report(synthetic_run_dir, mode="sanity")

        md_path = synthetic_run_dir / "eval_report.md"
        assert md_path.exists()

        content = md_path.read_text()
        assert "Score Vector" in content
        assert "Tier" in content  # at least one tier section
        assert "Caveats" in content

    def test_generate_report_produces_html(self, synthetic_run_dir: Path):
        from oasis.crypto.eval.report import generate_report

        generate_report(synthetic_run_dir, mode="sanity")
        html_path = synthetic_run_dir / "eval_report.html"
        assert html_path.exists()
        assert "<html" in html_path.read_text()

    def test_generate_report_produces_json(self, synthetic_run_dir: Path):
        from oasis.crypto.eval.report import generate_report

        report_data = generate_report(synthetic_run_dir, mode="sanity")
        json_path = synthetic_run_dir / "eval_report.json"
        assert json_path.exists()

        loaded = json.loads(json_path.read_text())
        assert "score_vector" in loaded
        assert "metrics" in loaded
        assert "caveats" in loaded

    def test_report_data_structure(self, synthetic_run_dir: Path):
        from oasis.crypto.eval.report import generate_report

        report_data = generate_report(synthetic_run_dir, mode="sanity")
        assert "score_vector" in report_data
        assert "metrics" in report_data
        assert isinstance(report_data["metrics"], list)
        assert len(report_data["metrics"]) > 0


# ---------------------------------------------------------------------------
# CLI tests
# ---------------------------------------------------------------------------


class TestCLI:
    def test_evaluate_help(self):
        result = subprocess.run(
            [sys.executable, "scripts/evaluate.py", "--help"],
            capture_output=True, text=True,
            cwd=str(Path(__file__).resolve().parents[2]),
        )
        assert result.returncode == 0
        assert "evaluate" in result.stdout.lower() or "eval" in result.stdout.lower()

    def test_calibrate_help(self):
        result = subprocess.run(
            [sys.executable, "scripts/calibrate.py", "--help"],
            capture_output=True, text=True,
            cwd=str(Path(__file__).resolve().parents[2]),
        )
        assert result.returncode == 0

    def test_calibrate_stub(self):
        result = subprocess.run(
            [sys.executable, "scripts/calibrate.py"],
            capture_output=True, text=True,
            cwd=str(Path(__file__).resolve().parents[2]),
        )
        assert result.returncode == 0
        assert "not implemented" in result.stdout.lower()
