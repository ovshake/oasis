"""Report generator: markdown + HTML eval reports."""

from __future__ import annotations

import json
import logging
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

import pandas as pd

from oasis.crypto.eval import MetricResult
from oasis.crypto.eval.aggregate import aggregate_metrics, bootstrap_ci
from oasis.crypto.eval.baselines import constant_prices, random_walk_prices, replay_prices
from oasis.crypto.eval.metrics.agent_level import conservation_check, gini_wealth
from oasis.crypto.eval.metrics.cross_asset import (
    correlation_frobenius_distance,
    correlation_matrix,
)
from oasis.crypto.eval.metrics.distributional import (
    green_red_ratio,
    realized_vol,
    return_kurtosis,
    vol_clustering_acf,
)
from oasis.crypto.eval.metrics.microstructure import active_agent_rate, trade_size_distribution
from oasis.crypto.eval.metrics.price_path import (
    direction_match_pct,
    drawdown_timing_error,
    path_correlation,
    peak_drawdown_error,
    terminal_price_error,
)
from oasis.crypto.eval.metrics.social import post_volume_around_news, sentiment_price_correlation
from oasis.crypto.eval.scoring import TIERS, score_tier, score_vector

logger = logging.getLogger(__name__)


def generate_report(
    run_dir: Path,
    gt: object | None = None,
    mode: Literal["historical", "sanity", "stress"] = "sanity",
) -> dict:
    """Generate eval report from a simulation run directory.

    Reads parquet outputs, computes metrics, baselines, and writes
    eval_report.md and eval_report.html to run_dir.

    Returns the report data dict (for JSON serialization by the UI).
    """
    run_dir = Path(run_dir)

    # Load parquet data
    prices_df = _read_parquet(run_dir / "prices.parquet")
    trades_df = _read_parquet(run_dir / "trades.parquet")
    actions_df = _read_parquet(run_dir / "actions.parquet")
    posts_df = _read_parquet(run_dir / "posts.parquet")
    news_df = _read_parquet(run_dir / "news.parquet")
    conservation_df = _read_parquet(run_dir / "conservation.parquet")

    # Extract primary price series (first asset)
    sim_prices = _extract_price_series(prices_df)

    # Ground truth prices (if available and historical mode)
    real_prices = None
    if gt is not None and hasattr(gt, "prices") and mode == "historical":
        gt_df = gt.prices()
        if not gt_df.empty:
            # Use first available asset column
            for col in gt_df.columns:
                if col not in ("datetime", "date", "step"):
                    real_prices = gt_df[col]
                    break

    # Compute all metrics
    all_metrics: list[MetricResult] = []

    # Tier A
    if real_prices is not None and sim_prices is not None:
        all_metrics.extend([
            direction_match_pct(sim_prices, real_prices),
            peak_drawdown_error(sim_prices, real_prices),
            drawdown_timing_error(sim_prices, real_prices),
            path_correlation(sim_prices, real_prices),
            terminal_price_error(sim_prices, real_prices),
        ])

    # Tier B
    if sim_prices is not None:
        all_metrics.extend([
            return_kurtosis(sim_prices),
            vol_clustering_acf(sim_prices, lag=1),
            realized_vol(sim_prices),
            green_red_ratio(sim_prices),
        ])

    # Tier C
    all_metrics.append(active_agent_rate(actions_df))
    all_metrics.append(trade_size_distribution(trades_df))

    # Tier E
    all_metrics.append(post_volume_around_news(posts_df, news_df))
    # Sentiment-price: build a synthetic sentiment df from posts
    if not posts_df.empty and "sentiment" in posts_df.columns:
        sent_df = posts_df.groupby("step")["sentiment"].mean().reset_index()
        price_step_df = prices_df.groupby("step")["last_price"].last().reset_index() if not prices_df.empty else pd.DataFrame()
        if not price_step_df.empty:
            all_metrics.append(sentiment_price_correlation(sent_df, price_step_df))

    # Tier F
    if not conservation_df.empty:
        all_metrics.append(conservation_check(conservation_df))

    # Baselines (derivable)
    baseline_metrics: dict[str, list[MetricResult]] = {}
    if sim_prices is not None and real_prices is not None:
        for bl_name, bl_fn in [
            ("random_walk", lambda: random_walk_prices(
                pd.DataFrame({"price": real_prices}), price_col="price",
            )["price"]),
            ("constant", lambda: constant_prices(
                pd.DataFrame({"price": real_prices}), price_col="price",
            )["price"]),
        ]:
            try:
                bl_prices = bl_fn()
                bl_results = [
                    direction_match_pct(bl_prices, real_prices),
                    path_correlation(bl_prices, real_prices),
                ]
                baseline_metrics[bl_name] = bl_results
            except Exception as e:
                logger.warning("Baseline %s failed: %s", bl_name, e)

    # Scoring
    scores = score_vector(all_metrics)

    # Build report data
    report_data = {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "mode": mode,
        "run_dir": str(run_dir),
        "score_vector": scores,
        "metrics": [m.model_dump() for m in all_metrics],
        "baselines": {
            k: [m.model_dump() for m in v]
            for k, v in baseline_metrics.items()
        },
        "caveats": _build_caveats(mode, prices_df, real_prices),
    }

    # Write markdown
    md = _render_markdown(report_data, all_metrics, scores, baseline_metrics)
    md_path = run_dir / "eval_report.md"
    md_path.write_text(md, encoding="utf-8")

    # Write HTML
    html = _render_html(md, report_data)
    html_path = run_dir / "eval_report.html"
    html_path.write_text(html, encoding="utf-8")

    # Write JSON for UI consumption
    json_path = run_dir / "eval_report.json"
    json_path.write_text(json.dumps(report_data, indent=2, default=str), encoding="utf-8")

    logger.info("Eval report written to %s", run_dir)
    return report_data


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _read_parquet(path: Path) -> pd.DataFrame:
    """Read parquet if it exists, else return empty DataFrame."""
    if path.exists():
        try:
            return pd.read_parquet(path)
        except Exception as e:
            logger.warning("Failed to read %s: %s", path, e)
    return pd.DataFrame()


def _extract_price_series(prices_df: pd.DataFrame) -> pd.Series | None:
    """Extract the primary price series from prices.parquet."""
    if prices_df.empty or "last_price" not in prices_df.columns:
        return None
    if "base_symbol" in prices_df.columns:
        # Take first asset (typically BTC)
        first_asset = prices_df["base_symbol"].iloc[0]
        subset = prices_df[prices_df["base_symbol"] == first_asset]
        return subset["last_price"].reset_index(drop=True)
    return prices_df["last_price"].reset_index(drop=True)


def _build_caveats(
    mode: str, prices_df: pd.DataFrame, real_prices: pd.Series | None,
) -> list[str]:
    """Build a list of caveats for the report."""
    caveats: list[str] = []
    if mode != "historical":
        caveats.append("Non-historical mode: Tier A (price path) metrics not computed.")
    if real_prices is None:
        caveats.append("No ground-truth prices available.")
    if not prices_df.empty:
        n_steps = prices_df["step"].nunique() if "step" in prices_df.columns else 0
        caveats.append(f"Sim duration: {n_steps} steps.")
    caveats.append("Single-seed run. Multi-seed aggregation recommended (N>=5).")
    caveats.append("Commodity ground truth at daily resolution only (1m data is paid).")
    return caveats


def _pass_symbol(m: MetricResult) -> str:
    if m.passed is True:
        return "PASS"
    if m.passed is False:
        return "FAIL"
    return "--"


def _render_markdown(
    report_data: dict,
    metrics: list[MetricResult],
    scores: dict[str, float],
    baselines: dict[str, list[MetricResult]],
) -> str:
    """Render the eval report as markdown."""
    lines: list[str] = []
    lines.append("# Eval Report")
    lines.append("")
    lines.append(f"Generated: {report_data['generated_at']}")
    lines.append(f"Mode: {report_data['mode']}")
    lines.append(f"Run: {report_data['run_dir']}")
    lines.append("")

    # Score Vector
    lines.append("## Score Vector")
    lines.append("")
    lines.append("| Tier | Score |")
    lines.append("|------|-------|")
    for tier_name in TIERS:
        val = scores.get(tier_name, 0.0)
        bar = _score_bar(val)
        lines.append(f"| {tier_name} | {val:.2f} {bar} |")
    for extra in ["vs_random_walk", "vs_no_news"]:
        val = scores.get(extra, 0.0)
        lines.append(f"| {extra} | {val:.2f} |")
    lines.append("")

    # Per-tier tables
    tier_groups = _group_by_tier(metrics)
    for tier_name, tier_label in [
        ("price_path", "Tier A -- Price Path"),
        ("style_facts", "Tier B -- Style Facts"),
        ("microstructure", "Tier C -- Microstructure"),
        ("cross_asset", "Tier D -- Cross-Asset"),
        ("social", "Tier E -- Social"),
        ("agent_level", "Tier F -- Agent-Level"),
    ]:
        tier_metrics = tier_groups.get(tier_name, [])
        lines.append(f"## {tier_label}")
        lines.append("")
        if not tier_metrics:
            lines.append("_No metrics computed for this tier._")
            lines.append("")
            continue
        lines.append("| Metric | Value | Threshold | Pass |")
        lines.append("|--------|-------|-----------|------|")
        for m in tier_metrics:
            val_str = f"{m.value:.4f}" if not math.isnan(m.value) else "NaN"
            thr_str = f"{m.threshold:.4f}" if m.threshold is not None else "--"
            lines.append(f"| {m.name} | {val_str} | {thr_str} | {_pass_symbol(m)} |")
        lines.append("")

    # Baselines
    if baselines:
        lines.append("## Baselines")
        lines.append("")
        for bl_name, bl_metrics in baselines.items():
            lines.append(f"### {bl_name}")
            lines.append("| Metric | Baseline Value | Pass |")
            lines.append("|--------|---------------|------|")
            for m in bl_metrics:
                val_str = f"{m.value:.4f}" if not math.isnan(m.value) else "NaN"
                lines.append(f"| {m.name} | {val_str} | {_pass_symbol(m)} |")
            lines.append("")

    # Caveats
    lines.append("## Caveats")
    lines.append("")
    for c in report_data.get("caveats", []):
        lines.append(f"- {c}")
    lines.append("")

    return "\n".join(lines)


def _score_bar(val: float, width: int = 20) -> str:
    """ASCII progress bar for score 0-1."""
    filled = int(val * width)
    return "[" + "#" * filled + "." * (width - filled) + "]"


def _group_by_tier(metrics: list[MetricResult]) -> dict[str, list[MetricResult]]:
    """Group metrics by tier using the naming convention."""
    from oasis.crypto.eval.scoring import _TIER_MAP

    groups: dict[str, list[MetricResult]] = {}
    for m in metrics:
        tier = _TIER_MAP.get(m.name)
        if tier is None:
            # Check prefixes
            if m.name.startswith("action_dist_"):
                tier = "agent_level"
            elif m.name.startswith("vol_clustering"):
                tier = "style_facts"
            else:
                continue
        groups.setdefault(tier, []).append(m)
    return groups


def _render_html(markdown: str, report_data: dict) -> str:
    """Convert markdown report to HTML. Uses jinja2 if available."""
    try:
        import jinja2
    except ImportError:
        # Fallback: wrap markdown in basic HTML
        return f"<html><body><pre>{markdown}</pre></body></html>"

    template_str = """\
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Eval Report</title>
<style>
  body { font-family: 'JetBrains Mono', monospace; background: #0a0e14; color: #e4ecf7; padding: 2rem; }
  h1, h2, h3 { color: #00ddff; }
  table { border-collapse: collapse; width: 100%; margin: 1rem 0; }
  th, td { border: 1px solid #1e2838; padding: 0.5rem; text-align: left; }
  th { background: #121823; }
  .pass { color: #00ff88; }
  .fail { color: #ff3355; }
  .bar { background: #121823; border-radius: 4px; height: 20px; }
  .bar-fill { background: #00ddff; height: 100%; border-radius: 4px; }
  pre { background: #121823; padding: 1rem; overflow-x: auto; }
</style>
</head>
<body>
<h1>Eval Report</h1>
<p>Generated: {{ data.generated_at }} | Mode: {{ data.mode }}</p>

<h2>Score Vector</h2>
<table>
<tr><th>Tier</th><th>Score</th><th>Bar</th></tr>
{% for tier, score in data.score_vector.items() %}
<tr>
  <td>{{ tier }}</td>
  <td>{{ "%.2f"|format(score) }}</td>
  <td><div class="bar"><div class="bar-fill" style="width: {{ (score * 100)|int }}%"></div></div></td>
</tr>
{% endfor %}
</table>

<h2>Metrics</h2>
<table>
<tr><th>Name</th><th>Value</th><th>Threshold</th><th>Pass</th><th>Notes</th></tr>
{% for m in data.metrics %}
<tr>
  <td>{{ m.name }}</td>
  <td>{{ "%.4f"|format(m.value) if m.value == m.value else "NaN" }}</td>
  <td>{{ "%.4f"|format(m.threshold) if m.threshold else "--" }}</td>
  <td class="{{ 'pass' if m.passed == true else ('fail' if m.passed == false else '') }}">
    {{ "PASS" if m.passed == true else ("FAIL" if m.passed == false else "--") }}
  </td>
  <td>{{ m.notes or "" }}</td>
</tr>
{% endfor %}
</table>

<h2>Caveats</h2>
<ul>
{% for c in data.caveats %}
<li>{{ c }}</li>
{% endfor %}
</ul>
</body>
</html>
"""
    template = jinja2.Template(template_str)
    return template.render(data=report_data)
