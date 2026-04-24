#!/usr/bin/env python3
"""CLI: evaluate a simulation run against ground truth and baselines.

Usage::

    python scripts/evaluate.py results/luna_depeg/2026-04-24T12:30:00Z/ \\
        --mode historical --seeds auto
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Ensure project root is on sys.path
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Evaluate a crypto simulation run.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
  python scripts/evaluate.py results/quiet_market/run1/ --mode sanity
  python scripts/evaluate.py results/luna_depeg/run1/ --mode historical
  python scripts/evaluate.py results/multi_seed_run/ --seeds auto
""",
    )
    parser.add_argument(
        "run_dir",
        type=Path,
        help="Path to the simulation run output directory (contains parquet files).",
    )
    parser.add_argument(
        "--mode",
        choices=["historical", "sanity", "stress"],
        default="sanity",
        help="Eval mode (default: sanity).",
    )
    parser.add_argument(
        "--seeds",
        default="single",
        help="'single' for one run, 'auto' to detect multi-seed subdirs, or an integer count.",
    )
    parser.add_argument(
        "--gt-start",
        type=str,
        default=None,
        help="Ground truth start date (YYYY-MM-DD). Required for historical mode.",
    )
    parser.add_argument(
        "--gt-end",
        type=str,
        default=None,
        help="Ground truth end date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--assets",
        type=str,
        nargs="*",
        default=["BTC", "ETH"],
        help="Assets to evaluate (default: BTC ETH).",
    )

    args = parser.parse_args()

    if not args.run_dir.exists():
        print(f"Error: run_dir does not exist: {args.run_dir}", file=sys.stderr)
        sys.exit(1)

    # Import eval modules
    from oasis.crypto.eval.report import generate_report

    # Build ground truth if historical
    gt = None
    if args.mode == "historical" and args.gt_start and args.gt_end:
        from oasis.crypto.eval.ground_truth.registry import GroundTruth

        gt = GroundTruth(
            start=args.gt_start,
            end=args.gt_end,
            assets=args.assets,
        )

    # Detect multi-seed
    if args.seeds == "auto":
        seed_dirs = sorted(
            d for d in args.run_dir.iterdir()
            if d.is_dir() and (d / "prices.parquet").exists()
        )
        if seed_dirs:
            print(f"Detected {len(seed_dirs)} seed subdirs.")
            # For MVP: run on each seed, aggregate later
            for sd in seed_dirs:
                print(f"  Evaluating {sd.name}...")
                generate_report(sd, gt=gt, mode=args.mode)
            print("Multi-seed aggregation: see individual reports.")
        else:
            generate_report(args.run_dir, gt=gt, mode=args.mode)
    else:
        report_data = generate_report(args.run_dir, gt=gt, mode=args.mode)
        print(f"\nEval report written to {args.run_dir}/eval_report.md")
        print(f"Score vector: {report_data['score_vector']}")


if __name__ == "__main__":
    main()
