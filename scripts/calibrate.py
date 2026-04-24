#!/usr/bin/env python3
"""CLI: calibration sweep (stub for MVP).

Full calibration requires re-running simulations with varied parameters.
For now, this is a placeholder that documents the workflow.

Usage::

    python scripts/calibrate.py --help
"""

from __future__ import annotations

import argparse
import sys


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Calibration sweep for crypto simulation (MVP stub).",
    )
    parser.add_argument(
        "--scenario", type=str, default=None,
        help="Scenario YAML to calibrate against.",
    )
    parser.add_argument(
        "--param-grid", type=str, default=None,
        help="JSON file with parameter grid to sweep.",
    )
    _ = parser.parse_args()

    print(
        "Calibration sweep not implemented in MVP.\n"
        "\n"
        "To calibrate manually:\n"
        "  1. Edit scenario YAML knobs (population_mix, gate rates, stimulus weights).\n"
        "  2. Run: python scripts/run_scenario.py data/scenarios/your_scenario.yaml\n"
        "  3. Eval: python scripts/evaluate.py results/<run_dir>/ --mode historical\n"
        "  4. Compare score vectors across runs.\n"
        "  5. Iterate until Tier A scores meet thresholds.\n"
    )
    sys.exit(0)


if __name__ == "__main__":
    main()
