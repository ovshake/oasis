#!/usr/bin/env python
"""Offline news ingestion CLI: fetch, enrich, and cache news items.

Usage examples::

    # Fetch from CryptoPanic for a date range, enrich with mock enricher
    python scripts/ingest_news.py \\
        --providers cryptopanic \\
        --start 2022-05-07 \\
        --end 2022-05-11 \\
        --enricher mock \\
        --output data/market/news_cache/

    # Load manual events from a YAML file
    python scripts/ingest_news.py \\
        --providers manual \\
        --manual-file scenarios/fed_hawkish_news.yaml \\
        --start 2022-05-07 \\
        --end 2022-05-11 \\
        --enricher mock \\
        --output data/market/news_cache/
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

# Ensure project root is importable when running as a script.
# Register oasis + oasis.crypto as lightweight namespace stubs to avoid
# pulling in the full OASIS dependency tree (torch, camel-ai, etc.).
import types

_SCRIPT_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _SCRIPT_DIR.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

_oasis_dir = _PROJECT_ROOT / "oasis"
_crypto_dir = _oasis_dir / "crypto"
if "oasis" not in sys.modules:
    _oasis_mod = types.ModuleType("oasis")
    _oasis_mod.__path__ = [str(_oasis_dir)]  # type: ignore[attr-defined]
    _oasis_mod.__package__ = "oasis"
    sys.modules["oasis"] = _oasis_mod

    _crypto_mod = types.ModuleType("oasis.crypto")
    _crypto_mod.__path__ = [str(_crypto_dir)]  # type: ignore[attr-defined]
    _crypto_mod.__package__ = "oasis.crypto"
    sys.modules["oasis.crypto"] = _crypto_mod

from oasis.crypto.news_ingest import (
    CryptoPanicAdapter,
    ManualAdapter,
    NewsAdapter,
    NewsItem,
    cache_path,
    dedupe_items,
    enrich,
    mock_enricher,
    save_cache,
    sonnet_enricher_stub,
)


def _parse_date(s: str) -> datetime:
    """Parse a YYYY-MM-DD string to a UTC-aware datetime at midnight."""
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def _load_manual_file(path: Path) -> list[dict]:
    """Load a YAML file containing a list of news event dicts.

    The file should be a YAML list (``- timestamp: ...``) or a mapping
    with a top-level ``events`` key containing the list.
    """
    import yaml

    with open(path, "r") as f:
        data = yaml.safe_load(f)
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and "events" in data:
        return data["events"]
    raise ValueError(
        f"Expected a YAML list or a mapping with an 'events' key in {path}"
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Fetch, enrich, and cache news items for crypto simulation.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Providers:\n"
            "  cryptopanic   CryptoPanic free-tier API\n"
            "  manual        Load from a YAML file (requires --manual-file)\n"
        ),
    )
    parser.add_argument(
        "--providers",
        nargs="+",
        required=True,
        choices=["cryptopanic", "manual"],
        help="News providers to fetch from.",
    )
    parser.add_argument(
        "--start",
        required=True,
        help="Start date (YYYY-MM-DD, inclusive).",
    )
    parser.add_argument(
        "--end",
        required=True,
        help="End date (YYYY-MM-DD, inclusive).",
    )
    parser.add_argument(
        "--enricher",
        default="mock",
        choices=["mock", "sonnet"],
        help="Enrichment strategy (default: mock).",
    )
    parser.add_argument(
        "--output",
        default="data/market/",
        help="Base output directory (cache stored under news_cache/ subdir).",
    )
    parser.add_argument(
        "--manual-file",
        default=None,
        help="Path to YAML file for the manual provider.",
    )
    parser.add_argument(
        "--cryptopanic-token",
        default=None,
        help="CryptoPanic API auth token (optional; public=true works without).",
    )

    args = parser.parse_args(argv)

    start = _parse_date(args.start)
    end = _parse_date(args.end)

    # Build adapters
    adapters: list[NewsAdapter] = []
    for provider in args.providers:
        if provider == "cryptopanic":
            adapters.append(CryptoPanicAdapter(auth_token=args.cryptopanic_token))
        elif provider == "manual":
            if not args.manual_file:
                parser.error("--manual-file is required when using the manual provider")
            items_data = _load_manual_file(Path(args.manual_file))
            adapters.append(ManualAdapter(items=items_data))

    # Fetch
    all_items: list[NewsItem] = []
    for adapter in adapters:
        fetched = adapter.fetch(start, end)
        all_items.extend(fetched)
        print(f"[{adapter.source_name}] fetched {len(fetched)} items")

    # Dedupe
    before_dedup = len(all_items)
    all_items = dedupe_items(all_items)
    deduped = before_dedup - len(all_items)
    if deduped:
        print(f"Deduped {deduped} items ({before_dedup} -> {len(all_items)})")

    # Enrich
    if args.enricher == "sonnet":
        enricher_fn = sonnet_enricher_stub
    else:
        enricher_fn = mock_enricher

    events = enrich(all_items, enricher=enricher_fn)

    # Cache
    output_dir = Path(args.output)
    out_path = cache_path(output_dir, start, end)
    save_cache(events, out_path)

    print(
        f"{len(all_items)} items fetched, {len(events)} enriched, "
        f"wrote to {out_path}"
    )


if __name__ == "__main__":
    main()
