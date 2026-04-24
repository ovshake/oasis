#!/usr/bin/env python
"""CLI scenario runner for the crypto exchange simulation.

Phase 9 deliverable. Loads a scenario YAML, resolves prices + news + personas,
constructs a Simulation, runs it, and dumps parquet results.

Usage::

    python scripts/run_scenario.py scenarios/quiet_market.yaml [--no-llm] [--seed N]
    python scripts/run_scenario.py scenarios/fed_hawkish.yaml --no-llm --seed 123
    python scripts/run_scenario.py --help
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

# Ensure project root is on sys.path so imports work when running as a script
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# Lightweight oasis namespace — avoids pulling torch and other heavy deps
# from oasis/__init__.py which are irrelevant to the crypto subpackage.
import types as _types

if "oasis" not in sys.modules:
    _oasis_mod = _types.ModuleType("oasis")
    _oasis_mod.__path__ = [str(_PROJECT_ROOT / "oasis")]  # type: ignore[attr-defined]
    _oasis_mod.__package__ = "oasis"
    sys.modules["oasis"] = _oasis_mod

    _crypto_mod = _types.ModuleType("oasis.crypto")
    _crypto_mod.__path__ = [str(_PROJECT_ROOT / "oasis" / "crypto")]  # type: ignore[attr-defined]
    _crypto_mod.__package__ = "oasis.crypto"
    sys.modules["oasis.crypto"] = _crypto_mod

from oasis.crypto.harness import (
    AnthropicLLMClient,
    MockLLMClient,
    Simulation,
    SimulationConfig,
)
from oasis.crypto.instrument import CryptoSchema
from oasis.crypto.news_ingest import (
    Audience,
    CryptoPanicAdapter,
    ManualAdapter,
    NewsEvent,
    NewsItem,
    enrich,
    load_cache,
    mock_enricher,
    save_cache,
    cache_path as news_cache_path,
)
from oasis.crypto.persona import ArchetypeTemplate, Persona, PersonaLibrary
from oasis.crypto.price_fetch import resolve_initial_prices
from oasis.crypto.scenario import Scenario

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Archetype templates loader
# ---------------------------------------------------------------------------

_ARCHETYPES_DIR = _PROJECT_ROOT / "data" / "personas" / "archetypes"
_ASSETS_YAML = _PROJECT_ROOT / "data" / "market" / "assets.yaml"


def load_templates() -> dict[str, ArchetypeTemplate]:
    """Load all archetype YAML templates from data/personas/archetypes/."""
    templates: dict[str, ArchetypeTemplate] = {}
    for path in sorted(_ARCHETYPES_DIR.glob("*.yaml")):
        tmpl = ArchetypeTemplate.from_yaml(path)
        templates[tmpl.archetype] = tmpl
    return templates


# ---------------------------------------------------------------------------
# Persona resolution
# ---------------------------------------------------------------------------


def resolve_personas(
    scenario: Scenario,
    templates: dict[str, ArchetypeTemplate],
) -> list[Persona]:
    """Load or generate personas for the scenario.

    If ``scenario.persona_library`` exists, load from JSONL and sample
    according to the population mix. Otherwise, generate from archetype
    templates.
    """
    import numpy as np

    library_path = _PROJECT_ROOT / scenario.persona_library
    mix = scenario.population_mix.to_dict()

    if library_path.exists():
        library = PersonaLibrary.load_from_jsonl(library_path)
        return library.sample(mix, scenario.agents_count, seed=scenario.seed)

    # Fallback: generate from archetype templates
    logger.info(
        "Persona library not found at %s, generating from templates", library_path
    )
    rng = np.random.default_rng(scenario.seed)
    personas: list[Persona] = []

    # Allocate counts per archetype (largest-remainder)
    raw = {a: frac * scenario.agents_count for a, frac in mix.items()}
    floored = {a: int(v) for a, v in raw.items()}
    remainder = {a: raw[a] - floored[a] for a in raw}
    allocated = sum(floored.values())
    for a in sorted(remainder, key=remainder.get, reverse=True):  # type: ignore[arg-type]
        if allocated >= scenario.agents_count:
            break
        floored[a] += 1
        allocated += 1

    idx = 0
    for archetype, count in floored.items():
        tmpl = templates.get(archetype)
        if tmpl is None:
            logger.warning("No template for archetype %s, skipping", archetype)
            continue
        for _ in range(count):
            pid = f"p_{idx:06d}"
            personas.append(tmpl.sample_persona(pid, rng))
            idx += 1

    # Shuffle for interleaved archetypes
    rng.shuffle(personas)  # type: ignore[arg-type]
    return personas


# ---------------------------------------------------------------------------
# News resolution
# ---------------------------------------------------------------------------


def resolve_news(
    scenario: Scenario,
) -> list[NewsEvent]:
    """Resolve news events based on the scenario's news_source spec.

    Modes:
    - manual: convert manual_events to NewsEvent via mock_enricher.
    - historical: load from cache (or fetch via CryptoPanic) + enrich.
    - live_snapshot: fetch last N hours via CryptoPanic + enrich.
    """
    events: list[NewsEvent] = []
    ns = scenario.news_source
    start_dt = scenario.as_of_date or datetime.now(timezone.utc)

    # 1. Process manual_events (always, even in non-manual modes for overlays)
    if scenario.manual_events:
        manual_items: list[NewsItem] = []
        for me in scenario.manual_events:
            # Map step to a timestamp
            ts = start_dt + timedelta(minutes=me.step * scenario.step_minutes)
            manual_items.append(
                NewsItem(
                    source="manual",
                    timestamp=ts,
                    title=me.title or me.content[:80],
                    body=me.content,
                    source_prior_sentiment=(
                        "bullish" if me.sentiment > 0.2
                        else "bearish" if me.sentiment < -0.2
                        else "neutral"
                    ),
                    raw_metadata={
                        "step": me.step,
                        "assets": me.assets,
                        "audience": me.audience,
                    },
                )
            )
        enriched = enrich(manual_items, mock_enricher)
        # Patch audience and affected_assets from manual spec
        for ev, me in zip(enriched, scenario.manual_events):
            if me.assets:
                ev.affected_assets = list(me.assets)
            if me.audience != "all":
                try:
                    ev.audience = Audience(me.audience)
                except ValueError:
                    ev.audience = Audience.ALL
            ev.sentiment_valence = max(-1.0, min(1.0, me.sentiment))
        events.extend(enriched)

    # 2. Fetch-based news for historical / live_snapshot
    if ns.kind == "historical":
        if ns.date_range and len(ns.date_range) >= 2:
            dr_start, dr_end = ns.date_range[0], ns.date_range[1]
        else:
            # Default: scenario date +/- 1 day
            dr_start = start_dt - timedelta(days=1)
            dr_end = start_dt + timedelta(days=1)

        # Try cache first
        cp = news_cache_path(_PROJECT_ROOT / "data" / "market", dr_start, dr_end)
        cached = load_cache(cp)
        if cached:
            events.extend(cached)
        else:
            # Fetch from providers (only CryptoPanic for MVP)
            try:
                adapter = CryptoPanicAdapter()
                raw_items = adapter.fetch(dr_start, dr_end)
                enriched = enrich(raw_items, mock_enricher)
                save_cache(enriched, cp)
                events.extend(enriched)
            except Exception as exc:
                logger.warning("Historical news fetch failed: %s", exc)

    elif ns.kind == "live_snapshot":
        hours = ns.lookback_hours or 24
        snap_end = datetime.now(timezone.utc)
        snap_start = snap_end - timedelta(hours=hours)
        try:
            adapter = CryptoPanicAdapter()
            raw_items = adapter.fetch(snap_start, snap_end)
            enriched = enrich(raw_items, mock_enricher)
            events.extend(enriched)
        except Exception as exc:
            logger.warning("Live snapshot news fetch failed: %s", exc)

    return events


# ---------------------------------------------------------------------------
# Database seeding
# ---------------------------------------------------------------------------


def seed_database(
    conn: sqlite3.Connection,
    scenario: Scenario,
    resolved_prices: dict,
) -> None:
    """Initialize schema and seed assets with resolved prices.

    Updates pair.last_price to match the resolved initial price for each
    base asset.
    """
    schema = CryptoSchema(":memory:")
    schema.init_schema(conn)
    schema.seed_assets(conn, str(_ASSETS_YAML))

    # Override last_price on pairs with resolved prices
    for sym, pr in resolved_prices.items():
        if sym == "USD":
            continue
        row = conn.execute(
            "SELECT instrument_id FROM instrument WHERE symbol = ?", (sym,)
        ).fetchone()
        if row is None:
            continue
        inst_id = row[0]
        conn.execute(
            "UPDATE pair SET last_price = ? WHERE base_instrument_id = ?",
            (pr.price, inst_id),
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Main flow
# ---------------------------------------------------------------------------


def run_scenario(
    scenario_path: Path,
    no_llm: bool = False,
    seed_override: int | None = None,
) -> dict:
    """Execute a full scenario run. Returns a summary dict."""
    t0 = time.perf_counter()

    # 1. Load scenario
    scenario = Scenario.from_yaml(scenario_path)
    if seed_override is not None:
        scenario.seed = seed_override
    if no_llm:
        scenario.llm_enabled = False

    logger.info("Loaded scenario: %s (%d steps, %d agents)",
                scenario.name, scenario.duration_steps, scenario.agents_count)

    # 2. Resolve initial prices
    resolved_prices = resolve_initial_prices(
        symbols=scenario.assets,
        assets_yaml_path=_ASSETS_YAML,
        initial_prices=scenario.initial_prices or None,
        price_source=scenario.price_source,
        as_of_date=scenario.as_of_date,
    )
    logger.info("Resolved prices: %s",
                {s: f"{p.price:.2f} ({p.source})" for s, p in resolved_prices.items()})

    # 3. Load templates and resolve personas
    templates = load_templates()
    personas = resolve_personas(scenario, templates)
    logger.info("Resolved %d personas across %d archetypes",
                len(personas), len(set(p.archetype for p in personas)))

    # 4. Resolve news
    news_events = resolve_news(scenario)
    logger.info("Resolved %d news events", len(news_events))

    # 5. Set up database
    output_dir = scenario.resolve_output_dir()
    output_dir.mkdir(parents=True, exist_ok=True)

    db_path = output_dir / "simulation.db"
    conn = sqlite3.connect(str(db_path))
    seed_database(conn, scenario, resolved_prices)

    # 6. Construct SimulationConfig
    start_dt = scenario.as_of_date or datetime.now(timezone.utc)
    config = SimulationConfig(
        name=scenario.name,
        duration_steps=scenario.duration_steps,
        step_minutes=scenario.step_minutes,
        start_datetime=start_dt,
        seed=scenario.seed,
        llm_enabled=scenario.llm_enabled,
        output_dir=output_dir,
    )

    # 7. Pick LLM client
    if scenario.llm_enabled:
        llm_client = AnthropicLLMClient()
    else:
        llm_client = MockLLMClient(seed=scenario.seed)

    # 8. Construct and run simulation
    sim = Simulation(
        conn=conn,
        config=config,
        personas=personas,
        templates=templates,
        news_events=news_events,
        llm_client=llm_client,
    )

    results = asyncio.run(sim.run())

    # 9. Save metadata alongside parquet outputs
    prices_out = {s: {"price": p.price, "source": p.source} for s, p in resolved_prices.items()}
    (output_dir / "initial_prices.json").write_text(
        json.dumps(prices_out, indent=2, default=str), encoding="utf-8"
    )
    # Save a copy of the scenario config
    config_out = scenario.model_dump(mode="json")
    (output_dir / "config.yaml").write_text(
        yaml.dump(config_out, default_flow_style=False), encoding="utf-8"
    )

    conn.close()

    # 10. Summary
    wall_time = time.perf_counter() - t0
    total_trades = sum(r.trades_executed for r in results)
    total_posts = sum(r.posts_created for r in results)
    total_active = sum(r.active_agents for r in results)

    # List parquet files
    parquet_files = sorted(p.name for p in output_dir.glob("*.parquet"))

    summary = {
        "scenario": scenario.name,
        "output_dir": str(output_dir),
        "duration_steps": scenario.duration_steps,
        "agents_count": len(personas),
        "total_trades": total_trades,
        "total_posts": total_posts,
        "total_active_agent_steps": total_active,
        "news_events": len(news_events),
        "wall_time_seconds": round(wall_time, 2),
        "llm_enabled": scenario.llm_enabled,
        "parquet_files": parquet_files,
    }

    return summary


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run a crypto exchange simulation scenario.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python scripts/run_scenario.py scenarios/quiet_market.yaml --no-llm\n"
            "  python scripts/run_scenario.py scenarios/fed_hawkish.yaml --seed 123\n"
        ),
    )
    parser.add_argument(
        "scenario",
        type=Path,
        help="Path to the scenario YAML file.",
    )
    parser.add_argument(
        "--no-llm",
        action="store_true",
        default=False,
        help="Disable LLM calls; use gate-only heuristic mode.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Override the scenario's random seed.",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        default=False,
        help="Enable verbose logging.",
    )

    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    summary = run_scenario(
        scenario_path=args.scenario,
        no_llm=args.no_llm,
        seed_override=args.seed,
    )

    # Print summary
    print("\n" + "=" * 60)
    print(f"  Scenario: {summary['scenario']}")
    print(f"  Output:   {summary['output_dir']}")
    print(f"  Steps:    {summary['duration_steps']}")
    print(f"  Agents:   {summary['agents_count']}")
    print(f"  Trades:   {summary['total_trades']}")
    print(f"  Posts:    {summary['total_posts']}")
    print(f"  News:     {summary['news_events']}")
    print(f"  LLM:      {'enabled' if summary['llm_enabled'] else 'disabled (gate-only)'}")
    print(f"  Wall time: {summary['wall_time_seconds']:.1f}s")
    print(f"  Parquet:  {', '.join(summary['parquet_files'])}")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
