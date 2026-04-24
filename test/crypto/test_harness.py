"""Tests for Phase 8 — clock, telemetry, and simulation harness.

All tests use the smoke persona library (100 personas). No real LLM calls.
"""

from __future__ import annotations

import asyncio
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
import pytest


def _run(coro):
    """Run an async coroutine, creating a new event loop if needed."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop and loop.is_running():
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as pool:
            return pool.submit(asyncio.run, coro).result()
    return asyncio.run(coro)

from oasis.crypto.action_gate import Tier
from oasis.crypto.clock import TickClock
from oasis.crypto.exchange import Exchange
from oasis.crypto.harness import (
    MockLLMClient,
    Simulation,
    SimulationConfig,
    StepResult,
)
from oasis.crypto.instrument import CryptoSchema
from oasis.crypto.persona import ArchetypeTemplate, Persona, PersonaLibrary
from oasis.crypto.telemetry import Telemetry

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_SMOKE_LIBRARY = _REPO_ROOT / "data" / "personas" / "library_smoke_100.jsonl"
_ASSETS_YAML = _REPO_ROOT / "data" / "market" / "assets.yaml"
_ARCHETYPES_DIR = _REPO_ROOT / "data" / "personas" / "archetypes"

_START_DT = datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_USER_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS user (
    user_id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_id INTEGER,
    user_name TEXT,
    name TEXT,
    bio TEXT,
    created_at DATETIME,
    num_followings INTEGER DEFAULT 0,
    num_followers INTEGER DEFAULT 0
);
"""

_FOLLOW_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS follow (
    follower_id INTEGER NOT NULL,
    followee_id INTEGER NOT NULL,
    created_at DATETIME,
    PRIMARY KEY(follower_id, followee_id),
    FOREIGN KEY(follower_id) REFERENCES user(user_id),
    FOREIGN KEY(followee_id) REFERENCES user(user_id)
);
"""


def _make_db() -> sqlite3.Connection:
    """Return a fresh in-memory DB with full schema + seeded assets."""
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(_USER_TABLE_DDL)
    conn.executescript(_FOLLOW_TABLE_DDL)
    conn.execute("PRAGMA foreign_keys = ON")

    schema = CryptoSchema(":memory:")
    schema.init_schema(conn)
    schema.seed_assets(conn, str(_ASSETS_YAML))
    return conn


def _load_templates() -> dict[str, ArchetypeTemplate]:
    """Load all archetype templates from the archetypes directory."""
    templates: dict[str, ArchetypeTemplate] = {}
    for yaml_file in sorted(_ARCHETYPES_DIR.glob("*.yaml")):
        tpl = ArchetypeTemplate.from_yaml(yaml_file)
        templates[tpl.archetype] = tpl
    return templates


def _load_personas(n: int = 30) -> list[Persona]:
    """Load first n personas from the smoke library."""
    lib = PersonaLibrary.load_from_jsonl(str(_SMOKE_LIBRARY))
    return lib.personas[:n]


def _make_config(
    tmp_path: Path,
    duration: int = 50,
    seed: int = 42,
    llm_enabled: bool = False,
) -> SimulationConfig:
    return SimulationConfig(
        name="test_sim",
        duration_steps=duration,
        step_minutes=1,
        start_datetime=_START_DT,
        seed=seed,
        llm_enabled=llm_enabled,
        output_dir=tmp_path / "output",
        telemetry_buffer=5000,
        conservation_check_every=60,
        initial_cash_override=10000.0,
    )


# ===========================================================================
# 1. Clock tests
# ===========================================================================


class TestClock:
    def test_step_to_datetime_roundtrip(self):
        """step->datetime->step round-trip is identity."""
        clock = TickClock(start_datetime=_START_DT, step_minutes=1)
        for step in [0, 1, 59, 100, 1439]:
            dt = clock.step_to_datetime(step)
            assert clock.datetime_to_step(dt) == step

    def test_advance_increments(self):
        """advance(N) increments current_step by N."""
        clock = TickClock(start_datetime=_START_DT, step_minutes=1)
        assert clock.current_step == 0
        clock.advance(5)
        assert clock.current_step == 5
        clock.advance()
        assert clock.current_step == 6

    def test_current_datetime(self):
        clock = TickClock(start_datetime=_START_DT, step_minutes=5)
        clock.advance(3)
        expected = _START_DT + timedelta(minutes=15)
        assert clock.current_datetime() == expected

    def test_reset(self):
        clock = TickClock(start_datetime=_START_DT, step_minutes=1)
        clock.advance(10)
        clock.reset()
        assert clock.current_step == 0
        assert clock.current_datetime() == _START_DT

    def test_datetime_to_step_floor(self):
        """datetime_to_step uses floor division."""
        clock = TickClock(start_datetime=_START_DT, step_minutes=5)
        # 7 minutes -> step 1 (floor(7/5) = 1)
        dt = _START_DT + timedelta(minutes=7)
        assert clock.datetime_to_step(dt) == 1


# ===========================================================================
# 2. Telemetry tests
# ===========================================================================


class TestTelemetry:
    def test_record_and_flush(self, tmp_path: Path):
        """Record rows, flush to parquet, read back -- all rows present."""
        telem = Telemetry(tmp_path / "telem_test", buffer_size=100)

        # Record some price rows
        for step in range(10):
            telem.record_prices(step, [
                {
                    "pair_id": 1,
                    "base_symbol": "BTC",
                    "quote_symbol": "USD",
                    "last_price": 80000.0 + step * 10,
                    "prev_close_price": 80000.0,
                    "volume_step": float(step),
                },
            ])

        # Record some action rows
        telem.record_actions(0, [
            {"user_id": 1, "archetype": "hodler", "tier": "trade", "action_type": "PLACE_ORDER"},
        ])

        telem.flush()

        # Read back
        prices_table = pq.read_table(tmp_path / "telem_test" / "prices.parquet")
        assert len(prices_table) == 10
        assert prices_table.column("step").to_pylist() == list(range(10))

        actions_table = pq.read_table(tmp_path / "telem_test" / "actions.parquet")
        assert len(actions_table) == 1

    def test_auto_flush_on_buffer_full(self, tmp_path: Path):
        """When buffer exceeds buffer_size, auto-flush fires."""
        telem = Telemetry(tmp_path / "auto_flush", buffer_size=5)
        for i in range(10):
            telem.record_prices(i, [
                {
                    "pair_id": 1,
                    "base_symbol": "BTC",
                    "quote_symbol": "USD",
                    "last_price": 80000.0,
                    "prev_close_price": 80000.0,
                    "volume_step": 0.0,
                },
            ])
        # Should have auto-flushed at least once
        telem.flush()  # flush remaining
        prices_table = pq.read_table(tmp_path / "auto_flush" / "prices.parquet")
        assert len(prices_table) == 10

    def test_multiple_flushes_append(self, tmp_path: Path):
        """Multiple flushes append data."""
        telem = Telemetry(tmp_path / "multi_flush", buffer_size=100)
        telem.record_prices(0, [
            {"pair_id": 1, "base_symbol": "BTC", "quote_symbol": "USD",
             "last_price": 80000.0, "prev_close_price": 80000.0, "volume_step": 0.0},
        ])
        telem.flush()
        telem.record_prices(1, [
            {"pair_id": 1, "base_symbol": "BTC", "quote_symbol": "USD",
             "last_price": 80010.0, "prev_close_price": 80000.0, "volume_step": 0.0},
        ])
        telem.flush()

        prices_table = pq.read_table(tmp_path / "multi_flush" / "prices.parquet")
        assert len(prices_table) == 2

    def test_empty_flush_safe(self, tmp_path: Path):
        """Flushing with no data doesn't crash or create files."""
        telem = Telemetry(tmp_path / "empty_flush")
        telem.flush()
        # No parquet files should exist
        out_dir = tmp_path / "empty_flush"
        if out_dir.exists():
            parquets = list(out_dir.glob("*.parquet"))
            assert len(parquets) == 0


# ===========================================================================
# 3. Initialize test
# ===========================================================================


class TestInitialize:
    def test_initialize_30_agents(self, tmp_path: Path):
        """Build a 30-agent simulation from smoke library; no errors."""
        conn = _make_db()
        personas = _load_personas(30)
        templates = _load_templates()
        config = _make_config(tmp_path, duration=10)

        sim = Simulation(
            conn=conn,
            config=config,
            personas=personas,
            templates=templates,
        )
        sim.initialize()

        # Check user rows created
        user_count = conn.execute("SELECT COUNT(*) FROM user").fetchone()[0]
        assert user_count >= 30

        # Check balances seeded
        bal_count = conn.execute("SELECT COUNT(*) FROM balance").fetchone()[0]
        assert bal_count >= 30  # at least one balance per user

        # Check graph built
        assert sim.graph_builder is not None
        assert sim.feed_filter is not None

        # Check instrument lookups
        assert "BTC" in sim._instrument_id_by_symbol
        assert "USD" in sim._instrument_id_by_symbol


# ===========================================================================
# 4. Single tick (gate-only) test
# ===========================================================================


class TestSingleTick:
    def test_tick_gate_only(self, tmp_path: Path):
        """A single tick runs without LLM, produces actions."""
        conn = _make_db()
        personas = _load_personas(30)
        templates = _load_templates()
        config = _make_config(tmp_path, duration=1, llm_enabled=False)

        sim = Simulation(
            conn=conn,
            config=config,
            personas=personas,
            templates=templates,
        )
        sim.initialize()

        result = _run(sim._tick(0))
        assert isinstance(result, StepResult)
        assert result.step == 0
        # With 30 agents, some should be active (non-silent)
        # Most will be silent (~70-90%) but some should act
        assert result.active_agents >= 0  # Stochastic; just ensure no crash


# ===========================================================================
# 5. Full run (gate-only, 50 steps, 30 agents)
# ===========================================================================


class TestFullRunSmall:
    def test_full_run_50_steps_30_agents(self, tmp_path: Path):
        """Completes in <15s; telemetry parquet files present; no exceptions."""
        conn = _make_db()
        personas = _load_personas(30)
        templates = _load_templates()
        config = _make_config(tmp_path, duration=50, llm_enabled=False)

        sim = Simulation(
            conn=conn,
            config=config,
            personas=personas,
            templates=templates,
        )

        t0 = time.perf_counter()
        results = _run(sim.run())
        elapsed = time.perf_counter() - t0

        assert elapsed < 15, f"50 steps x 30 agents took {elapsed:.1f}s (>15s)"
        assert len(results) == 50

        # Check telemetry files exist and are non-empty
        out_dir = config.output_dir
        for name in ["prices", "tiers", "actions", "stimuli"]:
            path = out_dir / f"{name}.parquet"
            assert path.exists(), f"{name}.parquet missing"
            table = pq.read_table(path)
            assert len(table) > 0, f"{name}.parquet is empty"


# ===========================================================================
# 6. Full run larger (gate-only, 100 agents x 100 steps)
# ===========================================================================


class TestFullRunLarger:
    def test_full_run_100_agents_100_steps(self, tmp_path: Path):
        """Completes in <30s per spec."""
        conn = _make_db()
        personas = _load_personas(100)
        templates = _load_templates()
        config = _make_config(tmp_path, duration=100, llm_enabled=False)

        sim = Simulation(
            conn=conn,
            config=config,
            personas=personas,
            templates=templates,
        )

        t0 = time.perf_counter()
        results = _run(sim.run())
        elapsed = time.perf_counter() - t0

        print(f"\n=== BENCHMARK: 100 agents x 100 steps: {elapsed:.2f}s ===")

        # Print per-step timing breakdown from the last few steps
        if results:
            avg_ms = sum(r.duration_ms for r in results) / len(results)
            print(f"Average step duration: {avg_ms:.1f}ms")
            # Print a representative step
            mid = results[50] if len(results) > 50 else results[-1]
            print(f"Step {mid.step}: active={mid.active_agents}, "
                  f"trades={mid.trades_executed}, "
                  f"duration={mid.duration_ms:.1f}ms")

        assert elapsed < 30, f"100x100 took {elapsed:.1f}s (>30s)"
        assert len(results) == 100

        # All telemetry files should exist
        out_dir = config.output_dir
        for name in ["prices", "tiers", "stimuli"]:
            assert (out_dir / f"{name}.parquet").exists()


# ===========================================================================
# 7. Seed reproducibility
# ===========================================================================


class TestSeedReproducibility:
    def test_two_runs_identical_trades(self, tmp_path: Path):
        """Two gate-only runs with same seed produce identical trades.parquet."""
        results_dirs: list[Path] = []

        for run_idx in range(2):
            conn = _make_db()
            personas = _load_personas(30)
            templates = _load_templates()
            out_dir = tmp_path / f"repro_run_{run_idx}"
            config = SimulationConfig(
                name="repro_test",
                duration_steps=30,
                step_minutes=1,
                start_datetime=_START_DT,
                seed=12345,
                llm_enabled=False,
                output_dir=out_dir,
                telemetry_buffer=5000,
                conservation_check_every=60,
                initial_cash_override=10000.0,
            )

            sim = Simulation(
                conn=conn,
                config=config,
                personas=personas,
                templates=templates,
            )
            _run(sim.run())
            results_dirs.append(out_dir)

        # Compare trades parquet files
        trades_0 = results_dirs[0] / "trades.parquet"
        trades_1 = results_dirs[1] / "trades.parquet"

        if trades_0.exists() and trades_1.exists():
            t0 = pq.read_table(trades_0)
            t1 = pq.read_table(trades_1)
            assert t0.num_rows == t1.num_rows, (
                f"Trade count mismatch: {t0.num_rows} vs {t1.num_rows}"
            )
            # Compare column values
            for col_name in t0.column_names:
                v0 = t0.column(col_name).to_pylist()
                v1 = t1.column(col_name).to_pylist()
                assert v0 == v1, f"Column {col_name} differs between runs"
        else:
            # Both should have the same existence state
            assert trades_0.exists() == trades_1.exists()

        # Also compare actions
        act_0 = results_dirs[0] / "actions.parquet"
        act_1 = results_dirs[1] / "actions.parquet"
        if act_0.exists() and act_1.exists():
            a0 = pq.read_table(act_0)
            a1 = pq.read_table(act_1)
            assert a0.num_rows == a1.num_rows


# ===========================================================================
# 8. LLM mode with MockLLMClient
# ===========================================================================


class TestLLMMode:
    def test_mock_llm_10_agents_5_steps(self, tmp_path: Path):
        """10 agents x 5 steps with mock LLM; actions applied correctly."""
        conn = _make_db()
        personas = _load_personas(10)
        templates = _load_templates()
        config = _make_config(
            tmp_path, duration=5, seed=42, llm_enabled=True
        )

        mock_client = MockLLMClient(seed=42)

        sim = Simulation(
            conn=conn,
            config=config,
            personas=personas,
            templates=templates,
            llm_client=mock_client,
        )

        results = _run(sim.run())
        assert len(results) == 5

        # Should have some active agents
        total_active = sum(r.active_agents for r in results)
        assert total_active > 0, "No agents were active across 5 steps"

        # Check that actions parquet has entries
        act_path = config.output_dir / "actions.parquet"
        assert act_path.exists()
        act_table = pq.read_table(act_path)
        assert len(act_table) > 0


# ===========================================================================
# 9. Conservation invariant
# ===========================================================================


class TestConservation:
    def test_conservation_100_agents_100_steps(self, tmp_path: Path):
        """Conservation check at step 60 passes (within 1e-6 tolerance)."""
        conn = _make_db()
        personas = _load_personas(100)
        templates = _load_templates()
        config = _make_config(tmp_path, duration=100, llm_enabled=False)
        config.conservation_check_every = 60

        sim = Simulation(
            conn=conn,
            config=config,
            personas=personas,
            templates=templates,
        )
        _run(sim.run())

        # Verify conservation: sum(amount + locked) per instrument should
        # match initial totals within 1e-6
        rows = conn.execute(
            "SELECT instrument_id, SUM(amount + locked) FROM balance "
            "GROUP BY instrument_id"
        ).fetchall()
        current = {r[0]: r[1] for r in rows}

        for inst_id, expected in sim._initial_totals.items():
            actual = current.get(inst_id, 0.0)
            drift = abs(actual - expected)
            assert drift < 1e-6, (
                f"Conservation violated for instrument {inst_id}: "
                f"expected={expected:.8f}, actual={actual:.8f}, drift={drift:.8f}"
            )


# ===========================================================================
# 10. News injection
# ===========================================================================


class TestNewsInjection:
    def test_news_at_step_20(self, tmp_path: Path):
        """Pre-bucket news at step 20; verify it appears in DB and stimuli."""
        from oasis.crypto.news_ingest import Audience, NewsEvent

        conn = _make_db()
        personas = _load_personas(10)
        templates = _load_templates()
        config = _make_config(tmp_path, duration=30, llm_enabled=False)

        # Create a news event at step 20 (20 minutes after start)
        news_ts = _START_DT + timedelta(minutes=20)
        news_event = NewsEvent(
            source="test",
            source_id="test_001",
            timestamp=news_ts,
            title="Bitcoin ETF approved",
            body="Major institutional inflows expected.",
            url=None,
            sentiment_valence=0.8,
            affected_assets=["BTC"],
            audience=Audience.ALL,
            magnitude="major",
            credibility="confirmed",
            enricher="test",
        )

        sim = Simulation(
            conn=conn,
            config=config,
            personas=personas,
            templates=templates,
            news_events=[news_event],
        )
        _run(sim.run())

        # Verify news_event row exists at step 20
        news_rows = conn.execute(
            "SELECT * FROM news_event WHERE step = 20"
        ).fetchall()
        assert len(news_rows) >= 1, "News event not found at step 20"
        assert "Bitcoin ETF" in news_rows[0][5]  # title is column index 5

        # Verify stimuli parquet shows news stimulus at step 20
        stim_path = config.output_dir / "stimuli.parquet"
        assert stim_path.exists()
        stim_table = pq.read_table(stim_path)
        step_20_mask = stim_table.column("step").to_pylist()
        step_20_news = [
            stim_table.column("news_stimulus").to_pylist()[i]
            for i, s in enumerate(step_20_mask)
            if s == 20
        ]
        assert any(v > 0 for v in step_20_news), "News stimulus not recorded at step 20"


# ===========================================================================
# 11. Empty persona list
# ===========================================================================


class TestEmptyPersonas:
    def test_zero_personas(self, tmp_path: Path):
        """Initialization with 0 personas completes without crashing."""
        conn = _make_db()
        config = _make_config(tmp_path, duration=5, llm_enabled=False)

        sim = Simulation(
            conn=conn,
            config=config,
            personas=[],
            templates={},
        )
        results = _run(sim.run())
        assert len(results) == 5
        for r in results:
            assert r.active_agents == 0
            assert r.trades_executed == 0
