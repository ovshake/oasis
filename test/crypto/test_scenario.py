"""Tests for Phase 9: Scenario YAML, price_fetch, and CLI runner.

All external network calls are monkey-patched. No real API hits.
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from oasis.crypto.persona import (
    ArchetypeTemplate,
    Persona,
    PersonaLibrary,
)
from oasis.crypto.price_fetch import (
    PriceResolution,
    binance_klines,
    fetch_historical_prices,
    fetch_live_prices,
    resolve_initial_prices,
)
from oasis.crypto.scenario import (
    ManualNewsEvent,
    NewsSourceSpec,
    PopulationMix,
    Scenario,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_ASSETS_YAML = _PROJECT_ROOT / "data" / "market" / "assets.yaml"
_ARCHETYPES_DIR = _PROJECT_ROOT / "data" / "personas" / "archetypes"
_PYTHON = Path.home() / "venvs" / "aragen" / "bin" / "python"

_SCENARIO_FILES = [
    _PROJECT_ROOT / "scenarios" / "quiet_market.yaml",
    _PROJECT_ROOT / "scenarios" / "fed_hawkish.yaml",
    _PROJECT_ROOT / "scenarios" / "kol_pump.yaml",
    _PROJECT_ROOT / "scenarios" / "live_today.yaml",
    _PROJECT_ROOT / "calibration" / "luna_depeg.yaml",
]

ALL_ARCHETYPES = [
    "lurker", "hodler", "paperhands", "fomo_degen", "ta",
    "contrarian", "news_trader", "whale", "kol", "market_maker",
]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def archetype_templates() -> dict[str, ArchetypeTemplate]:
    templates: dict[str, ArchetypeTemplate] = {}
    for name in ALL_ARCHETYPES:
        path = _ARCHETYPES_DIR / f"{name}.yaml"
        templates[name] = ArchetypeTemplate.from_yaml(path)
    return templates


@pytest.fixture(scope="module")
def smoke_library(
    archetype_templates: dict[str, ArchetypeTemplate],
) -> PersonaLibrary:
    """Small 100-persona library for fast tests."""
    rng = np.random.default_rng(12345)
    personas: list[Persona] = []
    for arch_name, template in archetype_templates.items():
        for i in range(10):
            pid = f"p_{arch_name}_{i:03d}"
            persona = template.sample_persona(pid, rng)
            personas.append(persona)
    return PersonaLibrary(personas=personas)


@pytest.fixture
def tmp_cache_dir(tmp_path: Path) -> Path:
    d = tmp_path / "price_cache"
    d.mkdir()
    return d


# ---------------------------------------------------------------------------
# Binance klines fixture response
# ---------------------------------------------------------------------------

_BINANCE_FIXTURE = [
    [
        1651881600000,   # open_time
        "36000.00",      # open
        "36100.00",      # high
        "35900.00",      # low
        "36050.00",      # close
        "123.456",       # volume
        1651881659999,   # close_time
        "4446832.80",    # quote_asset_volume
        100,             # number_of_trades
        "61.728",        # taker_buy_base_asset_volume
        "2223416.40",    # taker_buy_quote_asset_volume
        "0",             # ignore
    ],
    [
        1651881660000,
        "36050.00",
        "36150.00",
        "35950.00",
        "36100.50",
        "98.765",
        1651881719999,
        "3564000.00",
        80,
        "49.383",
        "1782000.00",
        "0",
    ],
]


# ---------------------------------------------------------------------------
# Test 1: Scenario.from_yaml on each of the 5 fixtures
# ---------------------------------------------------------------------------


class TestScenarioYAMLLoading:
    """All 5 scenario fixtures must load and validate successfully."""

    @pytest.mark.parametrize("path", _SCENARIO_FILES, ids=lambda p: p.stem)
    def test_from_yaml_succeeds(self, path: Path) -> None:
        assert path.exists(), f"Missing fixture: {path}"
        scenario = Scenario.from_yaml(path)
        assert scenario.name
        assert scenario.duration_steps > 0
        assert scenario.agents_count > 0
        assert len(scenario.assets) >= 1

    def test_quiet_market_no_events(self) -> None:
        s = Scenario.from_yaml(_PROJECT_ROOT / "scenarios" / "quiet_market.yaml")
        assert s.name == "quiet_market"
        assert len(s.manual_events) == 0
        assert s.news_source.kind == "manual"

    def test_fed_hawkish_has_event(self) -> None:
        s = Scenario.from_yaml(_PROJECT_ROOT / "scenarios" / "fed_hawkish.yaml")
        assert len(s.manual_events) == 1
        assert s.manual_events[0].step == 50
        assert s.manual_events[0].sentiment == pytest.approx(-0.7)

    def test_kol_pump_two_events(self) -> None:
        s = Scenario.from_yaml(_PROJECT_ROOT / "scenarios" / "kol_pump.yaml")
        assert len(s.manual_events) == 2
        assert s.manual_events[0].audience == "kols"
        assert s.manual_events[1].audience == "all"

    def test_live_today_live_source(self) -> None:
        s = Scenario.from_yaml(_PROJECT_ROOT / "scenarios" / "live_today.yaml")
        assert s.price_source == "live"
        assert s.news_source.kind == "live_snapshot"
        assert s.news_source.lookback_hours == 24

    def test_luna_depeg_historical(self) -> None:
        s = Scenario.from_yaml(_PROJECT_ROOT / "calibration" / "luna_depeg.yaml")
        assert s.duration_steps == 2880
        assert s.agents_count == 1000
        assert s.initial_prices.get("USDT") == pytest.approx(0.95)
        assert s.as_of_date is not None

    def test_resolve_output_dir(self) -> None:
        s = Scenario.from_yaml(_PROJECT_ROOT / "scenarios" / "quiet_market.yaml")
        out = s.resolve_output_dir()
        assert "quiet_market" in str(out)


# ---------------------------------------------------------------------------
# Test 2: PopulationMix validator rejects non-sum-to-1.0
# ---------------------------------------------------------------------------


class TestPopulationMix:
    def test_default_sums_to_one(self) -> None:
        mix = PopulationMix()
        total = sum(mix.to_dict().values())
        assert abs(total - 1.0) < 1e-6

    def test_rejects_non_sum_to_one(self) -> None:
        with pytest.raises(ValueError, match="sum to 1.0"):
            PopulationMix(lurker=0.5, hodler=0.5, paperhands=0.1)

    def test_accepts_exact_one(self) -> None:
        mix = PopulationMix(
            lurker=0.50, hodler=0.10, paperhands=0.10,
            fomo_degen=0.08, ta=0.05, contrarian=0.03,
            news_trader=0.04, whale=0.05, kol=0.03, market_maker=0.02,
        )
        assert abs(sum(mix.to_dict().values()) - 1.0) < 1e-6

    def test_to_dict_keys(self) -> None:
        mix = PopulationMix()
        d = mix.to_dict()
        assert set(d.keys()) == set(ALL_ARCHETYPES)


# ---------------------------------------------------------------------------
# Test 3: resolve_initial_prices with price_source=default
# ---------------------------------------------------------------------------


class TestResolvePricesDefault:
    def test_default_returns_all_symbols(self, tmp_cache_dir: Path) -> None:
        symbols = ["BTC", "ETH", "USDT", "XAU", "WTI", "USD"]
        result = resolve_initial_prices(
            symbols=symbols,
            assets_yaml_path=_ASSETS_YAML,
            price_source="default",
            cache_dir=tmp_cache_dir,
        )
        assert set(result.keys()) == set(symbols)

    def test_default_btc_price(self, tmp_cache_dir: Path) -> None:
        result = resolve_initial_prices(
            symbols=["BTC"],
            assets_yaml_path=_ASSETS_YAML,
            price_source="default",
            cache_dir=tmp_cache_dir,
        )
        assert result["BTC"].price == pytest.approx(80000.0)
        assert result["BTC"].source == "default"

    def test_default_eth_price(self, tmp_cache_dir: Path) -> None:
        result = resolve_initial_prices(
            symbols=["ETH"],
            assets_yaml_path=_ASSETS_YAML,
            price_source="default",
            cache_dir=tmp_cache_dir,
        )
        assert result["ETH"].price == pytest.approx(3500.0)

    def test_default_usd_is_one(self, tmp_cache_dir: Path) -> None:
        result = resolve_initial_prices(
            symbols=["USD"],
            assets_yaml_path=_ASSETS_YAML,
            price_source="default",
            cache_dir=tmp_cache_dir,
        )
        assert result["USD"].price == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# Test 4: resolve_initial_prices with initial_prices override
# ---------------------------------------------------------------------------


class TestResolvePricesOverride:
    def test_usdt_override(self, tmp_cache_dir: Path) -> None:
        result = resolve_initial_prices(
            symbols=["USDT", "BTC"],
            assets_yaml_path=_ASSETS_YAML,
            initial_prices={"USDT": 0.95},
            price_source="default",
            cache_dir=tmp_cache_dir,
        )
        assert result["USDT"].price == pytest.approx(0.95)
        assert result["USDT"].source == "manual"

    def test_btc_override(self, tmp_cache_dir: Path) -> None:
        result = resolve_initial_prices(
            symbols=["BTC"],
            assets_yaml_path=_ASSETS_YAML,
            initial_prices={"BTC": 50000.0},
            price_source="default",
            cache_dir=tmp_cache_dir,
        )
        assert result["BTC"].price == pytest.approx(50000.0)
        assert result["BTC"].source == "manual"


# ---------------------------------------------------------------------------
# Test 5: Stablecoin peg snap
# ---------------------------------------------------------------------------


class TestStablecoinSnap:
    def test_usdt_snapped_to_peg(self, tmp_cache_dir: Path) -> None:
        """USDT defaults to 1.0 (peg_target) even if default_price differs."""
        result = resolve_initial_prices(
            symbols=["USDT"],
            assets_yaml_path=_ASSETS_YAML,
            price_source="default",
            snap_stablecoins_to_peg=True,
            cache_dir=tmp_cache_dir,
        )
        assert result["USDT"].price == pytest.approx(1.0)
        assert result["USDT"].source == "peg_snap"

    def test_snap_overrides_live_fetch(self, tmp_cache_dir: Path) -> None:
        """Even when live returns 0.9998, snap should force 1.0."""
        fake_live = {
            "USDT": PriceResolution(
                asset="USDT", price=0.9998, source="live",
                resolution="minute",
                fetched_at=datetime.now(timezone.utc),
            ),
        }
        with patch(
            "oasis.crypto.price_fetch.fetch_live_prices",
            return_value=fake_live,
        ):
            result = resolve_initial_prices(
                symbols=["USDT"],
                assets_yaml_path=_ASSETS_YAML,
                price_source="live",
                snap_stablecoins_to_peg=True,
                cache_dir=tmp_cache_dir,
            )
        assert result["USDT"].price == pytest.approx(1.0)
        assert result["USDT"].source == "peg_snap"

    def test_manual_override_beats_snap(self, tmp_cache_dir: Path) -> None:
        """Explicit initial_prices for USDT should NOT be snapped."""
        result = resolve_initial_prices(
            symbols=["USDT"],
            assets_yaml_path=_ASSETS_YAML,
            initial_prices={"USDT": 0.95},
            snap_stablecoins_to_peg=True,
            cache_dir=tmp_cache_dir,
        )
        assert result["USDT"].price == pytest.approx(0.95)
        assert result["USDT"].source == "manual"

    def test_snap_disabled(self, tmp_cache_dir: Path) -> None:
        """When snap_stablecoins_to_peg=False, USDT gets default_price."""
        result = resolve_initial_prices(
            symbols=["USDT"],
            assets_yaml_path=_ASSETS_YAML,
            snap_stablecoins_to_peg=False,
            cache_dir=tmp_cache_dir,
        )
        # Should use default_price from assets.yaml (1.0), not peg_snap
        assert result["USDT"].source == "default"


# ---------------------------------------------------------------------------
# Test 6: binance_klines monkey-patched
# ---------------------------------------------------------------------------


class TestBinanceKlines:
    def test_parses_fixture_response(self) -> None:
        mock_response = MagicMock()
        mock_response.json.return_value = _BINANCE_FIXTURE
        mock_response.raise_for_status = MagicMock()

        with patch("oasis.crypto.price_fetch.requests.get" if hasattr(sys.modules.get("oasis.crypto.price_fetch", None), "requests") else "requests.get") as mock_get:
            # We need to patch within the module
            import oasis.crypto.price_fetch as pf
            with patch.object(pf, "binance_klines", wraps=pf.binance_klines):
                with patch("requests.get", return_value=mock_response):
                    result = binance_klines(
                        "BTCUSDT", "1m",
                        start_ms=1651881600000,
                        end_ms=1651881720000,
                        limit=2,
                    )

        assert len(result) == 2
        assert result[0]["open_time"] == 1651881600000
        assert result[0]["close"] == pytest.approx(36050.0)
        assert result[1]["close"] == pytest.approx(36100.50)
        assert "volume" in result[0]
        assert "high" in result[0]
        assert "low" in result[0]

    def test_empty_response(self) -> None:
        mock_response = MagicMock()
        mock_response.json.return_value = []
        mock_response.raise_for_status = MagicMock()

        with patch("requests.get", return_value=mock_response):
            result = binance_klines("BTCUSDT", "1m", limit=1)

        assert result == []


# ---------------------------------------------------------------------------
# Test 7: fetch_historical_prices uses cache on second call
# ---------------------------------------------------------------------------


class TestHistoricalPriceCache:
    def test_cache_hit_no_network(self, tmp_cache_dir: Path) -> None:
        as_of = datetime(2022, 5, 7, 12, 0, 0, tzinfo=timezone.utc)

        # Pre-populate cache
        cache_file = tmp_cache_dir / "2022-05-07.json"
        cache_data = {"BTC": 36000.0, "ETH": 2800.0}
        cache_file.write_text(json.dumps(cache_data))

        # Patch network calls to ensure they are NOT called
        with patch("oasis.crypto.price_fetch._fetch_crypto_historical") as mock_crypto, \
             patch("oasis.crypto.price_fetch._fetch_commodity_historical") as mock_commodity:
            result = fetch_historical_prices(
                ["BTC", "ETH"],
                as_of,
                assets_yaml_path=_ASSETS_YAML,
                cache_dir=tmp_cache_dir,
            )

        # Network should not be called
        mock_crypto.assert_not_called()
        mock_commodity.assert_not_called()

        # Results from cache
        assert result["BTC"].price == pytest.approx(36000.0)
        assert result["ETH"].price == pytest.approx(2800.0)
        assert result["BTC"].source == "historical"

    def test_cache_miss_fetches_and_caches(self, tmp_cache_dir: Path) -> None:
        as_of = datetime(2022, 6, 15, 12, 0, 0, tzinfo=timezone.utc)

        with patch(
            "oasis.crypto.price_fetch._fetch_crypto_historical",
            return_value={"BTC": 21000.0},
        ), patch(
            "oasis.crypto.price_fetch._fetch_commodity_historical",
            return_value={},
        ):
            result = fetch_historical_prices(
                ["BTC"],
                as_of,
                assets_yaml_path=_ASSETS_YAML,
                cache_dir=tmp_cache_dir,
            )

        assert result["BTC"].price == pytest.approx(21000.0)

        # Check cache was written
        cache_file = tmp_cache_dir / "2022-06-15.json"
        assert cache_file.exists()
        cached = json.loads(cache_file.read_text())
        assert cached["BTC"] == pytest.approx(21000.0)


# ---------------------------------------------------------------------------
# Test 8: End-to-end smoke via subprocess
# ---------------------------------------------------------------------------


class TestEndToEndSmoke:
    def test_quiet_market_e2e(self, tmp_path: Path) -> None:
        """Run quiet_market scenario via subprocess; check exit 0 + parquet output."""
        scenario_path = _PROJECT_ROOT / "scenarios" / "quiet_market.yaml"

        # Write a temporary scenario pointing output to tmp_path
        import yaml
        raw = yaml.safe_load(scenario_path.read_text())
        raw["output_dir"] = str(tmp_path / "output")
        raw["agents_count"] = 20  # minimal for speed
        raw["duration_steps"] = 10  # minimal for speed

        tmp_scenario = tmp_path / "test_scenario.yaml"
        tmp_scenario.write_text(yaml.dump(raw))

        result = subprocess.run(
            [
                str(_PYTHON),
                str(_PROJECT_ROOT / "scripts" / "run_scenario.py"),
                str(tmp_scenario),
                "--no-llm",
                "--seed", "42",
            ],
            capture_output=True,
            text=True,
            timeout=120,
            cwd=str(_PROJECT_ROOT),
        )

        assert result.returncode == 0, (
            f"Scenario runner failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
        )

        # Check output directory has parquet files.
        # output_dir is set to a flat path (no {name}/{timestamp} template),
        # so parquet files land directly in the output dir.
        out = tmp_path / "output"
        assert out.exists(), f"Output dir not created: {out}"
        parquet_files = list(out.glob("*.parquet"))
        assert len(parquet_files) >= 1, (
            f"No parquet files in {out}. Contents: {list(out.iterdir())}"
        )

        # Check metadata files
        assert (out / "initial_prices.json").exists()
        assert (out / "config.yaml").exists()


# ---------------------------------------------------------------------------
# Test 9: CLI --help exits 0
# ---------------------------------------------------------------------------


class TestCLIHelp:
    def test_help_exits_zero(self) -> None:
        result = subprocess.run(
            [
                str(_PYTHON),
                str(_PROJECT_ROOT / "scripts" / "run_scenario.py"),
                "--help",
            ],
            capture_output=True,
            text=True,
            timeout=30,
            cwd=str(_PROJECT_ROOT),
        )
        assert result.returncode == 0
        assert "scenario" in result.stdout.lower()
        assert "--no-llm" in result.stdout


# ---------------------------------------------------------------------------
# Additional edge case tests
# ---------------------------------------------------------------------------


class TestScenarioEdgeCases:
    def test_news_source_spec_defaults(self) -> None:
        ns = NewsSourceSpec()
        assert ns.kind == "manual"
        assert ns.enrich_with == "mock"

    def test_manual_news_event_defaults(self) -> None:
        ev = ManualNewsEvent(step=10, content="test")
        assert ev.sentiment == 0.0
        assert ev.audience == "all"
        assert ev.assets == []

    def test_scenario_defaults(self) -> None:
        s = Scenario(name="test")
        assert s.duration_steps == 240
        assert s.seed == 42
        assert "BTC" in s.assets
        assert s.price_source == "default"

    def test_price_resolution_model(self) -> None:
        pr = PriceResolution(
            asset="BTC",
            price=80000.0,
            source="default",
            resolution="minute",
            fetched_at=datetime.now(timezone.utc),
        )
        assert pr.asset == "BTC"
        assert pr.price == 80000.0
