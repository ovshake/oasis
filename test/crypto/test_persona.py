"""Tests for Phase 3: Persona dataclass, archetype templates, persona library."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import numpy as np
import pytest

from oasis.crypto.persona import (
    ActionBaseRates,
    ArchetypeTemplate,
    Distribution,
    HardRule,
    Persona,
    PersonaLibrary,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ARCHETYPES_DIR = Path(__file__).resolve().parents[2] / "data" / "personas" / "archetypes"

ALL_ARCHETYPES = [
    "lurker",
    "hodler",
    "paperhands",
    "fomo_degen",
    "ta",
    "contrarian",
    "news_trader",
    "whale",
    "kol",
    "market_maker",
]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def archetype_templates() -> dict[str, ArchetypeTemplate]:
    """Load all 10 archetype templates from YAML."""
    templates: dict[str, ArchetypeTemplate] = {}
    for name in ALL_ARCHETYPES:
        path = ARCHETYPES_DIR / f"{name}.yaml"
        templates[name] = ArchetypeTemplate.from_yaml(path)
    return templates


@pytest.fixture(scope="module")
def test_library(archetype_templates: dict[str, ArchetypeTemplate]) -> PersonaLibrary:
    """Build a 100-persona test library by sampling 10 from each archetype."""
    rng = np.random.default_rng(12345)
    personas: list[Persona] = []
    for arch_name, template in archetype_templates.items():
        for i in range(10):
            pid = f"p_{arch_name}_{i:03d}"
            persona = template.sample_persona(pid, rng)
            personas.append(persona)
    return PersonaLibrary(personas=personas)


# ---------------------------------------------------------------------------
# Test: all 10 archetype YAMLs load and validate
# ---------------------------------------------------------------------------

class TestArchetypeLoading:
    """All 10 archetype YAMLs must load into valid ArchetypeTemplate objects."""

    @pytest.mark.parametrize("archetype", ALL_ARCHETYPES)
    def test_yaml_loads(self, archetype: str) -> None:
        path = ARCHETYPES_DIR / f"{archetype}.yaml"
        assert path.exists(), f"Missing YAML: {path}"
        template = ArchetypeTemplate.from_yaml(path)
        assert template.archetype == archetype

    @pytest.mark.parametrize("archetype", ALL_ARCHETYPES)
    def test_base_rates_sum_to_one(self, archetype: str) -> None:
        template = ArchetypeTemplate.from_yaml(ARCHETYPES_DIR / f"{archetype}.yaml")
        rates = template.action_base_rates
        total = rates.silent + rates.react + rates.comment + rates.post + rates.trade
        assert abs(total - 1.0) < 1e-6, f"{archetype} rates sum to {total}"

    @pytest.mark.parametrize("archetype", ALL_ARCHETYPES)
    def test_distributions_are_sampleable(self, archetype: str) -> None:
        template = ArchetypeTemplate.from_yaml(ARCHETYPES_DIR / f"{archetype}.yaml")
        rng = np.random.default_rng(42)
        # All numeric distributions must sample without error
        template.risk_tolerance.sample(rng)
        template.time_horizon_minutes.sample(rng)
        template.social_sensitivity.sample(rng)
        template.herding_coefficient.sample(rng)
        template.capital_usd.sample(rng)
        template.target_follow_count.sample(rng)

    def test_all_ten_exist(self, archetype_templates: dict[str, ArchetypeTemplate]) -> None:
        assert len(archetype_templates) == 10
        assert set(archetype_templates.keys()) == set(ALL_ARCHETYPES)


# ---------------------------------------------------------------------------
# Test: Persona round-trip JSON serialization
# ---------------------------------------------------------------------------

class TestPersonaSerialization:
    """Persona must round-trip: create -> to_json -> from_json -> equality."""

    def _make_persona(self) -> Persona:
        return Persona(
            persona_id="p_test_001",
            archetype="hodler",
            name="Test HODLer",
            backstory="A steadfast believer in BTC since 2013.",
            voice_style="Calm and measured. Says 'zoom out' a lot.",
            risk_tolerance=0.35,
            time_horizon_minutes=50000,
            social_sensitivity=0.45,
            herding_coefficient=-0.1,
            capital_usd=25000.0,
            action_base_rates=ActionBaseRates(
                silent=0.90, react=0.05, comment=0.02, post=0.01, trade=0.02
            ),
            hard_rules=[
                HardRule(
                    name="no_sell_drawdown",
                    description="Won't sell during drawdowns",
                    condition="drawdown_pct > 0.20",
                    effect="block_action:SELL",
                )
            ],
            initial_holdings={"USD": 25000.0, "BTC": 0.5},
            follows_archetypes={"hodler": 2.0, "news_trader": 1.5},
            generated_by="test",
            generated_at="2026-01-01T00:00:00",
        )

    def test_json_round_trip(self) -> None:
        original = self._make_persona()
        json_str = original.model_dump_json()
        restored = Persona.model_validate_json(json_str)
        assert restored == original

    def test_dict_round_trip(self) -> None:
        original = self._make_persona()
        d = original.model_dump()
        restored = Persona.model_validate(d)
        assert restored == original

    def test_json_contains_all_fields(self) -> None:
        persona = self._make_persona()
        d = json.loads(persona.model_dump_json())
        assert d["persona_id"] == "p_test_001"
        assert d["archetype"] == "hodler"
        assert d["risk_tolerance"] == 0.35
        assert len(d["hard_rules"]) == 1
        assert d["initial_holdings"]["BTC"] == 0.5


# ---------------------------------------------------------------------------
# Test: ArchetypeTemplate.sample_persona
# ---------------------------------------------------------------------------

class TestArchetypeSampling:
    """ArchetypeTemplate can sample valid Persona instances."""

    @pytest.mark.parametrize("archetype", ALL_ARCHETYPES)
    def test_sample_produces_valid_persona(
        self,
        archetype: str,
        archetype_templates: dict[str, ArchetypeTemplate],
    ) -> None:
        template = archetype_templates[archetype]
        rng = np.random.default_rng(99)
        persona = template.sample_persona(f"p_{archetype}_test", rng)
        assert persona.archetype == archetype
        assert 0.0 <= persona.risk_tolerance <= 1.0
        assert persona.time_horizon_minutes >= 1
        assert 0.0 <= persona.social_sensitivity <= 1.0
        assert -1.0 <= persona.herding_coefficient <= 1.0
        assert persona.capital_usd > 0

    def test_different_seeds_different_results(
        self,
        archetype_templates: dict[str, ArchetypeTemplate],
    ) -> None:
        template = archetype_templates["fomo_degen"]
        p1 = template.sample_persona("p1", np.random.default_rng(1))
        p2 = template.sample_persona("p2", np.random.default_rng(2))
        # At least one axis should differ
        assert (
            p1.risk_tolerance != p2.risk_tolerance
            or p1.capital_usd != p2.capital_usd
        )

    def test_same_seed_same_result(
        self,
        archetype_templates: dict[str, ArchetypeTemplate],
    ) -> None:
        template = archetype_templates["whale"]
        p1 = template.sample_persona("p1", np.random.default_rng(42))
        p2 = template.sample_persona("p1", np.random.default_rng(42))
        assert p1.risk_tolerance == p2.risk_tolerance
        assert p1.capital_usd == p2.capital_usd
        assert p1.time_horizon_minutes == p2.time_horizon_minutes


# ---------------------------------------------------------------------------
# Test: PersonaLibrary JSONL round-trip
# ---------------------------------------------------------------------------

class TestPersonaLibraryPersistence:
    """PersonaLibrary round-trips through JSONL."""

    def test_jsonl_round_trip(self, test_library: PersonaLibrary) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "test_lib.jsonl"
            test_library.save_to_jsonl(path)
            restored = PersonaLibrary.load_from_jsonl(path)
            assert len(restored.personas) == len(test_library.personas)
            for orig, rest in zip(test_library.personas, restored.personas):
                assert orig.persona_id == rest.persona_id
                assert orig.archetype == rest.archetype
                assert orig == rest

    def test_get_by_id(self, test_library: PersonaLibrary) -> None:
        first = test_library.personas[0]
        found = test_library.get_by_id(first.persona_id)
        assert found == first

    def test_get_by_id_missing(self, test_library: PersonaLibrary) -> None:
        with pytest.raises(KeyError):
            test_library.get_by_id("nonexistent_id")

    def test_by_archetype(self, test_library: PersonaLibrary) -> None:
        hodlers = test_library.by_archetype("hodler")
        assert len(hodlers) == 10
        assert all(p.archetype == "hodler" for p in hodlers)

    def test_distribution_summary(self, test_library: PersonaLibrary) -> None:
        summary = test_library.distribution_summary()
        assert len(summary) == 10
        assert all(v == 10 for v in summary.values())


# ---------------------------------------------------------------------------
# Test: PersonaLibrary.sample respects mix
# ---------------------------------------------------------------------------

class TestPersonaLibrarySampling:
    """Sampling from PersonaLibrary must respect archetype mix fractions."""

    def test_sample_respects_mix(self, test_library: PersonaLibrary) -> None:
        """Sample 100 from a 50/50 hodler/fomo_degen mix.

        With n=100 and p=0.5, the 3-sigma Poisson band is roughly 37-63.
        We use 40-60 as the spec says.
        """
        mix = {"hodler": 0.5, "fomo_degen": 0.5}
        sampled = test_library.sample(mix=mix, count=100, seed=42)
        assert len(sampled) == 100

        counts: dict[str, int] = {}
        for p in sampled:
            counts[p.archetype] = counts.get(p.archetype, 0) + 1

        assert 40 <= counts.get("hodler", 0) <= 60, f"hodler count: {counts.get('hodler', 0)}"
        assert 40 <= counts.get("fomo_degen", 0) <= 60, f"fomo_degen count: {counts.get('fomo_degen', 0)}"

    def test_sample_deterministic(self, test_library: PersonaLibrary) -> None:
        """Same seed must produce identical output."""
        mix = {"lurker": 0.3, "kol": 0.3, "ta": 0.4}
        s1 = test_library.sample(mix=mix, count=50, seed=123)
        s2 = test_library.sample(mix=mix, count=50, seed=123)
        assert len(s1) == len(s2)
        for a, b in zip(s1, s2):
            assert a.persona_id == b.persona_id

    def test_sample_different_seed_different_output(self, test_library: PersonaLibrary) -> None:
        mix = {"lurker": 0.5, "whale": 0.5}
        s1 = test_library.sample(mix=mix, count=50, seed=1)
        s2 = test_library.sample(mix=mix, count=50, seed=2)
        ids1 = [p.persona_id for p in s1]
        ids2 = [p.persona_id for p in s2]
        assert ids1 != ids2

    def test_sample_mix_must_sum_to_one(self, test_library: PersonaLibrary) -> None:
        with pytest.raises(ValueError, match="sum to 1.0"):
            test_library.sample(mix={"hodler": 0.3, "lurker": 0.3}, count=10)

    def test_sample_three_way_mix(self, test_library: PersonaLibrary) -> None:
        """Three-archetype mix with uneven fractions."""
        mix = {"lurker": 0.6, "hodler": 0.3, "whale": 0.1}
        sampled = test_library.sample(mix=mix, count=200, seed=7)
        counts: dict[str, int] = {}
        for p in sampled:
            counts[p.archetype] = counts.get(p.archetype, 0) + 1

        # Check each archetype is within a reasonable band
        assert 100 <= counts.get("lurker", 0) <= 140
        assert 45 <= counts.get("hodler", 0) <= 75
        assert 10 <= counts.get("whale", 0) <= 30


# ---------------------------------------------------------------------------
# Test: Distribution sampling edge cases
# ---------------------------------------------------------------------------

class TestDistribution:

    def test_constant(self) -> None:
        d = Distribution(kind="constant", value=3.14)
        assert d.sample(np.random.default_rng(0)) == 3.14

    def test_uniform(self) -> None:
        d = Distribution(kind="uniform", min=0.0, max=1.0)
        rng = np.random.default_rng(42)
        vals = [d.sample(rng) for _ in range(1000)]
        assert all(0.0 <= v <= 1.0 for v in vals)

    def test_loguniform(self) -> None:
        d = Distribution(kind="loguniform", min=10.0, max=10000.0)
        rng = np.random.default_rng(42)
        vals = [d.sample(rng) for _ in range(1000)]
        assert all(10.0 <= v <= 10000.0 for v in vals)
        # Should be log-spread: median much closer to geometric mean
        import statistics
        median = statistics.median(vals)
        geo_mean = (10.0 * 10000.0) ** 0.5  # ~316
        assert median < 1000, "Loguniform median should be well below arithmetic midpoint"

    def test_normal(self) -> None:
        d = Distribution(kind="normal", mean=50.0, std=5.0)
        rng = np.random.default_rng(42)
        vals = [d.sample(rng) for _ in range(1000)]
        avg = sum(vals) / len(vals)
        assert 45.0 < avg < 55.0

    def test_missing_params_raises(self) -> None:
        d = Distribution(kind="uniform")
        with pytest.raises(ValueError):
            d.sample(np.random.default_rng(0))


# ---------------------------------------------------------------------------
# Test: ActionBaseRates validation
# ---------------------------------------------------------------------------

class TestActionBaseRates:

    def test_valid_rates(self) -> None:
        r = ActionBaseRates(silent=0.9, react=0.05, comment=0.02, post=0.01, trade=0.02)
        assert abs(r.silent + r.react + r.comment + r.post + r.trade - 1.0) < 1e-6

    def test_negative_rate_rejected(self) -> None:
        with pytest.raises(ValueError):
            ActionBaseRates(silent=-0.1, react=0.6, comment=0.2, post=0.2, trade=0.1)

    def test_wrong_sum_rejected(self) -> None:
        with pytest.raises(ValueError):
            ActionBaseRates(silent=0.5, react=0.1, comment=0.1, post=0.1, trade=0.1)
