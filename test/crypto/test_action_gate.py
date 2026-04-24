"""Tests for Phase 5: Action gate — tier decision, stimulus, rate limits."""

from __future__ import annotations

import time
from collections import Counter
from pathlib import Path

import numpy as np
import pytest

from oasis.crypto.action_gate import (
    ActionGate,
    DEFAULT_RATE_LIMITS,
    RateLimits,
    StimulusInputs,
    Tier,
    TIER_ALLOWED_ACTIONS,
)
from oasis.crypto.persona import (
    ActionBaseRates,
    ArchetypeTemplate,
    Persona,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ARCHETYPES_DIR = Path(__file__).resolve().parents[2] / "data" / "personas" / "archetypes"

ALL_ARCHETYPES = [
    "lurker", "hodler", "paperhands", "fomo_degen", "ta",
    "contrarian", "news_trader", "whale", "kol", "market_maker",
]

N_SAMPLES = 10_000


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_persona(
    archetype: str = "lurker",
    silent: float = 0.97,
    react: float = 0.019,
    comment: float = 0.005,
    post: float = 0.001,
    trade: float = 0.005,
    social_sensitivity: float = 0.25,
) -> Persona:
    """Quick helper to build a minimal Persona for gate tests."""
    return Persona(
        persona_id="test_001",
        archetype=archetype,
        name="Test Agent",
        backstory="A test persona.",
        voice_style="terse",
        risk_tolerance=0.5,
        time_horizon_minutes=60,
        social_sensitivity=social_sensitivity,
        herding_coefficient=0.0,
        capital_usd=10_000.0,
        action_base_rates=ActionBaseRates(
            silent=silent, react=react, comment=comment, post=post, trade=trade,
        ),
    )


@pytest.fixture(scope="module")
def archetype_templates() -> dict[str, ArchetypeTemplate]:
    """Load all 10 archetype templates from YAML."""
    templates: dict[str, ArchetypeTemplate] = {}
    for name in ALL_ARCHETYPES:
        path = ARCHETYPES_DIR / f"{name}.yaml"
        templates[name] = ArchetypeTemplate.from_yaml(path)
    return templates


@pytest.fixture(scope="module")
def archetype_personas(
    archetype_templates: dict[str, ArchetypeTemplate],
) -> dict[str, Persona]:
    """One representative persona per archetype (sampled with fixed seed)."""
    rng = np.random.default_rng(99)
    return {
        name: tpl.sample_persona(f"p_{name}", rng)
        for name, tpl in archetype_templates.items()
    }


# ---------------------------------------------------------------------------
# 1. Silent dominates at zero stimulus for lurker
# ---------------------------------------------------------------------------

class TestSilentDominatesLurker:
    def test_lurker_zero_stimulus_over_90pct_silent(self) -> None:
        lurker = _make_persona(archetype="lurker", silent=0.97)
        gate = ActionGate(seed=42)
        tiers = [gate.decide_tier(lurker, stimulus=0.0) for _ in range(N_SAMPLES)]
        silent_rate = tiers.count(Tier.SILENT) / N_SAMPLES
        assert silent_rate > 0.90, (
            f"Lurker at stimulus=0 should be >90% silent, got {silent_rate:.4f}"
        )


# ---------------------------------------------------------------------------
# 2. Stimulus reduces silence
# ---------------------------------------------------------------------------

class TestStimulusReducesSilence:
    def test_lurker_high_stimulus_less_silent(self) -> None:
        """Even a lurker (base silent=0.97) should be measurably less silent
        at high stimulus. With social_sensitivity=0.25 and stimulus=5.0,
        p_silent_adj = 0.97^3.5 ~ 0.90, so the threshold is 0.92 (below base
        but not dramatic -- lurkers are sticky by design). The relative
        reduction is the core assertion."""
        lurker = _make_persona(archetype="lurker", silent=0.97, social_sensitivity=0.25)
        gate_quiet = ActionGate(seed=42)
        gate_loud = ActionGate(seed=42)

        quiet = [gate_quiet.decide_tier(lurker, stimulus=0.0) for _ in range(N_SAMPLES)]
        loud = [gate_loud.decide_tier(lurker, stimulus=5.0) for _ in range(N_SAMPLES)]

        silent_quiet = quiet.count(Tier.SILENT) / N_SAMPLES
        silent_loud = loud.count(Tier.SILENT) / N_SAMPLES

        # p_silent_adj = 0.97^(1+2*5*0.25) = 0.97^3.5 ~ 0.899
        assert silent_loud < 0.92, (
            f"Lurker at stimulus=5 should be <92% silent "
            f"(adj p ~ 0.90), got {silent_loud:.4f}"
        )
        assert silent_loud < silent_quiet, (
            f"Stimulus should reduce silence: quiet={silent_quiet:.4f}, "
            f"loud={silent_loud:.4f}"
        )

    def test_fomo_degen_high_stimulus_much_less_silent(self) -> None:
        """FOMO degen (base silent=0.70, social_sensitivity=0.85) responds
        dramatically: 0.70^(1+2*5*0.85) = 0.70^9.5 ~ 0.024. This is the
        persona type where stimulus really moves the needle."""
        degen = _make_persona(
            archetype="fomo_degen", silent=0.70, react=0.15,
            comment=0.06, post=0.05, trade=0.04, social_sensitivity=0.85,
        )
        gate = ActionGate(seed=42)
        tiers = [gate.decide_tier(degen, stimulus=5.0) for _ in range(N_SAMPLES)]
        silent_rate = tiers.count(Tier.SILENT) / N_SAMPLES
        assert silent_rate < 0.10, (
            f"FOMO degen at stimulus=5 should be <10% silent, got {silent_rate:.4f}"
        )


# ---------------------------------------------------------------------------
# 3. Social sensitivity amplifies response
# ---------------------------------------------------------------------------

class TestSocialSensitivityAmplifies:
    def test_high_sensitivity_less_silent(self) -> None:
        low_s = _make_persona(
            archetype="test", silent=0.85, react=0.10, comment=0.03,
            post=0.01, trade=0.01, social_sensitivity=0.1,
        )
        high_s = _make_persona(
            archetype="test", silent=0.85, react=0.10, comment=0.03,
            post=0.01, trade=0.01, social_sensitivity=0.9,
        )
        gate_low = ActionGate(seed=42)
        gate_high = ActionGate(seed=42)

        low_tiers = [gate_low.decide_tier(low_s, stimulus=1.0) for _ in range(N_SAMPLES)]
        high_tiers = [gate_high.decide_tier(high_s, stimulus=1.0) for _ in range(N_SAMPLES)]

        silent_low = low_tiers.count(Tier.SILENT) / N_SAMPLES
        silent_high = high_tiers.count(Tier.SILENT) / N_SAMPLES

        assert silent_high < silent_low, (
            f"Higher social_sensitivity should yield less silence: "
            f"low={silent_low:.4f}, high={silent_high:.4f}"
        )


# ---------------------------------------------------------------------------
# 4. Probability normalization
# ---------------------------------------------------------------------------

class TestProbNormalization:
    @pytest.mark.parametrize("stimulus", [0.0, 0.5, 1.0, 5.0, 20.0])
    def test_probs_sum_to_one(self, stimulus: float) -> None:
        persona = _make_persona()
        gate = ActionGate(seed=1)
        probs = gate._adjusted_tier_probs(persona, stimulus)
        assert abs(probs.sum() - 1.0) < 1e-10, (
            f"Probs should sum to 1.0, got {probs.sum():.15f} at stimulus={stimulus}"
        )

    def test_probs_non_negative(self) -> None:
        persona = _make_persona()
        gate = ActionGate(seed=1)
        for stimulus in [0.0, 1.0, 10.0, 100.0]:
            probs = gate._adjusted_tier_probs(persona, stimulus)
            assert np.all(probs >= 0.0), f"Negative prob at stimulus={stimulus}"


# ---------------------------------------------------------------------------
# 5. Determinism (same seed -> identical tier sequence)
# ---------------------------------------------------------------------------

class TestDeterminism:
    def test_same_seed_same_output(self) -> None:
        persona = _make_persona()
        g1 = ActionGate(seed=777)
        g2 = ActionGate(seed=777)
        seq1 = [g1.decide_tier(persona, 0.5) for _ in range(500)]
        seq2 = [g2.decide_tier(persona, 0.5) for _ in range(500)]
        assert seq1 == seq2, "Same seed must produce identical tier sequences"


# ---------------------------------------------------------------------------
# 6. Batch distributional equivalence (KS test)
# ---------------------------------------------------------------------------

class TestBatchEquivalence:
    @staticmethod
    def _ks_2samp(a: list[int], b: list[int]) -> tuple[float, float]:
        """Two-sample Kolmogorov-Smirnov test (no scipy dependency).

        Returns (ks_statistic, approximate_p_value) using the asymptotic
        distribution.
        """
        a_sorted = np.sort(a)
        b_sorted = np.sort(b)
        all_vals = np.sort(np.concatenate([a_sorted, b_sorted]))
        # ECDFs evaluated at all unique values
        cdf_a = np.searchsorted(a_sorted, all_vals, side="right") / len(a)
        cdf_b = np.searchsorted(b_sorted, all_vals, side="right") / len(b)
        d = float(np.max(np.abs(cdf_a - cdf_b)))
        # Asymptotic p-value: P(D > d) ~ 2*exp(-2*n_eff*d^2)
        n = len(a)
        m = len(b)
        n_eff = n * m / (n + m)
        lam = (n_eff**0.5 + 0.12 + 0.11 / n_eff**0.5) * d
        p_val = 2.0 * np.exp(-2.0 * lam**2)
        return d, float(np.clip(p_val, 0.0, 1.0))

    def test_batch_vs_serial_distribution(self) -> None:
        persona = _make_persona(
            archetype="fomo_degen", silent=0.70, react=0.15,
            comment=0.06, post=0.05, trade=0.04, social_sensitivity=0.85,
        )
        N = 5000
        personas = [persona] * N
        stimuli = [0.0] * N

        # Batch
        gate_batch = ActionGate(seed=42)
        batch_tiers = gate_batch.decide_tiers_batch(personas, stimuli)

        # Serial
        gate_serial = ActionGate(seed=42)
        serial_tiers = [gate_serial.decide_tier(persona, 0.0) for _ in range(N)]

        # Convert to numeric for KS test (ordinal mapping)
        tier_to_int = {Tier.SILENT: 0, Tier.REACT: 1, Tier.COMMENT: 2,
                       Tier.POST: 3, Tier.TRADE: 4}
        batch_vals = [tier_to_int[t] for t in batch_tiers]
        serial_vals = [tier_to_int[t] for t in serial_tiers]

        ks_stat, p_val = self._ks_2samp(batch_vals, serial_vals)
        assert p_val > 0.05, (
            f"Batch and serial should be distributionally equivalent "
            f"(KS p={p_val:.4f}, stat={ks_stat:.4f})"
        )


# ---------------------------------------------------------------------------
# 7. Batch performance (10k agents < 200ms)
# ---------------------------------------------------------------------------

class TestBatchPerformance:
    def test_10k_agents_under_200ms(self, archetype_templates: dict[str, ArchetypeTemplate]) -> None:
        rng = np.random.default_rng(42)
        # Build 10k personas (mix of archetypes)
        personas: list[Persona] = []
        for name, tpl in archetype_templates.items():
            for i in range(1000):
                personas.append(tpl.sample_persona(f"perf_{name}_{i}", rng))
        stimuli = [0.0] * len(personas)

        gate = ActionGate(seed=42)
        start = time.perf_counter()
        gate.decide_tiers_batch(personas, stimuli)
        elapsed_ms = (time.perf_counter() - start) * 1000

        assert elapsed_ms < 200, (
            f"10k-agent batch should complete in <200ms, took {elapsed_ms:.1f}ms"
        )
        # Print for the report
        print(f"\n  [PERF] 10k-agent batch: {elapsed_ms:.1f}ms")


# ---------------------------------------------------------------------------
# 8. Allowed action set matches tier
# ---------------------------------------------------------------------------

class TestAllowedActions:
    def test_post_tier(self) -> None:
        assert ActionGate.allowed_actions(Tier.POST) == ["CREATE_POST"]

    def test_trade_tier_contains_place_order(self) -> None:
        actions = ActionGate.allowed_actions(Tier.TRADE)
        assert "PLACE_ORDER" in actions

    def test_silent_tier(self) -> None:
        assert ActionGate.allowed_actions(Tier.SILENT) == ["DO_NOTHING"]

    def test_react_tier(self) -> None:
        actions = ActionGate.allowed_actions(Tier.REACT)
        assert "LIKE_POST" in actions
        assert "REPOST" in actions

    def test_comment_tier(self) -> None:
        actions = ActionGate.allowed_actions(Tier.COMMENT)
        assert "CREATE_COMMENT" in actions


# ---------------------------------------------------------------------------
# 9. Rate limit downgrade
# ---------------------------------------------------------------------------

class TestRateLimitDowngrade:
    def test_fomo_degen_post_rate_limit(self) -> None:
        """FOMO degen limited to 1 post / 10 steps. After recording a POST,
        subsequent POSTs within the window get downgraded."""
        persona = _make_persona(
            archetype="fomo_degen", silent=0.10, react=0.10,
            comment=0.10, post=0.60, trade=0.10, social_sensitivity=0.85,
        )
        # Custom rate limit: 1 post per 10 steps
        gate = ActionGate(
            seed=42,
            rate_limits={"fomo_degen": RateLimits(post_per_steps=(10, 1))},
        )

        user_id = 1
        post_steps: list[int] = []

        for step in range(100):
            tier = gate.decide_tier(persona, stimulus=2.0, user_id=user_id, step=step)
            if tier == Tier.POST:
                gate.record_action(user_id, Tier.POST, step)
                post_steps.append(step)

        # Verify: no two POSTs within 10 steps of each other
        for i in range(1, len(post_steps)):
            gap = post_steps[i] - post_steps[i - 1]
            assert gap > 10, (
                f"Post rate limit violated: posts at steps {post_steps[i-1]} "
                f"and {post_steps[i]} (gap={gap}, limit=10)"
            )


# ---------------------------------------------------------------------------
# 10. Sliding-window purges
# ---------------------------------------------------------------------------

class TestSlidingWindowPurge:
    def test_old_entries_purged(self) -> None:
        persona = _make_persona(archetype="fomo_degen")
        gate = ActionGate(
            seed=42,
            rate_limits={"fomo_degen": RateLimits(post_per_steps=(10, 1))},
        )
        limits = gate.rate_limits["fomo_degen"]
        user_id = 42

        # Record a POST at step 0
        gate.record_action(user_id, Tier.POST, step=0)
        assert gate._would_violate(user_id, Tier.POST, step=5, limits=limits)

        # At step 15 (window=10), step 0 should be purged
        assert not gate._would_violate(user_id, Tier.POST, step=15, limits=limits)

        # Verify the deque was actually pruned
        key = (user_id, Tier.POST.value)
        history = gate._history.get(key, None)
        assert history is not None
        assert 0 not in history, "Step 0 should have been purged from history"


# ---------------------------------------------------------------------------
# 11. Stimulus computation
# ---------------------------------------------------------------------------

class TestStimulusComputation:
    def test_mvp_weights(self) -> None:
        result = ActionGate.compute_stimulus(
            StimulusInputs(price_stimulus=0.05, news_stimulus=1.0)
        )
        expected = 0.05 * 1.0 + 1.0 * 2.0  # = 2.05
        assert abs(result - expected) < 1e-10, (
            f"Expected {expected}, got {result}"
        )

    def test_all_zeros(self) -> None:
        assert ActionGate.compute_stimulus(StimulusInputs()) == 0.0

    def test_all_terms(self) -> None:
        result = ActionGate.compute_stimulus(
            StimulusInputs(
                price_stimulus=1.0,
                news_stimulus=1.0,
                follow_stimulus=1.0,
                mention_stimulus=1.0,
                personal_stimulus=1.0,
            )
        )
        # 1*1 + 2*1 + 1*1 + 1*1 + 1*1 = 6.0
        assert abs(result - 6.0) < 1e-10


# ---------------------------------------------------------------------------
# 12. Degenerate persona (silent=1.0)
# ---------------------------------------------------------------------------

class TestDegeneratePersona:
    def test_always_silent(self) -> None:
        persona = _make_persona(
            silent=1.0, react=0.0, comment=0.0, post=0.0, trade=0.0,
        )
        gate = ActionGate(seed=42)
        tiers = [gate.decide_tier(persona, stimulus=s) for s in [0.0, 1.0, 5.0, 50.0]]
        assert all(t == Tier.SILENT for t in tiers), (
            f"Degenerate persona should always be SILENT, got {tiers}"
        )

    def test_probs_degenerate(self) -> None:
        persona = _make_persona(
            silent=1.0, react=0.0, comment=0.0, post=0.0, trade=0.0,
        )
        gate = ActionGate(seed=42)
        probs = gate._adjusted_tier_probs(persona, stimulus=5.0)
        assert probs[0] == 1.0
        assert all(probs[1:] == 0.0)


# ---------------------------------------------------------------------------
# 13. KOL at stimulus=0 is <50% silent
# ---------------------------------------------------------------------------

class TestKolNotTooSilent:
    def test_kol_zero_stimulus_active(self, archetype_personas: dict[str, Persona]) -> None:
        kol = archetype_personas["kol"]
        gate = ActionGate(seed=42)
        tiers = [gate.decide_tier(kol, stimulus=0.0) for _ in range(N_SAMPLES)]
        silent_rate = tiers.count(Tier.SILENT) / N_SAMPLES
        assert silent_rate < 0.50, (
            f"KOL at stimulus=0 should be <50% silent (base=0.30), got {silent_rate:.4f}"
        )


# ---------------------------------------------------------------------------
# Bonus: Silent-rate table for all archetypes at stimulus=0
# ---------------------------------------------------------------------------

class TestSilentRateTable:
    """Not a pass/fail gate -- produces the table for the report."""

    def test_silent_rate_per_archetype(
        self, archetype_personas: dict[str, Persona]
    ) -> None:
        gate = ActionGate(seed=42)
        print("\n\n  Silent-rate table (10k samples, stimulus=0):")
        print(f"  {'Archetype':<16} {'Base silent':>12} {'Sampled silent':>15}")
        print(f"  {'-'*16} {'-'*12} {'-'*15}")

        for name in ALL_ARCHETYPES:
            persona = archetype_personas[name]
            # Fresh gate per archetype so RNG sequences don't affect each other
            g = ActionGate(seed=42)
            tiers = [g.decide_tier(persona, stimulus=0.0) for _ in range(N_SAMPLES)]
            silent_rate = tiers.count(Tier.SILENT) / N_SAMPLES
            base = persona.action_base_rates.silent
            print(f"  {name:<16} {base:>12.3f} {silent_rate:>15.4f}")

            # Sampled rate should be within 3 sigma of base rate
            # sigma = sqrt(p*(1-p)/N)
            sigma = (base * (1 - base) / N_SAMPLES) ** 0.5
            assert abs(silent_rate - base) < 3 * sigma, (
                f"{name}: sampled silent rate {silent_rate:.4f} deviates >3sigma "
                f"from base {base:.3f} (sigma={sigma:.4f})"
            )
