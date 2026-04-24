"""Persona dataclass, archetype templates, and persona library.

Phase 3 deliverable — stores persona definitions and archetype sampling
distributions. No LLM calls, no DB code, no action-gate logic.
"""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Literal

import numpy as np
import yaml
from pydantic import BaseModel, Field, field_validator, model_validator


# ---------------------------------------------------------------------------
# Hard rule (condition/effect are opaque strings until Phase 5 evaluates them)
# ---------------------------------------------------------------------------

class HardRule(BaseModel):
    """A hard constraint that overrides agent behaviour at decision time."""
    name: str                       # e.g. "no_sell_drawdown"
    description: str                # human-readable explanation
    condition: str                  # DSL string evaluated in Phase 5
    effect: str                     # e.g. "block_action:SELL"


# ---------------------------------------------------------------------------
# Action base rates
# ---------------------------------------------------------------------------

class ActionBaseRates(BaseModel):
    """Per-tick probability of each action tier. Must sum to 1."""
    silent: float
    react: float
    comment: float
    post: float
    trade: float

    @field_validator("*")
    @classmethod
    def _non_negative(cls, v: float) -> float:
        if v < 0:
            raise ValueError(f"Action base rate must be non-negative, got {v}")
        return v

    @model_validator(mode="after")
    def _sum_to_one(self) -> "ActionBaseRates":
        total = self.silent + self.react + self.comment + self.post + self.trade
        if abs(total - 1.0) > 1e-6:
            raise ValueError(
                f"Action base rates must sum to 1.0, got {total:.8f}"
            )
        return self


# ---------------------------------------------------------------------------
# Persona
# ---------------------------------------------------------------------------

class Persona(BaseModel):
    """A fully instantiated agent persona."""

    persona_id: str                                                # "p_000001"
    archetype: str                                                 # one of 10
    name: str                                                      # display name
    backstory: str                                                 # 1-2 paragraphs
    voice_style: str                                               # writing style

    # 5 MVP axes
    risk_tolerance: float = Field(ge=0.0, le=1.0)
    time_horizon_minutes: int = Field(gt=0)
    social_sensitivity: float = Field(ge=0.0, le=1.0)
    herding_coefficient: float = Field(ge=-1.0, le=1.0)
    capital_usd: float = Field(gt=0.0)

    action_base_rates: ActionBaseRates
    hard_rules: list[HardRule] = Field(default_factory=list)

    # Initial state & relationships
    initial_holdings: dict[str, float] = Field(default_factory=dict)
    follows_archetypes: dict[str, float] = Field(default_factory=dict)

    # Generation metadata
    generated_by: str | None = None
    generated_at: str | None = None


# ---------------------------------------------------------------------------
# Distribution — sampling primitive for archetype templates
# ---------------------------------------------------------------------------

class Distribution(BaseModel):
    """Describes how to sample a numeric param from an archetype template."""

    kind: Literal["uniform", "loguniform", "normal", "constant"]
    min: float | None = None
    max: float | None = None
    mean: float | None = None
    std: float | None = None
    value: float | None = None

    def sample(self, rng: np.random.Generator) -> float:
        """Draw a single value from this distribution."""
        if self.kind == "constant":
            if self.value is None:
                raise ValueError("constant distribution requires 'value'")
            return self.value
        elif self.kind == "uniform":
            if self.min is None or self.max is None:
                raise ValueError("uniform distribution requires 'min' and 'max'")
            return float(rng.uniform(self.min, self.max))
        elif self.kind == "loguniform":
            if self.min is None or self.max is None:
                raise ValueError("loguniform distribution requires 'min' and 'max'")
            if self.min <= 0 or self.max <= 0:
                raise ValueError("loguniform requires positive min and max")
            log_val = rng.uniform(math.log(self.min), math.log(self.max))
            return float(math.exp(log_val))
        elif self.kind == "normal":
            if self.mean is None or self.std is None:
                raise ValueError("normal distribution requires 'mean' and 'std'")
            return float(rng.normal(self.mean, self.std))
        else:
            raise ValueError(f"Unknown distribution kind: {self.kind}")


# ---------------------------------------------------------------------------
# Archetype template
# ---------------------------------------------------------------------------

class ArchetypeTemplate(BaseModel):
    """Blueprint for generating Persona instances of a given archetype."""

    archetype: str
    description: str
    voice_style_template: str

    # Distributions for each numeric axis
    risk_tolerance: Distribution
    time_horizon_minutes: Distribution
    social_sensitivity: Distribution
    herding_coefficient: Distribution
    capital_usd: Distribution

    # Behaviour config (fixed per archetype)
    action_base_rates: ActionBaseRates
    hard_rules: list[HardRule] = Field(default_factory=list)

    # Phase 6 social graph target
    target_follow_count: Distribution

    # Follow preferences (weights, normalized at use time)
    follows_archetypes: dict[str, float]

    # Initial holdings distributions
    initial_holdings_dist: dict[str, Distribution] = Field(default_factory=dict)

    @classmethod
    def from_yaml(cls, path: str | Path) -> "ArchetypeTemplate":
        """Load an archetype template from a YAML file."""
        path = Path(path)
        with open(path, "r") as f:
            data = yaml.safe_load(f)
        return cls.model_validate(data)

    def sample_persona(
        self,
        persona_id: str,
        rng: np.random.Generator,
    ) -> Persona:
        """Instantiate a Persona by sampling all distribution axes.

        Name / backstory / voice_style get placeholder values — Phase 4
        (the LLM generation pipeline) fills in real ones.
        """
        # Sample numeric axes
        risk_tol = float(np.clip(self.risk_tolerance.sample(rng), 0.0, 1.0))
        time_h = max(1, int(round(self.time_horizon_minutes.sample(rng))))
        social_s = float(np.clip(self.social_sensitivity.sample(rng), 0.0, 1.0))
        herding = float(np.clip(self.herding_coefficient.sample(rng), -1.0, 1.0))
        capital = max(0.01, self.capital_usd.sample(rng))

        # Sample initial holdings
        holdings: dict[str, float] = {}
        for symbol, dist in self.initial_holdings_dist.items():
            holdings[symbol] = max(0.0, dist.sample(rng))

        return Persona(
            persona_id=persona_id,
            archetype=self.archetype,
            name=f"{self.archetype}_{persona_id}",
            backstory=f"Auto-generated {self.archetype} persona.",
            voice_style=self.voice_style_template,
            risk_tolerance=risk_tol,
            time_horizon_minutes=time_h,
            social_sensitivity=social_s,
            herding_coefficient=herding,
            capital_usd=capital,
            action_base_rates=self.action_base_rates,
            hard_rules=[r.model_copy() for r in self.hard_rules],
            initial_holdings=holdings,
            follows_archetypes=dict(self.follows_archetypes),
            generated_by="archetype_sampler",
            generated_at=None,
        )


# ---------------------------------------------------------------------------
# Persona library
# ---------------------------------------------------------------------------

class PersonaLibrary(BaseModel):
    """Holds a collection of Persona instances with serialization + sampling."""

    personas: list[Persona]

    # -- Persistence -----------------------------------------------------------

    @classmethod
    def load_from_jsonl(cls, path: str | Path) -> "PersonaLibrary":
        """Load personas from a JSONL file (one JSON object per line)."""
        path = Path(path)
        personas: list[Persona] = []
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                personas.append(Persona.model_validate_json(line))
        return cls(personas=personas)

    def save_to_jsonl(self, path: str | Path) -> None:
        """Save personas to a JSONL file."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            for p in self.personas:
                f.write(p.model_dump_json() + "\n")

    # -- Lookup ----------------------------------------------------------------

    def get_by_id(self, persona_id: str) -> Persona:
        """Return a persona by its ID. Raises KeyError if not found."""
        for p in self.personas:
            if p.persona_id == persona_id:
                return p
        raise KeyError(f"Persona '{persona_id}' not found")

    def by_archetype(self, archetype: str) -> list[Persona]:
        """Return all personas of a given archetype."""
        return [p for p in self.personas if p.archetype == archetype]

    def distribution_summary(self) -> dict[str, int]:
        """Return {archetype: count} for all personas."""
        counts: dict[str, int] = {}
        for p in self.personas:
            counts[p.archetype] = counts.get(p.archetype, 0) + 1
        return counts

    # -- Sampling --------------------------------------------------------------

    def sample(
        self,
        mix: dict[str, float],
        count: int,
        seed: int = 42,
    ) -> list[Persona]:
        """Sample *count* personas respecting the archetype mix fractions.

        Parameters
        ----------
        mix : dict mapping archetype name to fraction (must sum to ~1.0).
        count : total number of personas to return.
        seed : random seed for reproducibility.

        Returns a list of *count* personas sampled (with replacement if
        a bucket is smaller than the requested count for that archetype).
        """
        total_frac = sum(mix.values())
        if abs(total_frac - 1.0) > 1e-6:
            raise ValueError(
                f"Mix fractions must sum to 1.0, got {total_frac:.6f}"
            )

        rng = np.random.default_rng(seed)

        # Build per-archetype pools
        pools: dict[str, list[Persona]] = {}
        for arch in mix:
            pool = self.by_archetype(arch)
            if not pool:
                raise ValueError(
                    f"No personas of archetype '{arch}' in the library"
                )
            pools[arch] = pool

        # Allocate counts per archetype (largest-remainder method)
        raw = {a: frac * count for a, frac in mix.items()}
        floored = {a: int(v) for a, v in raw.items()}
        remainder = {a: raw[a] - floored[a] for a in raw}
        allocated = sum(floored.values())
        # Distribute leftover seats to archetypes with largest remainders
        for a in sorted(remainder, key=remainder.get, reverse=True):  # type: ignore[arg-type]
            if allocated >= count:
                break
            floored[a] += 1
            allocated += 1

        # Sample from each pool
        result: list[Persona] = []
        for arch, n in floored.items():
            pool = pools[arch]
            indices = rng.choice(len(pool), size=n, replace=True)
            result.extend(pool[int(i)] for i in indices)

        # Shuffle the final list so archetypes aren't grouped
        rng.shuffle(result)  # type: ignore[arg-type]
        return result
