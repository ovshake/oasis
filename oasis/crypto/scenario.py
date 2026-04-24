"""Scenario YAML schema, validation, and resolution.

Phase 9 deliverable. A ``Scenario`` is the user-facing configuration object
that describes a simulation run. It is loaded from YAML, validated, and then
resolved (prices, news, personas) by the CLI runner before constructing a
``Simulation`` instance.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field, model_validator


# ---------------------------------------------------------------------------
# News source spec
# ---------------------------------------------------------------------------


class NewsSourceSpec(BaseModel):
    """How to obtain news events for the simulation."""

    kind: Literal["manual", "historical", "live_snapshot"] = "manual"
    providers: list[str] = Field(default_factory=list)
    date_range: list[datetime] | None = None
    lookback_hours: int | None = None
    relevance_filter: dict = Field(default_factory=dict)
    enrich_with: str = "mock"


class ManualNewsEvent(BaseModel):
    """An inline news event in the scenario YAML."""

    step: int
    content: str
    title: str | None = None
    sentiment: float = 0.0
    assets: list[str] = Field(default_factory=list)
    audience: str = "all"


# ---------------------------------------------------------------------------
# Population mix
# ---------------------------------------------------------------------------


class PopulationMix(BaseModel):
    """Archetype fractions for agent sampling. Must sum to 1.0."""

    lurker: float = 0.45
    hodler: float = 0.15
    paperhands: float = 0.15
    fomo_degen: float = 0.08
    ta: float = 0.05
    contrarian: float = 0.03
    news_trader: float = 0.04
    whale: float = 0.01
    kol: float = 0.02
    market_maker: float = 0.02

    @model_validator(mode="after")
    def _check_sum(self) -> "PopulationMix":
        total = (
            self.lurker
            + self.hodler
            + self.paperhands
            + self.fomo_degen
            + self.ta
            + self.contrarian
            + self.news_trader
            + self.whale
            + self.kol
            + self.market_maker
        )
        if abs(total - 1.0) > 1e-6:
            raise ValueError(
                f"PopulationMix fractions must sum to 1.0, got {total:.8f}"
            )
        return self

    def to_dict(self) -> dict[str, float]:
        """Return {archetype_name: fraction} for use with PersonaLibrary.sample."""
        return {
            "lurker": self.lurker,
            "hodler": self.hodler,
            "paperhands": self.paperhands,
            "fomo_degen": self.fomo_degen,
            "ta": self.ta,
            "contrarian": self.contrarian,
            "news_trader": self.news_trader,
            "whale": self.whale,
            "kol": self.kol,
            "market_maker": self.market_maker,
        }


# ---------------------------------------------------------------------------
# Scenario
# ---------------------------------------------------------------------------

# Template pattern for output_dir placeholders
_TEMPLATE_RE = re.compile(r"\{(\w+)\}")


class Scenario(BaseModel):
    """Top-level scenario configuration loaded from YAML."""

    name: str
    duration_steps: int = 240
    step_minutes: int = 1
    seed: int = 42
    agents_count: int = 1000
    assets: list[str] = Field(
        default_factory=lambda: ["BTC", "ETH", "USDT", "XAU", "WTI", "USD"]
    )
    price_source: Literal["manual", "live", "historical", "default"] = "default"
    as_of_date: datetime | None = None
    initial_prices: dict[str, float] = Field(default_factory=dict)
    population_mix: PopulationMix = Field(default_factory=PopulationMix)
    news_source: NewsSourceSpec = Field(default_factory=NewsSourceSpec)
    manual_events: list[ManualNewsEvent] = Field(default_factory=list)
    persona_library: str = "data/personas/library.jsonl"
    llm_enabled: bool = True
    output_dir: str = "results/{name}/{timestamp}"

    @classmethod
    def from_yaml(cls, path: Path) -> "Scenario":
        """Load and validate a Scenario from a YAML file."""
        path = Path(path)
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        if raw is None:
            raise ValueError(f"Empty YAML file: {path}")
        return cls.model_validate(raw)

    def resolve_output_dir(self) -> Path:
        """Resolve the output directory, substituting ``{name}`` and ``{timestamp}``."""
        now = datetime.now(timezone.utc)
        replacements = {
            "name": self.name,
            "timestamp": now.strftime("%Y%m%d_%H%M%S"),
        }

        def _replace(m: re.Match) -> str:
            key = m.group(1)
            return replacements.get(key, m.group(0))

        resolved = _TEMPLATE_RE.sub(_replace, self.output_dir)
        return Path(resolved)
