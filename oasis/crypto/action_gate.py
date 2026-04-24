"""Action gate — per-step tier decision for each agent.

Phase 5 deliverable. Runs BEFORE the LLM is invoked: decides whether the
agent acts at all (SILENT tier → skip the LLM call entirely) or what tier
of action they take. For non-silent tiers, narrows the allowed action set
passed to the LLM prompt.

No LLM calls, no DB writes, no network. Rate-limit history is in-memory
only (``_history`` dict of deques); the harness must call ``record_action``
after an action is actually performed. DB persistence of rate limits is
deferred to a future phase.

numpy + stdlib + pydantic only.
"""

from __future__ import annotations

from collections import deque
from enum import Enum
from typing import Iterable

import numpy as np
from pydantic import BaseModel, Field

from oasis.crypto.persona import Persona


# ---------------------------------------------------------------------------
# Tier enum
# ---------------------------------------------------------------------------

class Tier(str, Enum):
    SILENT = "silent"
    REACT = "react"
    COMMENT = "comment"
    POST = "post"
    TRADE = "trade"


# ---------------------------------------------------------------------------
# Tier -> allowed action-type names
# ---------------------------------------------------------------------------

# Action names are strings (not an enum) because the mapping to OASIS
# ActionType happens at the harness layer; keeping strings here keeps
# the gate decoupled from the agent-action module.
TIER_ALLOWED_ACTIONS: dict[Tier, list[str]] = {
    Tier.SILENT:  ["DO_NOTHING"],
    Tier.REACT:   ["LIKE_POST", "REPOST", "UNLIKE_POST"],
    Tier.COMMENT: ["CREATE_COMMENT", "LIKE_COMMENT"],
    Tier.POST:    ["CREATE_POST"],
    Tier.TRADE:   ["PLACE_ORDER", "CANCEL_ORDER", "VIEW_ORDER_BOOK", "VIEW_PORTFOLIO"],
}


# ---------------------------------------------------------------------------
# Stimulus inputs
# ---------------------------------------------------------------------------

class StimulusInputs(BaseModel):
    """Per-agent, per-step signals fed into the stimulus formula.

    MVP uses only price_stimulus and news_stimulus; other terms default to 0
    and are placeholders for Phase 6 when the info filter provides them.
    """
    price_stimulus: float = 0.0        # |delta portfolio value| / portfolio_value
    news_stimulus: float = 0.0         # 0 or 1 (news event this step?)
    follow_stimulus: float = 0.0       # posts from followed accounts (count-based); MVP=0
    mention_stimulus: float = 0.0      # mentions of held tokens; MVP=0
    personal_stimulus: float = 0.0     # replies to own posts; MVP=0


# ---------------------------------------------------------------------------
# Rate limits
# ---------------------------------------------------------------------------

class RateLimits(BaseModel):
    """Per-persona hard caps to prevent runaways even if the gate and LLM
    both say 'act'. Max actions of a given tier in a sliding window of
    N steps. None = no cap."""
    post_per_steps: tuple[int, int] | None = None        # (window, max)
    comment_per_steps: tuple[int, int] | None = None     # (window, max)
    trade_per_steps: tuple[int, int] | None = None       # (window, max)
    # react is never limited; reaction is cheap in reality too.


# Default rate limits per archetype (MVP). Keys match Persona.archetype.
DEFAULT_RATE_LIMITS: dict[str, RateLimits] = {
    "lurker":       RateLimits(post_per_steps=(1440, 1), comment_per_steps=(60, 1)),
    "hodler":       RateLimits(post_per_steps=(60, 1),   comment_per_steps=(10, 1),
                               trade_per_steps=(1440, 2)),
    "paperhands":   RateLimits(post_per_steps=(30, 1),   comment_per_steps=(5, 1),
                               trade_per_steps=(10, 2)),
    "fomo_degen":   RateLimits(post_per_steps=(10, 1),   comment_per_steps=(5, 3),
                               trade_per_steps=(5, 3)),
    "ta":           RateLimits(post_per_steps=(30, 1),   trade_per_steps=(10, 2)),
    "contrarian":   RateLimits(post_per_steps=(60, 1),   trade_per_steps=(60, 3)),
    "news_trader":  RateLimits(post_per_steps=(10, 2),   trade_per_steps=(5, 2)),
    "whale":        RateLimits(post_per_steps=(120, 1),  trade_per_steps=(60, 3)),
    "kol":          RateLimits(post_per_steps=(3, 1),    comment_per_steps=(3, 2)),
    "market_maker": RateLimits(),  # MMs have no social caps; trading is their job
}


# ---------------------------------------------------------------------------
# ActionGate
# ---------------------------------------------------------------------------

class ActionGate:
    """Decides the per-step action tier for each agent -- runs before the LLM.

    Stimulus-adjusted p_silent::

        p_silent_adj = p_silent_base ** (1 + alpha * stimulus * social_sensitivity)

    When stimulus > 0 the exponent grows > 1, so p_silent (which is < 1)
    shrinks -- the agent is more likely to act. Remaining probability mass
    is renormalized across react/comment/post/trade using the persona's
    base-rate ratios.

    Rate-limit history is in-memory only (dict of deques, keyed by
    (user_id, tier_name)). The harness must call ``record_action`` after
    an action is actually performed so that the sliding-window counters
    stay accurate.
    """

    def __init__(
        self,
        alpha: float = 2.0,
        seed: int = 42,
        rate_limits: dict[str, RateLimits] | None = None,
    ):
        self.alpha = alpha
        self.rng = np.random.default_rng(seed)
        self.rate_limits = rate_limits or DEFAULT_RATE_LIMITS
        # Sliding-window counters: {(user_id, tier_name): deque of step numbers}
        self._history: dict[tuple[int, str], deque[int]] = {}

    # ------------------------------------------------------------------ #
    # Stimulus                                                            #
    # ------------------------------------------------------------------ #

    @staticmethod
    def compute_stimulus(inputs: StimulusInputs) -> float:
        """Weighted sum of stimulus terms.

        MVP weights: price=1.0, news=2.0, others=1.0 (currently zero-valued).
        Weights can be tuned in calibration (Phase 11).
        """
        return (
            1.0 * inputs.price_stimulus
            + 2.0 * inputs.news_stimulus
            + 1.0 * inputs.follow_stimulus
            + 1.0 * inputs.mention_stimulus
            + 1.0 * inputs.personal_stimulus
        )

    # ------------------------------------------------------------------ #
    # Tier decision                                                       #
    # ------------------------------------------------------------------ #

    def _adjusted_tier_probs(
        self, persona: Persona, stimulus: float
    ) -> np.ndarray:
        """Return array of 5 probabilities [silent, react, comment, post, trade]."""
        base = persona.action_base_rates
        base_arr = np.array([
            base.silent, base.react, base.comment, base.post, base.trade
        ])
        p_silent_adj = base.silent ** (
            1.0 + self.alpha * stimulus * persona.social_sensitivity
        )
        p_silent_adj = float(np.clip(p_silent_adj, 0.0, 0.999))
        remaining = 1.0 - p_silent_adj

        # Distribute remaining across non-silent tiers by their base-rate ratios
        non_silent_base = base_arr[1:]
        denom = non_silent_base.sum()
        if denom < 1e-12:
            # Degenerate persona -- all mass goes to silent
            return np.array([1.0, 0.0, 0.0, 0.0, 0.0])
        non_silent_adj = non_silent_base / denom * remaining
        return np.concatenate([[p_silent_adj], non_silent_adj])

    def decide_tier(
        self,
        persona: Persona,
        stimulus: float,
        user_id: int | None = None,
        step: int | None = None,
    ) -> Tier:
        """Sample one tier for one persona.

        If *user_id* and *step* are provided and a rate limit would be
        violated, the tier is downgraded (post -> comment -> react -> silent;
        trade -> silent).
        """
        probs = self._adjusted_tier_probs(persona, stimulus)
        choice_idx = int(self.rng.choice(5, p=probs))
        tier_list = [Tier.SILENT, Tier.REACT, Tier.COMMENT, Tier.POST, Tier.TRADE]
        tier = tier_list[choice_idx]

        if user_id is not None and step is not None:
            tier = self._apply_rate_limit(persona, tier, user_id, step)
        return tier

    def decide_tiers_batch(
        self,
        personas: list[Persona],
        stimuli: list[float],
    ) -> list[Tier]:
        """Vectorized tier sampling for large populations.

        Does NOT apply rate limits (the caller should loop with rate-limit
        checks for the non-silent subset -- silent dominates so this is
        cheap).
        """
        N = len(personas)
        probs = np.zeros((N, 5))
        for i, (p, s) in enumerate(zip(personas, stimuli)):
            probs[i] = self._adjusted_tier_probs(p, s)
        # Inverse-CDF sampling
        u = self.rng.random(N)
        cdf = np.cumsum(probs, axis=1)
        choice_idx = (u[:, None] > cdf).sum(axis=1)
        tier_list = [Tier.SILENT, Tier.REACT, Tier.COMMENT, Tier.POST, Tier.TRADE]
        return [tier_list[i] for i in choice_idx]

    # ------------------------------------------------------------------ #
    # Rate limiting                                                       #
    # ------------------------------------------------------------------ #

    def _apply_rate_limit(
        self, persona: Persona, tier: Tier, user_id: int, step: int
    ) -> Tier:
        """Downgrade tier if it would violate this persona's rate limit.

        Downgrade chain: post -> comment -> react (stop); trade -> silent.
        """
        downgrade_chain = {
            Tier.POST: Tier.COMMENT,
            Tier.COMMENT: Tier.REACT,
            Tier.REACT: Tier.REACT,    # react is never rate-limited
            Tier.TRADE: Tier.TRADE,    # trade blocks to silent if hit
            Tier.SILENT: Tier.SILENT,
        }
        limits = self.rate_limits.get(persona.archetype, RateLimits())

        while True:
            if not self._would_violate(user_id, tier, step, limits):
                break
            if tier == Tier.TRADE:
                tier = Tier.SILENT
                break
            nxt = downgrade_chain[tier]
            if nxt == tier:
                break
            tier = nxt
        return tier

    def _would_violate(
        self, user_id: int, tier: Tier, step: int, limits: RateLimits
    ) -> bool:
        """Check whether recording *tier* at *step* would exceed the cap."""
        cap = None
        if tier == Tier.POST and limits.post_per_steps:
            cap = limits.post_per_steps
        elif tier == Tier.COMMENT and limits.comment_per_steps:
            cap = limits.comment_per_steps
        elif tier == Tier.TRADE and limits.trade_per_steps:
            cap = limits.trade_per_steps
        if cap is None:
            return False
        window, max_n = cap
        history = self._history.get((user_id, tier.value), deque())
        # Purge old entries outside the window
        while history and history[0] < step - window:
            history.popleft()
        return len(history) >= max_n

    def record_action(self, user_id: int, tier: Tier, step: int) -> None:
        """Called by the harness after an action is actually performed.

        Appends the step to the sliding-window history for this (user, tier).
        """
        key = (user_id, tier.value)
        self._history.setdefault(key, deque()).append(step)

    # ------------------------------------------------------------------ #
    # Allowed action set                                                  #
    # ------------------------------------------------------------------ #

    @staticmethod
    def allowed_actions(tier: Tier) -> list[str]:
        """Return the list of action-type names allowed for *tier*."""
        return TIER_ALLOWED_ACTIONS[tier]
