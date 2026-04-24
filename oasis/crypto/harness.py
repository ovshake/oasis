"""Simulation harness -- the fixed-tick orchestrator.

Phase 8 deliverable. Owns clock, exchange, agents, social graph, feed
filter, memory, gate. Drives the per-step tick.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

import numpy as np
from pydantic import BaseModel, Field

from oasis.crypto.action_gate import ActionGate, StimulusInputs, Tier
from oasis.crypto.clock import TickClock
from oasis.crypto.exchange import Exchange
from oasis.crypto.info_filter import FeedFilter, Post, SocialGraphBuilder, GraphConfig
from oasis.crypto.memory import MemoryKind, MemoryStore
from oasis.crypto.news_ingest import NewsEvent, wall_clock_to_step
from oasis.crypto.persona import ArchetypeTemplate, Persona
from oasis.crypto.telemetry import Telemetry

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


class SimulationConfig(BaseModel):
    """Static configuration for a simulation run."""

    name: str
    duration_steps: int = 240
    step_minutes: int = 1
    start_datetime: datetime  # UTC scenario anchor
    seed: int = 42

    # Core knobs
    llm_enabled: bool = True  # False = gate-only mode
    llm_model: str = "gpt-4o-mini"
    llm_concurrency: int = 50
    conservation_check_every: int = 60

    # Output
    output_dir: Path
    telemetry_buffer: int = 5000

    # Initial holdings policy
    initial_cash_override: float | None = None


# ---------------------------------------------------------------------------
# LLM client protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class LLMClient(Protocol):
    """Injectable LLM. Harness never imports anthropic/openai directly."""

    async def batch_complete(
        self, system_prompts: list[str], user_prompts: list[str]
    ) -> list[str]: ...


# ---------------------------------------------------------------------------
# LLM client implementations
# ---------------------------------------------------------------------------


class MockLLMClient:
    """Deterministic stub for tests. Returns heuristic JSON responses."""

    def __init__(self, seed: int = 42) -> None:
        self.rng = np.random.default_rng(seed)

    async def batch_complete(
        self, system_prompts: list[str], user_prompts: list[str]
    ) -> list[str]:
        """Return a JSON-encoded action per prompt.

        Parses the user prompt for the tier hint and returns an appropriate
        heuristic action. Deterministic given seed.
        """
        results: list[str] = []
        for sp, up in zip(system_prompts, user_prompts):
            action = self._generate_action(sp, up)
            results.append(json.dumps(action))
        return results

    def _generate_action(self, system_prompt: str, user_prompt: str) -> dict:
        """Generate a simple action dict based on the prompt."""
        combined = (system_prompt + " " + user_prompt).lower()
        if "trade" in combined:
            side = "buy" if self.rng.random() > 0.5 else "sell"
            return {
                "action_type": "PLACE_ORDER",
                "side": side,
                "symbol": "BTC",
                "price_offset_bps": int(self.rng.integers(-50, 50)),
                "quantity_frac": round(float(self.rng.uniform(0.001, 0.01)), 5),
            }
        elif "post" in combined:
            return {
                "action_type": "CREATE_POST",
                "content": f"Mock post {int(self.rng.integers(0, 99999))}",
            }
        elif "comment" in combined:
            return {
                "action_type": "CREATE_COMMENT",
                "content": f"Mock comment {int(self.rng.integers(0, 99999))}",
            }
        else:
            return {"action_type": "LIKE_POST", "target_post_id": None}


class AnthropicLLMClient:
    """Real Claude API client. Uses ANTHROPIC_API_KEY from env.

    For MVP: claude-sonnet-4-6 model. Async concurrency via Semaphore.
    DO NOT use in tests -- tests use MockLLMClient.
    """

    def __init__(
        self, model: str = "claude-sonnet-4-6", concurrency: int = 50
    ) -> None:
        self.model = model
        self.concurrency = concurrency
        self._client: Any = None

    def _ensure_client(self) -> Any:
        if self._client is None:
            try:
                import anthropic

                self._client = anthropic.AsyncAnthropic()
            except ImportError:
                raise ImportError(
                    "anthropic package required for AnthropicLLMClient"
                )
        return self._client

    async def batch_complete(
        self, system_prompts: list[str], user_prompts: list[str]
    ) -> list[str]:
        import asyncio

        client = self._ensure_client()
        sem = asyncio.Semaphore(self.concurrency)

        async def _call(system: str, user: str) -> str:
            async with sem:
                response = await client.messages.create(
                    model=self.model,
                    max_tokens=1024,
                    system=system,
                    messages=[{"role": "user", "content": user}],
                )
                return response.content[0].text

        tasks = [
            _call(sp, up) for sp, up in zip(system_prompts, user_prompts)
        ]
        return await asyncio.gather(*tasks)


# ---------------------------------------------------------------------------
# Step result
# ---------------------------------------------------------------------------


@dataclass
class StepResult:
    step: int
    active_agents: int
    trades_executed: int
    posts_created: int
    news_injected: int
    duration_ms: float


# ---------------------------------------------------------------------------
# User table DDL for standalone sim (no full OASIS)
# ---------------------------------------------------------------------------

_USER_TABLE_DDL = """\
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

_FOLLOW_TABLE_DDL = """\
CREATE TABLE IF NOT EXISTS follow (
    follower_id INTEGER NOT NULL,
    followee_id INTEGER NOT NULL,
    created_at DATETIME,
    PRIMARY KEY(follower_id, followee_id),
    FOREIGN KEY(follower_id) REFERENCES user(user_id),
    FOREIGN KEY(followee_id) REFERENCES user(user_id)
);
"""

_POST_TABLE_DDL = """\
CREATE TABLE IF NOT EXISTS post (
    post_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    content TEXT,
    created_at DATETIME,
    num_likes INTEGER DEFAULT 0,
    num_dislikes INTEGER DEFAULT 0,
    FOREIGN KEY(user_id) REFERENCES user(user_id)
);
"""

_COMMENT_TABLE_DDL = """\
CREATE TABLE IF NOT EXISTS comment (
    comment_id INTEGER PRIMARY KEY AUTOINCREMENT,
    post_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    content TEXT,
    created_at DATETIME,
    FOREIGN KEY(post_id) REFERENCES post(post_id),
    FOREIGN KEY(user_id) REFERENCES user(user_id)
);
"""

_LIKE_TABLE_DDL = """\
CREATE TABLE IF NOT EXISTS like_table (
    user_id INTEGER NOT NULL,
    post_id INTEGER NOT NULL,
    created_at DATETIME,
    PRIMARY KEY(user_id, post_id),
    FOREIGN KEY(user_id) REFERENCES user(user_id),
    FOREIGN KEY(post_id) REFERENCES post(post_id)
);
"""


# ---------------------------------------------------------------------------
# Simulation
# ---------------------------------------------------------------------------


class Simulation:
    """The fixed-tick simulation engine.

    USAGE::

        sim = Simulation(conn, config, personas, templates, news_events,
                         llm_client=MockLLMClient())
        await sim.run()
        sim.telemetry.flush()
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        config: SimulationConfig,
        personas: list[Persona],
        templates: dict[str, ArchetypeTemplate],
        news_events: list[NewsEvent] | None = None,
        llm_client: LLMClient | None = None,
    ) -> None:
        self.conn = conn
        self.config = config
        self.personas = personas
        self.templates = templates
        self.news_events = list(news_events or [])
        self.llm_client = llm_client

        # Wire everything
        self.clock = TickClock(
            start_datetime=config.start_datetime,
            step_minutes=config.step_minutes,
        )
        self.exchange = Exchange(conn)
        self.gate = ActionGate(seed=config.seed)
        self.memory = MemoryStore(conn)
        self.graph_builder: SocialGraphBuilder | None = None
        self.feed_filter: FeedFilter | None = None
        self.telemetry = Telemetry(
            config.output_dir, buffer_size=config.telemetry_buffer
        )

        self.rng = np.random.default_rng(config.seed)
        self.persona_idx_to_user_id: dict[int, int] = {}
        self.user_id_to_persona_idx: dict[int, int] = {}

        # Pre-cached lookups built in initialize()
        self._instrument_id_by_symbol: dict[str, int] = {}
        self._pair_id_by_symbols: dict[tuple[str, str], int] = {}
        self._news_by_step: dict[int, list[NewsEvent]] = {}
        self._pair_ids: list[int] = []

        # Tracking
        self._initial_totals: dict[int, float] = {}  # instrument_id -> total
        self._posts_this_step: list[Post] = []

    # ---- Init ----

    def initialize(self) -> None:
        """Run once before .run(). Sets up agents, graph, initial holdings."""
        # 0. Ensure social tables exist (standalone, no full OASIS)
        self.conn.executescript(_USER_TABLE_DDL)
        self.conn.executescript(_FOLLOW_TABLE_DDL)
        self.conn.executescript(_POST_TABLE_DDL)
        self.conn.executescript(_COMMENT_TABLE_DDL)
        self.conn.executescript(_LIKE_TABLE_DDL)
        self.conn.execute("PRAGMA foreign_keys = ON")

        # 1. Load instrument / pair lookups
        for row in self.conn.execute(
            "SELECT instrument_id, symbol FROM instrument"
        ).fetchall():
            self._instrument_id_by_symbol[row[1]] = row[0]

        for row in self.conn.execute(
            "SELECT pair_id, base_instrument_id, quote_instrument_id FROM pair"
        ).fetchall():
            pair_id, base_id, quote_id = row
            # Look up symbols
            base_sym = next(
                s for s, i in self._instrument_id_by_symbol.items() if i == base_id
            )
            quote_sym = next(
                s for s, i in self._instrument_id_by_symbol.items() if i == quote_id
            )
            self._pair_id_by_symbols[(base_sym, quote_sym)] = pair_id
            self._pair_ids.append(pair_id)

        # 2. Create OASIS user rows for each persona
        for idx, persona in enumerate(self.personas):
            cursor = self.conn.execute(
                "INSERT INTO user (user_name, name, bio, created_at) "
                "VALUES (?, ?, ?, ?)",
                (
                    persona.persona_id,
                    persona.name,
                    persona.backstory[:200] if persona.backstory else "",
                    self.config.start_datetime.isoformat(),
                ),
            )
            user_id = cursor.lastrowid
            assert user_id is not None
            self.persona_idx_to_user_id[idx] = user_id
            self.user_id_to_persona_idx[user_id] = idx

            # Insert into agent_persona mapping
            self.conn.execute(
                "INSERT OR IGNORE INTO persona (persona_id, archetype, name) "
                "VALUES (?, ?, ?)",
                (persona.persona_id, persona.archetype, persona.name),
            )
            self.conn.execute(
                "INSERT OR IGNORE INTO agent_persona (user_id, persona_id) "
                "VALUES (?, ?)",
                (user_id, persona.persona_id),
            )
        self.conn.commit()

        # 3. Seed wallet balances
        for idx, persona in enumerate(self.personas):
            user_id = self.persona_idx_to_user_id[idx]
            holdings = dict(persona.initial_holdings)
            if self.config.initial_cash_override is not None:
                holdings["USD"] = self.config.initial_cash_override
            if holdings:
                self.exchange.ensure_balances(user_id, holdings)

        # 4. Build social graph
        if len(self.personas) >= 2:
            self.graph_builder = SocialGraphBuilder(
                self.personas,
                self.templates,
                GraphConfig(seed=self.config.seed),
            )
            self.graph_builder.build()

            # Persist edges to DB
            pid_to_uid = {
                self.personas[idx].persona_id: uid
                for idx, uid in self.persona_idx_to_user_id.items()
            }
            self.graph_builder.persist_to_db(self.conn, pid_to_uid)

            # 5. Build feed filter
            self.feed_filter = FeedFilter(self.personas, self.graph_builder)

        # 6. Pre-bucket news events by step
        for event in self.news_events:
            try:
                step = wall_clock_to_step(
                    event.timestamp,
                    self.config.start_datetime,
                    self.config.step_minutes,
                )
                if 0 <= step < self.config.duration_steps:
                    self._news_by_step.setdefault(step, []).append(event)
            except ValueError:
                logger.warning(
                    "News event %s has timestamp before scenario start, skipping",
                    event.title,
                )

        # 7. Record initial conservation baseline
        self._record_initial_totals()

    async def run(self) -> list[StepResult]:
        """Main loop."""
        self.initialize()
        results: list[StepResult] = []
        for step in range(self.config.duration_steps):
            result = await self._tick(step)
            results.append(result)
            self.clock.advance()
        self.telemetry.flush()
        return results

    # ---- The tick ----

    async def _tick(self, step: int) -> StepResult:
        """Execute one tick per the 15-stage spec."""
        t0 = time.perf_counter()
        self._posts_this_step = []
        posts_created = 0

        # Stage 2: Fire scheduled news events for this step.
        news_this_step = self._news_by_step.get(step, [])
        self._inject_news(step, news_this_step)

        # Stage 3: Snapshot state (reads below see end-of-step-(N-1) state)

        # Stage 4: Compute stimulus per agent (vectorized)
        stimuli = self._compute_stimuli_batch(step, news_this_step)

        # Stage 5: Gate decides tier per agent (numpy-batched)
        tiers = self.gate.decide_tiers_batch(self.personas, stimuli)

        # Stage 6-7: For non-silent agents, build prompts and run LLM.
        active_indices = [i for i, t in enumerate(tiers) if t != Tier.SILENT]

        if (
            self.llm_client
            and self.config.llm_enabled
            and active_indices
        ):
            actions = await self._run_llm_for_active(
                active_indices, tiers, step
            )
        else:
            # Gate-only: synthesize heuristic actions
            actions = [
                self._heuristic_action(self.personas[i], tiers[i], step)
                for i in active_indices
            ]

        # Stage 8: Enforce hard-rule invariants
        actions = self._enforce_invariants(active_indices, actions, step)

        # Stage 9: Apply actions
        for idx, action in zip(active_indices, actions):
            applied = await self._apply_action(idx, action, step)
            if applied and action.get("action_type") == "CREATE_POST":
                posts_created += 1
            self.gate.record_action(
                self.persona_idx_to_user_id[idx], tiers[idx], step
            )

        # Stage 10: Match orders per pair
        match_results = self.exchange.match_all_pairs(step)
        total_trades = sum(len(t) for t in match_results.values())

        # Stage 11: Auto-generate market-news if large price moves
        self._check_market_news(step, match_results)

        # Stage 12: last_price updated inside matching (already happens)

        # Stage 13: Update memory for all agents
        self._update_memories(step, active_indices, actions, tiers)

        # Stage 14: Telemetry snapshot
        self._snapshot(step, tiers, actions, active_indices, stimuli)

        # Stage 15: Clock advance handled by run() loop

        # Periodic conservation check
        if (
            step > 0
            and self.config.conservation_check_every > 0
            and step % self.config.conservation_check_every == 0
        ):
            self._check_conservation(step)

        duration_ms = (time.perf_counter() - t0) * 1000
        return StepResult(
            step=step,
            active_agents=len(active_indices),
            trades_executed=total_trades,
            posts_created=posts_created,
            news_injected=len(news_this_step),
            duration_ms=duration_ms,
        )

    # ---- Internal helpers ----

    def _compute_stimuli_batch(
        self, step: int, news_this_step: list[NewsEvent]
    ) -> list[float]:
        """Compute scalar stimulus for every persona. Vectorized."""
        N = len(self.personas)
        if N == 0:
            return []

        # News indicator: 1.0 if any news this step, else 0.0
        news_val = 1.0 if news_this_step else 0.0

        # Price stimulus: |delta_price| for the most-traded pair.
        price_stim = 0.0
        for pair_id in self._pair_ids:
            state = self.exchange.pair_state(pair_id)
            lp = state["last_price"]
            pcp = state["prev_close_price"]
            if lp is not None and pcp is not None and pcp > 0:
                delta = abs(lp - pcp) / pcp
                price_stim = max(price_stim, delta)

        stimuli: list[float] = []
        for _i in range(N):
            inp = StimulusInputs(
                price_stimulus=price_stim,
                news_stimulus=news_val,
            )
            stimuli.append(ActionGate.compute_stimulus(inp))
        return stimuli

    async def _run_llm_for_active(
        self,
        active_indices: list[int],
        tiers: list[Tier],
        step: int,
    ) -> list[dict]:
        """Build prompts in parallel, batch-call LLM, parse responses."""
        assert self.llm_client is not None

        system_prompts: list[str] = []
        user_prompts: list[str] = []

        for idx in active_indices:
            persona = self.personas[idx]
            tier = tiers[idx]
            user_id = self.persona_idx_to_user_id[idx]

            # System prompt: persona card + allowed actions
            allowed = ActionGate.allowed_actions(tier)
            sys_p = (
                f"You are {persona.name}, a {persona.archetype} crypto trader.\n"
                f"Voice: {persona.voice_style}\n"
                f"Allowed actions this step: {allowed}\n"
                f"Tier: {tier.value}\n"
                f"Risk tolerance: {persona.risk_tolerance:.2f}\n"
                f"Respond with a JSON object with 'action_type' and relevant fields."
            )

            # User prompt: memory + market state
            mem_block = self.memory.build_prompt_block(user_id, step)
            market_info = self._build_market_info(step)
            user_p = f"{mem_block}\n\nMarket state:\n{market_info}\n\nStep: {step}"

            system_prompts.append(sys_p)
            user_prompts.append(user_p)

        # Batch LLM call
        raw_responses = await self.llm_client.batch_complete(
            system_prompts, user_prompts
        )

        # Parse responses
        actions: list[dict] = []
        for resp in raw_responses:
            try:
                action = json.loads(resp)
                if not isinstance(action, dict):
                    action = {"action_type": "DO_NOTHING"}
            except (json.JSONDecodeError, TypeError):
                action = {"action_type": "DO_NOTHING"}
            actions.append(action)

        return actions

    def _build_market_info(self, step: int) -> str:
        """Build a compact market state string for the LLM prompt."""
        lines: list[str] = []
        for pair_id in self._pair_ids:
            state = self.exchange.pair_state(pair_id)
            lp = state["last_price"]
            pcp = state["prev_close_price"]
            change = ""
            if lp is not None and pcp is not None and pcp > 0:
                pct = (lp - pcp) / pcp * 100
                change = f" ({pct:+.2f}%)"
            lines.append(
                f"{state['base_symbol']}/{state['quote_symbol']}: "
                f"{lp or 'N/A'}{change}"
            )
        return "\n".join(lines)

    def _heuristic_action(
        self, persona: Persona, tier: Tier, step: int
    ) -> dict:
        """Deterministic rule-based action for gate-only mode.

        Must be fast and seed-deterministic.
        """
        if tier == Tier.SILENT:
            return {"action_type": "DO_NOTHING"}

        if tier == Tier.REACT:
            return {"action_type": "LIKE_POST", "target_post_id": None}

        if tier == Tier.COMMENT:
            return {
                "action_type": "CREATE_COMMENT",
                "content": f"[{persona.archetype}] step {step}",
            }

        if tier == Tier.POST:
            return {
                "action_type": "CREATE_POST",
                "content": f"[{persona.archetype}] market update step {step}",
            }

        if tier == Tier.TRADE:
            return self._heuristic_trade(persona, step)

        return {"action_type": "DO_NOTHING"}

    def _heuristic_trade(self, persona: Persona, step: int) -> dict:
        """Generate a heuristic trade action.

        MMs place bid+ask at last_price +/- 20bps.
        Others place a small random order.
        """
        # Pick the first pair (BTC/USD typically)
        if not self._pair_ids:
            return {"action_type": "DO_NOTHING"}

        pair_id = self._pair_ids[0]
        state = self.exchange.pair_state(pair_id)
        last_price = state["last_price"]
        if last_price is None or last_price <= 0:
            return {"action_type": "DO_NOTHING"}

        base_sym = state["base_symbol"]

        if persona.archetype == "market_maker":
            # MM: bid and ask at last_price +/- 20bps
            spread_bps = 20
            bid_price = round(last_price * (1 - spread_bps / 10000), 2)
            ask_price = round(last_price * (1 + spread_bps / 10000), 2)
            qty = round(float(self.rng.uniform(0.001, 0.005)), 6)
            return {
                "action_type": "PLACE_ORDER",
                "orders": [
                    {"side": "buy", "symbol": base_sym, "price": bid_price, "quantity": qty},
                    {"side": "sell", "symbol": base_sym, "price": ask_price, "quantity": qty},
                ],
            }
        else:
            # Random small order
            side = "buy" if self.rng.random() > 0.5 else "sell"
            offset_bps = float(self.rng.integers(-30, 30))
            price = round(last_price * (1 + offset_bps / 10000), 2)
            qty = round(float(self.rng.uniform(0.0001, 0.002)), 6)
            return {
                "action_type": "PLACE_ORDER",
                "side": side,
                "symbol": base_sym,
                "price": price,
                "quantity": qty,
            }

    def _enforce_invariants(
        self,
        indices: list[int],
        actions: list[dict],
        step: int,
    ) -> list[dict]:
        """Evaluate each action against the persona's hard_rules.

        Replace blocked actions with DO_NOTHING. Simple DSL evaluator
        with a fixed vocabulary.
        """
        result: list[dict] = []
        for idx, action in zip(indices, actions):
            persona = self.personas[idx]
            blocked = False
            for rule in persona.hard_rules:
                if self._rule_blocks(rule, action, idx, step):
                    blocked = True
                    break
            if blocked:
                result.append({"action_type": "DO_NOTHING"})
            else:
                result.append(action)
        return result

    def _rule_blocks(
        self, rule: Any, action: dict, persona_idx: int, step: int
    ) -> bool:
        """Check if a hard rule blocks this action.

        Simple DSL: effect strings like 'block_action:SELL'.
        Condition strings like 'drawdown_pct > 0.20' are evaluated with
        a small fixed vocabulary of variables.
        """
        effect = rule.effect
        if not effect.startswith("block_action:"):
            return False

        blocked_action = effect.split(":", 1)[1].upper()
        action_type = action.get("action_type", "")
        action_side = action.get("side", "")

        # Check if the action matches the blocked type
        if blocked_action == "SELL" and (
            action_type == "PLACE_ORDER" and action_side == "sell"
        ):
            # For MVP we do not evaluate conditions -- the rule just applies
            # when the action type matches. Full condition eval is Phase 11.
            return False  # Allow for now; conditions not evaluated in MVP
        if blocked_action == "BUY" and (
            action_type == "PLACE_ORDER" and action_side == "buy"
        ):
            return False  # Allow for now

        return False

    async def _apply_action(
        self, persona_idx: int, action: dict, step: int
    ) -> bool:
        """Route to post/comment/like/order. Returns True if applied."""
        user_id = self.persona_idx_to_user_id[persona_idx]
        action_type = action.get("action_type", "DO_NOTHING")
        created_at = self.clock.step_to_datetime(step).isoformat()

        if action_type == "DO_NOTHING":
            return False

        if action_type == "CREATE_POST":
            content = action.get("content", "")
            cursor = self.conn.execute(
                "INSERT INTO post (user_id, content, created_at) VALUES (?, ?, ?)",
                (user_id, content, created_at),
            )
            self.conn.commit()
            post_id = cursor.lastrowid
            self._posts_this_step.append(
                Post(
                    post_id=post_id or 0,
                    author_user_id=user_id,
                    author_persona_idx=persona_idx,
                    content=content,
                    sentiment=0.0,
                    created_step=step,
                )
            )
            return True

        if action_type == "CREATE_COMMENT":
            # For MVP, comments need a post_id. Pick the most recent post.
            recent = self.conn.execute(
                "SELECT post_id FROM post ORDER BY post_id DESC LIMIT 1"
            ).fetchone()
            if recent:
                self.conn.execute(
                    "INSERT INTO comment (post_id, user_id, content, created_at) "
                    "VALUES (?, ?, ?, ?)",
                    (recent[0], user_id, action.get("content", ""), created_at),
                )
                self.conn.commit()
            return True

        if action_type == "LIKE_POST":
            recent = self.conn.execute(
                "SELECT post_id FROM post WHERE user_id != ? "
                "ORDER BY post_id DESC LIMIT 1",
                (user_id,),
            ).fetchone()
            if recent:
                self.conn.execute(
                    "INSERT OR IGNORE INTO like_table (user_id, post_id, created_at) "
                    "VALUES (?, ?, ?)",
                    (user_id, recent[0], created_at),
                )
                self.conn.commit()
            return True

        if action_type == "PLACE_ORDER":
            # Handle single order or MM multi-order
            orders_list = action.get("orders", [action])
            for order in orders_list:
                side = order.get("side", "buy")
                symbol = order.get("symbol", "BTC")
                price = order.get("price", 0)
                quantity = order.get("quantity", 0)

                pair_id = self._pair_id_by_symbols.get((symbol, "USD"))
                if pair_id is None or price <= 0 or quantity <= 0:
                    continue

                try:
                    self.exchange.place_order(
                        user_id, pair_id, side, price, quantity, step
                    )
                except (ValueError, Exception) as e:
                    logger.debug(
                        "Order rejected for user %d: %s", user_id, e
                    )
            return True

        return False

    def _inject_news(self, step: int, news_events: list[NewsEvent]) -> None:
        """Insert news events into the DB and create corresponding posts."""
        for event in news_events:
            self.conn.execute(
                "INSERT INTO news_event "
                "(step, source, audience, content, title, sentiment_valence, "
                " magnitude, credibility, affected_instruments) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    step,
                    event.source,
                    event.audience.value if hasattr(event.audience, "value") else str(event.audience),
                    event.body or "",
                    event.title,
                    event.sentiment_valence,
                    event.magnitude,
                    event.credibility,
                    json.dumps(event.affected_assets),
                ),
            )
        self.conn.commit()

    def _check_market_news(
        self, step: int, match_results: dict[int, list[int]]
    ) -> None:
        """If a pair's price moved >5% since prev step, emit synthetic news."""
        for pair_id in self._pair_ids:
            state = self.exchange.pair_state(pair_id)
            lp = state["last_price"]
            pcp = state["prev_close_price"]
            if lp is None or pcp is None or pcp <= 0:
                continue
            change = abs(lp / pcp - 1)
            if change > 0.05:
                direction = "up" if lp > pcp else "down"
                title = (
                    f"MARKET ALERT: {state['base_symbol']} moved {direction} "
                    f"{change * 100:.1f}%"
                )
                self.conn.execute(
                    "INSERT INTO news_event "
                    "(step, source, audience, title, sentiment_valence) "
                    "VALUES (?, 'market_auto', 'all', ?, ?)",
                    (step, title, 0.5 if direction == "up" else -0.5),
                )
                self.conn.commit()

    def _update_memories(
        self,
        step: int,
        active_indices: list[int],
        actions: list[dict],
        tiers: list[Tier],
    ) -> None:
        """Write memory rows for all personas."""
        # Active agents: write action memory
        for idx, action in zip(active_indices, actions):
            user_id = self.persona_idx_to_user_id[idx]
            action_type = action.get("action_type", "DO_NOTHING")
            if action_type != "DO_NOTHING":
                self.memory.write_action(user_id, step, action_type, action)

        # All agents: write news observations
        news_this_step = self._news_by_step.get(step, [])
        if news_this_step:
            for idx in range(len(self.personas)):
                user_id = self.persona_idx_to_user_id[idx]
                for event in news_this_step:
                    self.memory.write_observation(
                        user_id,
                        step,
                        MemoryKind.NEWS,
                        {"title": event.title, "source": event.source},
                    )

    def _snapshot(
        self,
        step: int,
        tiers: list[Tier],
        actions: list[dict],
        active_indices: list[int],
        stimuli: list[float],
    ) -> None:
        """Record telemetry for this step."""
        # Prices
        price_rows: list[dict] = []
        for pair_id in self._pair_ids:
            state = self.exchange.pair_state(pair_id)
            # Compute step volume from trades
            trade_count = self.conn.execute(
                "SELECT COALESCE(SUM(quantity), 0) FROM trade WHERE pair_id = ? AND step = ?",
                (pair_id, step),
            ).fetchone()
            vol = trade_count[0] if trade_count else 0
            price_rows.append({
                "pair_id": pair_id,
                "base_symbol": state["base_symbol"],
                "quote_symbol": state["quote_symbol"],
                "last_price": state["last_price"] or 0.0,
                "prev_close_price": state["prev_close_price"] or 0.0,
                "volume_step": float(vol),
            })
        self.telemetry.record_prices(step, price_rows)

        # Trades
        trade_rows_db = self.conn.execute(
            "SELECT trade_id, pair_id, price, quantity, buyer_id, seller_id "
            "FROM trade WHERE step = ?",
            (step,),
        ).fetchall()
        trade_rows = [
            {
                "trade_id": r[0],
                "pair_id": r[1],
                "price": r[2],
                "qty": r[3],
                "buyer_id": r[4],
                "seller_id": r[5],
            }
            for r in trade_rows_db
        ]
        self.telemetry.record_trades(step, trade_rows)

        # Actions
        action_rows: list[dict] = []
        for idx, action in zip(active_indices, actions):
            persona = self.personas[idx]
            user_id = self.persona_idx_to_user_id[idx]
            action_rows.append({
                "user_id": user_id,
                "archetype": persona.archetype,
                "tier": tiers[idx].value,
                "action_type": action.get("action_type", "DO_NOTHING"),
            })
        self.telemetry.record_actions(step, action_rows)

        # Stimuli
        stim_rows: list[dict] = []
        for idx in range(len(self.personas)):
            user_id = self.persona_idx_to_user_id.get(idx)
            if user_id is None:
                continue
            s = stimuli[idx] if idx < len(stimuli) else 0.0
            stim_rows.append({
                "user_id": user_id,
                "price_stimulus": s,
                "news_stimulus": 1.0 if self._news_by_step.get(step) else 0.0,
                "total_stimulus": s,
            })
        self.telemetry.record_stimuli(step, stim_rows)

        # Tier distribution
        tier_counts = Counter(t.value for t in tiers)
        tier_rows = [
            {"tier": t, "count": c}
            for t, c in tier_counts.items()
        ]
        self.telemetry.record_tiers(step, tier_rows)

        # News
        news_this_step = self._news_by_step.get(step, [])
        if news_this_step:
            news_rows = [
                {
                    "source": e.source,
                    "title": e.title,
                    "sentiment_valence": e.sentiment_valence,
                    "audience": e.audience.value if hasattr(e.audience, "value") else str(e.audience),
                    "affected_assets": json.dumps(e.affected_assets),
                }
                for e in news_this_step
            ]
            self.telemetry.record_news(step, news_rows)

    def _record_initial_totals(self) -> None:
        """Record sum(amount + locked) per instrument for conservation checks."""
        rows = self.conn.execute(
            "SELECT instrument_id, SUM(amount + locked) FROM balance "
            "GROUP BY instrument_id"
        ).fetchall()
        self._initial_totals = {r[0]: r[1] for r in rows}

    def _check_conservation(self, step: int) -> None:
        """Verify sum(amount + locked) per instrument hasn't drifted.

        Note: stablecoin peg snap and trade-fee mechanisms would invalidate
        naive conservation. For MVP without fees, it should hold tightly.
        """
        rows = self.conn.execute(
            "SELECT instrument_id, SUM(amount + locked) FROM balance "
            "GROUP BY instrument_id"
        ).fetchall()
        current = {r[0]: r[1] for r in rows}

        conservation_rows: list[dict] = []
        for inst_id, expected in self._initial_totals.items():
            actual = current.get(inst_id, 0.0)
            drift = abs(actual - expected)

            # Look up symbol
            sym_row = self.conn.execute(
                "SELECT symbol FROM instrument WHERE instrument_id = ?",
                (inst_id,),
            ).fetchone()
            symbol = sym_row[0] if sym_row else f"instrument_{inst_id}"

            conservation_rows.append({
                "instrument": symbol,
                "total_amount": actual,
                "total_locked": 0.0,  # split not tracked here
                "total_supply": expected,
            })

            if drift > 1e-6:
                logger.warning(
                    "Conservation drift at step %d: %s expected=%.8f actual=%.8f drift=%.8f",
                    step,
                    symbol,
                    expected,
                    actual,
                    drift,
                )

        self.telemetry.record_conservation(step, conservation_rows)
