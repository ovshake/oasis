"""Social graph construction + feed filter.

Phase 6 — hybrid persona-affinity x preferential-attachment graph + per-persona
feed filtering. No LLM calls, no network I/O.
"""
from __future__ import annotations

import sqlite3
from collections import defaultdict
from datetime import datetime, timezone

import numpy as np
from pydantic import BaseModel, Field

from oasis.crypto.persona import ArchetypeTemplate, Persona

# ---------------------------------------------------------------------------
# Graph construction
# ---------------------------------------------------------------------------

class GraphConfig(BaseModel):
    """Controls graph construction. Defaults match Phase 6 spec."""
    affinity_threshold: float = 0.3
    seed: int = 42
    use_target_counts: bool = True
    fallback_target: tuple[int, int] = (10, 50)


class SocialGraphBuilder:
    """Hybrid: persona-affinity x preferential attachment.

    Stage 1: For each new agent A being added, compute the eligible pool --
    other agents already in the graph whose archetype is weighted >=
    affinity_threshold in A.follows_archetypes.

    Stage 2: Sample A's follow targets from the eligible pool with weights
    w_i = (current_follower_count_i + 1) * affinity_weight (Barabasi-Albert style).

    Build order: randomized per seed but recorded so graphs are reproducible.
    """

    def __init__(
        self,
        personas: list[Persona],
        templates: dict[str, ArchetypeTemplate],
        config: GraphConfig = GraphConfig(),
    ):
        self.personas = personas
        self.templates = templates
        self.config = config
        self.rng = np.random.default_rng(config.seed)
        self.edges: dict[int, set[int]] = defaultdict(set)
        self.follower_count: np.ndarray = np.zeros(len(personas), dtype=np.int64)
        self._build_order: list[int] = []

    def build(self) -> None:
        """Construct the full graph."""
        N = len(self.personas)
        order = self.rng.permutation(N).tolist()
        self._build_order = order
        for step_i, idx in enumerate(order):
            in_graph_indices = order[:step_i]
            self._add_agent(idx, in_graph_indices)

    def _add_agent(self, new_idx: int, in_graph_indices: list[int]) -> None:
        if not in_graph_indices:
            return
        new_persona = self.personas[new_idx]
        follows_weights = new_persona.follows_archetypes

        # Stage 1: filter to eligible archetypes
        eligible = [
            i
            for i in in_graph_indices
            if follows_weights.get(self.personas[i].archetype, 0.0)
            >= self.config.affinity_threshold
        ]
        if not eligible:
            return

        # Stage 2: target count
        k = self._sample_target_count(new_persona)
        k = min(k, len(eligible))
        if k == 0:
            return

        # Weighted sample: (follower_count + 1) * affinity_weight
        weights = np.array(
            [
                (self.follower_count[i] + 1)
                * max(follows_weights.get(self.personas[i].archetype, 0.0), 0.01)
                for i in eligible
            ],
            dtype=np.float64,
        )
        probs = weights / weights.sum()
        chosen_positions = self.rng.choice(
            len(eligible), size=k, replace=False, p=probs
        )
        for pos in chosen_positions:
            followee = eligible[int(pos)]
            self.edges[new_idx].add(followee)
            self.follower_count[followee] += 1

    def _sample_target_count(self, persona: Persona) -> int:
        tpl = self.templates.get(persona.archetype)
        if self.config.use_target_counts and tpl is not None:
            val = tpl.target_follow_count.sample(self.rng)
            return max(1, int(round(val)))
        lo, hi = self.config.fallback_target
        return int(self.rng.integers(lo, hi + 1))

    # ---- Introspection ----

    def degree_distribution(self) -> tuple[np.ndarray, np.ndarray]:
        """Return (out_degrees, in_degrees) arrays indexed by persona."""
        N = len(self.personas)
        out_deg = np.array([len(self.edges.get(i, set())) for i in range(N)])
        return out_deg, self.follower_count.copy()

    def top_k_followers(self, k: int = 10) -> list[tuple[int, int, str]]:
        """Return [(persona_idx, follower_count, archetype)] top-k by followers."""
        indices = np.argsort(self.follower_count)[::-1][:k]
        return [
            (int(i), int(self.follower_count[i]), self.personas[i].archetype)
            for i in indices
        ]

    def followers_by_archetype(self, archetype: str) -> dict[str, int]:
        """For agents of *archetype*, breakdown of their followers' archetypes."""
        counts: dict[str, int] = defaultdict(int)
        target_indices = [
            i for i, p in enumerate(self.personas) if p.archetype == archetype
        ]
        for target_idx in target_indices:
            for follower_idx, followees in self.edges.items():
                if target_idx in followees:
                    counts[self.personas[follower_idx].archetype] += 1
        return dict(counts)

    def edges_to_list(self) -> list[tuple[int, int]]:
        """Return [(follower_idx, followee_idx)] for all edges."""
        result: list[tuple[int, int]] = []
        for follower, followees in self.edges.items():
            for followee in followees:
                result.append((follower, followee))
        return result

    def persist_to_db(
        self,
        conn: sqlite3.Connection,
        persona_id_to_user_id: dict[str, int],
    ) -> int:
        """Write edges to the existing OASIS 'follow' table.

        Returns number of edges written.
        """
        now = datetime.now(timezone.utc).isoformat()
        rows: list[tuple[int, int, str]] = []
        for follower_idx, followees in self.edges.items():
            follower_pid = self.personas[follower_idx].persona_id
            follower_uid = persona_id_to_user_id.get(follower_pid)
            if follower_uid is None:
                continue
            for followee_idx in followees:
                followee_pid = self.personas[followee_idx].persona_id
                followee_uid = persona_id_to_user_id.get(followee_pid)
                if followee_uid is None:
                    continue
                rows.append((follower_uid, followee_uid, now))
        conn.executemany(
            "INSERT OR IGNORE INTO follow(follower_id, followee_id, created_at) "
            "VALUES (?, ?, ?)",
            rows,
        )
        conn.commit()
        return len(rows)


# ---------------------------------------------------------------------------
# Feed filter
# ---------------------------------------------------------------------------

SOURCE_WEIGHT_BOOST = 3.0


class FeedFilterConfig(BaseModel):
    """Scoring weights for feed filtering."""
    topic_weight: float = 1.0
    sentiment_weight: float = 0.5
    source_archetype_weight: float = 1.0
    max_posts_in_feed: int = 20


class Post(BaseModel):
    """Canonical post shape used for filtering."""
    post_id: int
    author_user_id: int
    author_persona_idx: int | None = None
    content: str = ""
    sentiment: float = 0.0
    topics: list[str] = Field(default_factory=list)
    is_news_event: bool = False
    created_step: int = 0


class FeedFilter:
    """Given an agent and a pool of candidate posts, filter/score to produce
    the subset the LLM actually sees.

    Scoring formula per (post, persona):
        follow_edge(author) * SOURCE_WEIGHT_BOOST
        + topic_match * topic_weight
        + sentiment_alignment * sentiment_weight
        + archetype_affinity * source_archetype_weight

    News events with is_news_event=True always pass for personas with
    social_sensitivity >= 0.5 or archetype in {news_trader, kol}.
    """

    _NEWS_ALWAYS_ARCHETYPES = {"news_trader", "kol"}

    def __init__(
        self,
        personas: list[Persona],
        graph: SocialGraphBuilder,
        config: FeedFilterConfig = FeedFilterConfig(),
    ):
        self.personas = personas
        self.graph = graph
        self.config = config
        # Pre-build follow lookup: persona_idx -> set of followee persona_idx
        self._follow_set: dict[int, set[int]] = dict(graph.edges)

    def _score_post(self, persona_idx: int, post: Post) -> float:
        persona = self.personas[persona_idx]
        score = 0.0

        # Follow edge boost
        if post.author_persona_idx is not None:
            if post.author_persona_idx in self._follow_set.get(persona_idx, set()):
                score += SOURCE_WEIGHT_BOOST

        # Topic match: post topics intersect persona holdings keys
        if post.topics:
            holding_keys = set(persona.initial_holdings.keys())
            if holding_keys & set(post.topics):
                score += self.config.topic_weight

        # Sentiment alignment: sign(sentiment) * sign(herding)
        if abs(post.sentiment) > 1e-6 and abs(persona.herding_coefficient) > 1e-6:
            alignment = (
                1.0
                if (post.sentiment > 0) == (persona.herding_coefficient > 0)
                else -0.5
            )
            score += alignment * self.config.sentiment_weight

        # Archetype affinity
        if post.author_persona_idx is not None:
            author_arch = self.personas[post.author_persona_idx].archetype
            affinity = persona.follows_archetypes.get(author_arch, 0.0)
            score += affinity * self.config.source_archetype_weight

        return score

    def _news_always_passes(self, persona_idx: int, post: Post) -> bool:
        """Return True if this news post should always be included."""
        if not post.is_news_event:
            return False
        persona = self.personas[persona_idx]
        if persona.archetype in self._NEWS_ALWAYS_ARCHETYPES:
            return True
        if persona.social_sensitivity >= 0.5:
            return True
        return False

    def filter_for(self, persona_idx: int, posts: list[Post]) -> list[Post]:
        """Rank and return top max_posts_in_feed posts for this persona."""
        if not posts:
            return []

        scored: list[tuple[float, int, Post]] = []
        for i, post in enumerate(posts):
            if self._news_always_passes(persona_idx, post):
                # Give news-always posts a very high score to guarantee inclusion
                scored.append((1e6 + i, i, post))
            else:
                scored.append((self._score_post(persona_idx, post), i, post))

        # Sort descending by score, stable by original order (index)
        scored.sort(key=lambda x: (-x[0], x[1]))
        return [p for _, _, p in scored[: self.config.max_posts_in_feed]]

    def batch_filter(
        self, persona_indices: list[int], posts: list[Post]
    ) -> dict[int, list[Post]]:
        """Filter same post pool for multiple personas."""
        if not posts:
            return {idx: [] for idx in persona_indices}

        # Pre-compute shared data: author archetypes, topic sets
        author_archetypes: list[str | None] = []
        post_topic_sets: list[set[str]] = []
        for post in posts:
            if post.author_persona_idx is not None:
                author_archetypes.append(self.personas[post.author_persona_idx].archetype)
            else:
                author_archetypes.append(None)
            post_topic_sets.append(set(post.topics))

        results: dict[int, list[Post]] = {}
        for pidx in persona_indices:
            persona = self.personas[pidx]
            follow_set = self._follow_set.get(pidx, set())
            holding_keys = set(persona.initial_holdings.keys())

            scored: list[tuple[float, int]] = []
            for i, post in enumerate(posts):
                if self._news_always_passes(pidx, post):
                    scored.append((1e6 + i, i))
                    continue

                score = 0.0
                # Follow edge
                if post.author_persona_idx is not None:
                    if post.author_persona_idx in follow_set:
                        score += SOURCE_WEIGHT_BOOST
                # Topic match
                if post_topic_sets[i] and (holding_keys & post_topic_sets[i]):
                    score += self.config.topic_weight
                # Sentiment alignment
                if abs(post.sentiment) > 1e-6 and abs(persona.herding_coefficient) > 1e-6:
                    alignment = (
                        1.0
                        if (post.sentiment > 0) == (persona.herding_coefficient > 0)
                        else -0.5
                    )
                    score += alignment * self.config.sentiment_weight
                # Archetype affinity
                arch = author_archetypes[i]
                if arch is not None:
                    score += persona.follows_archetypes.get(arch, 0.0) * self.config.source_archetype_weight

                scored.append((score, i))

            scored.sort(key=lambda x: (-x[0], x[1]))
            results[pidx] = [posts[i] for _, i in scored[: self.config.max_posts_in_feed]]

        return results
