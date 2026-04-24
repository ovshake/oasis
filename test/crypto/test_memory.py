"""Tests for oasis.crypto.memory — Phase 7 short-term memory store."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

# conftest.py already registers the lightweight oasis namespace stubs.
from oasis.crypto.instrument import CryptoSchema
from oasis.crypto.memory import MemoryEntry, MemoryKind, MemoryStore

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


@pytest.fixture()
def db() -> sqlite3.Connection:
    """In-memory SQLite with crypto schema loaded."""
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(_USER_TABLE_DDL)
    conn.execute("PRAGMA foreign_keys = ON")

    schema = CryptoSchema(":memory:")
    schema.init_schema(conn)

    # Insert a couple of users so FK constraints pass.
    conn.execute(
        "INSERT INTO user (user_id, user_name, name) VALUES (1, 'alice', 'Alice')"
    )
    conn.execute(
        "INSERT INTO user (user_id, user_name, name) VALUES (2, 'bob', 'Bob')"
    )
    conn.commit()
    return conn


@pytest.fixture()
def store(db: sqlite3.Connection) -> MemoryStore:
    return MemoryStore(db)


# ---------------------------------------------------------------------------
# 1. write returns monotonically increasing memory_id
# ---------------------------------------------------------------------------

class TestWriteBasics:
    def test_write_returns_monotonic_ids(self, store: MemoryStore) -> None:
        id1 = store.write(1, 0, MemoryKind.ACTION, {"action_type": "buy"})
        id2 = store.write(1, 1, MemoryKind.ACTION, {"action_type": "sell"})
        id3 = store.write(1, 2, MemoryKind.PNL, {"pnl_delta": 0.5})
        assert id1 < id2 < id3

    # 2. write with kind as string and as enum both work
    def test_write_accepts_string_and_enum_kind(self, store: MemoryStore) -> None:
        id_enum = store.write(1, 0, MemoryKind.ACTION, {"x": 1})
        id_str = store.write(1, 1, "action", {"x": 2})
        assert id_enum is not None
        assert id_str is not None
        assert id_enum < id_str


# ---------------------------------------------------------------------------
# 3. JSON round-trip
# ---------------------------------------------------------------------------

class TestJsonRoundTrip:
    def test_content_round_trips_unchanged(self, store: MemoryStore) -> None:
        content = {
            "action_type": "buy",
            "details": {"pair": "BTC/USD", "qty": 0.5, "price": 65000.0},
            "tags": ["momentum", "breakout"],
        }
        mid = store.write(1, 0, MemoryKind.ACTION, content)
        entries = store.last_actions(1, n=1)
        assert len(entries) == 1
        assert entries[0].content == content
        assert entries[0].memory_id == mid


# ---------------------------------------------------------------------------
# 4. last_actions returns newest n in newest-first order
# ---------------------------------------------------------------------------

class TestLastActions:
    def test_returns_newest_n_newest_first(self, store: MemoryStore) -> None:
        for step in range(10):
            store.write_action(1, step, f"act_{step}", {"step": step})
        result = store.last_actions(1, n=3)
        assert len(result) == 3
        steps = [e.step for e in result]
        assert steps == [9, 8, 7]

    # 5. last_actions returns empty list for user with no history
    def test_returns_empty_for_no_history(self, store: MemoryStore) -> None:
        result = store.last_actions(2, n=5)
        assert result == []


# ---------------------------------------------------------------------------
# 6. last_pnl returns None for no PNL entry
# 7. last_pnl returns most recent when multiple exist
# ---------------------------------------------------------------------------

class TestLastPnl:
    def test_returns_none_when_no_pnl(self, store: MemoryStore) -> None:
        # Write a non-PNL entry so the user exists in memory.
        store.write_action(1, 0, "buy", {})
        assert store.last_pnl(1) is None

    def test_returns_most_recent_pnl(self, store: MemoryStore) -> None:
        store.write_pnl(1, 0, pnl_delta=-2.0, wealth_usd=9800.0)
        store.write_pnl(1, 5, pnl_delta=3.5, wealth_usd=10350.0)
        store.write_pnl(1, 10, pnl_delta=1.2, wealth_usd=10500.0)

        result = store.last_pnl(1)
        assert result is not None
        assert result.step == 10
        assert result.content["pnl_delta"] == 1.2
        assert result.content["wealth_usd"] == 10500.0


# ---------------------------------------------------------------------------
# 8. notable_observations filters kinds + respects since_step
# ---------------------------------------------------------------------------

class TestNotableObservations:
    def test_filters_to_observation_kinds(self, store: MemoryStore) -> None:
        store.write_action(1, 0, "buy", {})  # should be excluded
        store.write_pnl(1, 1, 0.5, 10000)  # should be excluded
        store.write_observation(1, 2, MemoryKind.SAW_POST, {"author": "whale1"})
        store.write_observation(1, 3, MemoryKind.NEWS, {"title": "Fed hikes"})
        store.write_observation(1, 4, MemoryKind.MENTION, {"author": "bob"})
        store.write_observation(1, 5, MemoryKind.REPLY, {"author": "alice"})

        result = store.notable_observations(1, k=10)
        assert len(result) == 4
        kinds = {e.kind for e in result}
        assert kinds == {
            MemoryKind.SAW_POST, MemoryKind.NEWS,
            MemoryKind.MENTION, MemoryKind.REPLY,
        }

    def test_since_step_filter(self, store: MemoryStore) -> None:
        store.write_observation(1, 5, MemoryKind.NEWS, {"title": "old news"})
        store.write_observation(1, 50, MemoryKind.NEWS, {"title": "recent news"})
        store.write_observation(1, 100, MemoryKind.SAW_POST, {"author": "x"})

        result = store.notable_observations(1, k=10, since_step=50)
        assert len(result) == 2
        steps = {e.step for e in result}
        assert steps == {50, 100}

    def test_write_observation_rejects_invalid_kind(self, store: MemoryStore) -> None:
        with pytest.raises(ValueError, match="observation_kind"):
            store.write_observation(1, 0, MemoryKind.ACTION, {})


# ---------------------------------------------------------------------------
# 9. window returns only entries within [step-lookback, step]
# ---------------------------------------------------------------------------

class TestWindow:
    def test_window_range(self, store: MemoryStore) -> None:
        # Write entries at steps 80, 85, 90, 95, 100, 105.
        for s in [80, 85, 90, 95, 100, 105]:
            store.write(1, s, MemoryKind.ACTION, {"s": s})

        result = store.window(1, step=100, lookback=10)
        steps = [e.step for e in result]
        # [90, 100] inclusive
        assert steps == [90, 95, 100]

    def test_window_sorted_asc(self, store: MemoryStore) -> None:
        for s in [50, 48, 45, 42, 40]:
            store.write(1, s, MemoryKind.NEWS, {"s": s})
        result = store.window(1, step=50, lookback=10)
        steps = [e.step for e in result]
        assert steps == sorted(steps)


# ---------------------------------------------------------------------------
# 10. build_prompt_block empty-memory stub
# ---------------------------------------------------------------------------

class TestBuildPromptBlock:
    def test_empty_memory_stub(self, store: MemoryStore) -> None:
        block = store.build_prompt_block(1, step=0)
        assert block == "=== RECENT MEMORY ===\nNo prior activity.\n==="

    # 11. includes actions in newest-first order
    def test_actions_newest_first(self, store: MemoryStore) -> None:
        store.write_action(1, 1, "buy", {"pair": "BTC/USD"})
        store.write_action(1, 2, "sell", {"pair": "ETH/USD"})
        store.write_action(1, 3, "post", {"text": "bullish"})

        block = store.build_prompt_block(1, step=3, n_actions=3)
        lines = block.split("\n")

        # Find the action lines
        action_lines = [l for l in lines if l.strip().startswith("- step")]
        # Within the "Recent actions:" section, they should be newest first.
        action_section = []
        in_actions = False
        for line in lines:
            if "Recent actions:" in line:
                in_actions = True
                continue
            if in_actions and line.strip().startswith("- step"):
                action_section.append(line)
            elif in_actions and not line.strip().startswith("- step"):
                break

        assert len(action_section) == 3
        assert "step 3" in action_section[0]
        assert "step 2" in action_section[1]
        assert "step 1" in action_section[2]

    # 12. bounded output length with large history
    def test_block_length_bounded(self, store: MemoryStore) -> None:
        # Insert 1000+ memory entries across various kinds.
        for step in range(500):
            store.write_action(
                1, step, "trade",
                {"pair": "BTC/USD", "qty": 0.1, "price": 65000 + step},
            )
            store.write_pnl(1, step, pnl_delta=0.01 * step, wealth_usd=10000 + step)
            store.write_observation(
                1, step, MemoryKind.SAW_POST,
                {"author": f"user_{step}", "snippet": f"Post content at step {step}"},
            )

        block = store.build_prompt_block(1, step=499, n_actions=5, n_observations=5)
        assert len(block) < 5000, f"Block too long: {len(block)} chars"


# ---------------------------------------------------------------------------
# 13. Deterministic serialization
# ---------------------------------------------------------------------------

class TestDeterministicSerialization:
    def test_sort_keys_produces_identical_text(self, db: sqlite3.Connection) -> None:
        store = MemoryStore(db)

        # Two dicts with same key-value pairs in different insertion order.
        id1 = store.write(1, 0, MemoryKind.ACTION, {"a": 1, "b": 2})
        id2 = store.write(1, 1, MemoryKind.ACTION, {"b": 2, "a": 1})

        row1 = db.execute(
            "SELECT content_json FROM agent_memory WHERE memory_id = ?", (id1,)
        ).fetchone()
        row2 = db.execute(
            "SELECT content_json FROM agent_memory WHERE memory_id = ?", (id2,)
        ).fetchone()

        assert row1[0] == row2[0], (
            f"Expected identical JSON text, got:\n  {row1[0]}\n  {row2[0]}"
        )


# ---------------------------------------------------------------------------
# Extra: prompt block includes P&L and observations
# ---------------------------------------------------------------------------

class TestPromptBlockContent:
    def test_includes_pnl_and_observations(self, store: MemoryStore) -> None:
        store.write_action(1, 1, "buy", {"pair": "BTC/USD"})
        store.write_pnl(1, 2, pnl_delta=1.5, wealth_usd=10150.0)
        store.write_observation(1, 3, MemoryKind.NEWS, {"title": "CPI data released"})

        block = store.build_prompt_block(1, step=3)
        assert "Recent P&L:" in block
        assert "$10,150.00" in block
        assert "+1.50%" in block
        assert "CPI data released" in block
        assert "=== RECENT MEMORY (as of step 3) ===" in block
        assert block.endswith("===")
