"""Short-term memory store backed by the agent_memory table.

Phase 7 deliverable. Provides structured write/read operations and a
prompt-injection helper. No LLM calls, no reflection, no summarization.
"""

from __future__ import annotations

import json
import sqlite3
from enum import Enum

from pydantic import BaseModel


# ---------------------------------------------------------------------------
# Memory kinds
# ---------------------------------------------------------------------------

class MemoryKind(str, Enum):
    ACTION = "action"
    PNL = "pnl"
    SAW_POST = "saw_post"
    MENTION = "mention"
    REPLY = "reply"
    NEWS = "news"


_OBSERVATION_KINDS = frozenset({
    MemoryKind.SAW_POST,
    MemoryKind.MENTION,
    MemoryKind.REPLY,
    MemoryKind.NEWS,
})


# ---------------------------------------------------------------------------
# Memory entry (Pydantic model)
# ---------------------------------------------------------------------------

class MemoryEntry(BaseModel):
    memory_id: int | None = None
    user_id: int
    step: int
    kind: MemoryKind
    content: dict  # JSON-serializable; stored as content_json TEXT


# ---------------------------------------------------------------------------
# Serialization helpers
# ---------------------------------------------------------------------------

def _serialize(content: dict) -> str:
    """Deterministic JSON serialization."""
    return json.dumps(content, sort_keys=True, default=str)


def _deserialize(text: str) -> dict:
    return json.loads(text)


def _normalize_kind(kind: MemoryKind | str) -> str:
    """Accept both MemoryKind enum and plain string, return the string value."""
    if isinstance(kind, MemoryKind):
        return kind.value
    return str(kind)


def _row_to_entry(row: tuple) -> MemoryEntry:
    """Convert a DB row (memory_id, user_id, step, kind, content_json) to a MemoryEntry."""
    return MemoryEntry(
        memory_id=row[0],
        user_id=row[1],
        step=row[2],
        kind=MemoryKind(row[3]),
        content=_deserialize(row[4]),
    )


# ---------------------------------------------------------------------------
# Store
# ---------------------------------------------------------------------------

_SELECT_COLS = "memory_id, user_id, step, kind, content_json"


class MemoryStore:
    """Thin wrapper over the agent_memory table.

    Writes are immediate (single-statement INSERT).
    Reads support windowed retrieval per agent.
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    # ------- Writes -------

    def write(
        self,
        user_id: int,
        step: int,
        kind: MemoryKind | str,
        content: dict,
    ) -> int:
        """Insert one memory row. Returns memory_id."""
        cur = self._conn.execute(
            "INSERT INTO agent_memory (user_id, step, kind, content_json) "
            "VALUES (?, ?, ?, ?)",
            (user_id, step, _normalize_kind(kind), _serialize(content)),
        )
        self._conn.commit()
        return cur.lastrowid  # type: ignore[return-value]

    def write_action(
        self, user_id: int, step: int, action_type: str, details: dict
    ) -> int:
        """Convenience: write(kind=ACTION, content={'action_type': ..., 'details': ...})."""
        return self.write(
            user_id, step, MemoryKind.ACTION,
            {"action_type": action_type, "details": details},
        )

    def write_pnl(
        self, user_id: int, step: int, pnl_delta: float, wealth_usd: float
    ) -> int:
        """Convenience for P&L. content = {'pnl_delta': ..., 'wealth_usd': ...}."""
        return self.write(
            user_id, step, MemoryKind.PNL,
            {"pnl_delta": pnl_delta, "wealth_usd": wealth_usd},
        )

    def write_observation(
        self,
        user_id: int,
        step: int,
        observation_kind: MemoryKind,
        content: dict,
    ) -> int:
        """Convenience. Validates that observation_kind is in {SAW_POST, MENTION, REPLY, NEWS}."""
        if observation_kind not in _OBSERVATION_KINDS:
            raise ValueError(
                f"observation_kind must be one of {_OBSERVATION_KINDS}, "
                f"got {observation_kind!r}"
            )
        return self.write(user_id, step, observation_kind, content)

    # ------- Reads -------

    def last_actions(self, user_id: int, n: int = 5) -> list[MemoryEntry]:
        """Return the n most recent ACTION entries (newest first)."""
        rows = self._conn.execute(
            f"SELECT {_SELECT_COLS} FROM agent_memory "
            "WHERE user_id = ? AND kind = ? "
            "ORDER BY step DESC, memory_id DESC LIMIT ?",
            (user_id, MemoryKind.ACTION.value, n),
        ).fetchall()
        return [_row_to_entry(r) for r in rows]

    def last_pnl(self, user_id: int) -> MemoryEntry | None:
        """Return the single most recent PNL entry, or None if no history."""
        row = self._conn.execute(
            f"SELECT {_SELECT_COLS} FROM agent_memory "
            "WHERE user_id = ? AND kind = ? "
            "ORDER BY step DESC, memory_id DESC LIMIT 1",
            (user_id, MemoryKind.PNL.value),
        ).fetchone()
        return _row_to_entry(row) if row else None

    def notable_observations(
        self, user_id: int, k: int = 10, since_step: int | None = None
    ) -> list[MemoryEntry]:
        """Return up to k most recent SAW_POST + MENTION + REPLY + NEWS entries.

        If since_step given, filter to step >= since_step.
        """
        kind_placeholders = ",".join("?" for _ in _OBSERVATION_KINDS)
        kind_values = [mk.value for mk in _OBSERVATION_KINDS]

        if since_step is not None:
            sql = (
                f"SELECT {_SELECT_COLS} FROM agent_memory "
                f"WHERE user_id = ? AND kind IN ({kind_placeholders}) "
                "AND step >= ? "
                "ORDER BY step DESC, memory_id DESC LIMIT ?"
            )
            params: tuple = (user_id, *kind_values, since_step, k)
        else:
            sql = (
                f"SELECT {_SELECT_COLS} FROM agent_memory "
                f"WHERE user_id = ? AND kind IN ({kind_placeholders}) "
                "ORDER BY step DESC, memory_id DESC LIMIT ?"
            )
            params = (user_id, *kind_values, k)

        rows = self._conn.execute(sql, params).fetchall()
        return [_row_to_entry(r) for r in rows]

    def window(
        self, user_id: int, step: int, lookback: int
    ) -> list[MemoryEntry]:
        """Return all entries for user_id in [step - lookback, step]. Sorted by step ASC."""
        rows = self._conn.execute(
            f"SELECT {_SELECT_COLS} FROM agent_memory "
            "WHERE user_id = ? AND step >= ? AND step <= ? "
            "ORDER BY step ASC, memory_id ASC",
            (user_id, step - lookback, step),
        ).fetchall()
        return [_row_to_entry(r) for r in rows]

    # ------- Prompt injection -------

    def build_prompt_block(
        self,
        user_id: int,
        step: int,
        n_actions: int = 5,
        n_observations: int = 5,
    ) -> str:
        """Return a formatted RECENT MEMORY block suitable for LLM prompt injection.

        If user has no memory yet, return the empty-memory stub.
        """
        actions = self.last_actions(user_id, n=n_actions)
        pnl = self.last_pnl(user_id)
        observations = self.notable_observations(user_id, k=n_observations)

        if not actions and pnl is None and not observations:
            return "=== RECENT MEMORY ===\nNo prior activity.\n==="

        lines: list[str] = [f"=== RECENT MEMORY (as of step {step}) ==="]

        # Recent actions (newest first)
        if actions:
            lines.append("Recent actions:")
            for entry in actions:
                c = entry.content
                action_type = c.get("action_type", "unknown")
                details = c.get("details", {})
                detail_str = _compact_details(details)
                lines.append(f"  - step {entry.step}: {action_type} -- {detail_str}")

        # P&L line
        if pnl is not None:
            c = pnl.content
            wealth = c.get("wealth_usd", 0)
            delta = c.get("pnl_delta", 0)
            sign = "+" if delta >= 0 else ""
            lines.append(f"Recent P&L: wealth ${wealth:,.2f}, delta {sign}{delta:.2f}%")

        # Observations (newest first)
        if observations:
            lines.append("Recent observations:")
            for entry in observations:
                desc = _describe_observation(entry)
                lines.append(f"  - step {entry.step}: {desc}")

        lines.append("===")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Formatting helpers (keep prompt blocks compact)
# ---------------------------------------------------------------------------

def _compact_details(details: dict) -> str:
    """One-line summary of action details, capped at 120 chars."""
    if not details:
        return "(no details)"
    parts = []
    for k, v in details.items():
        parts.append(f"{k}={v}")
    raw = ", ".join(parts)
    if len(raw) > 120:
        return raw[:117] + "..."
    return raw


def _describe_observation(entry: MemoryEntry) -> str:
    """One-line description of an observation entry, capped at 150 chars."""
    c = entry.content
    kind = entry.kind.value

    if kind == "saw_post":
        author = c.get("author", "?")
        snippet = c.get("snippet", c.get("text", ""))
        raw = f"[saw_post] @{author}: {snippet}"
    elif kind == "mention":
        author = c.get("author", "?")
        snippet = c.get("snippet", c.get("text", ""))
        raw = f"[mention] @{author} mentioned you: {snippet}"
    elif kind == "reply":
        author = c.get("author", "?")
        snippet = c.get("snippet", c.get("text", ""))
        raw = f"[reply] @{author} replied: {snippet}"
    elif kind == "news":
        title = c.get("title", c.get("headline", ""))
        raw = f"[news] {title}"
    else:
        raw = f"[{kind}] {json.dumps(c, default=str)}"

    if len(raw) > 150:
        return raw[:147] + "..."
    return raw
