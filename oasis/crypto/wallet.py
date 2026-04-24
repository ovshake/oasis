"""Multi-asset balance operations with escrow (amount + locked).

Phase 2 deliverable. All writes are single SQL statements — the caller
controls transactions (Exchange wraps them in BEGIN IMMEDIATE ... COMMIT).
"""

from __future__ import annotations

import sqlite3


class Wallet:
    """Multi-asset balance operations backed by the ``balance`` table."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def get(self, user_id: int, instrument_id: int) -> tuple[float, float]:
        """Return (amount, locked). Creates row with zeros if missing."""
        row = self.conn.execute(
            "SELECT amount, locked FROM balance "
            "WHERE user_id = ? AND instrument_id = ?",
            (user_id, instrument_id),
        ).fetchone()
        if row is not None:
            return (row[0], row[1])
        # Auto-create the row so callers never deal with None.
        self.conn.execute(
            "INSERT INTO balance (user_id, instrument_id, amount, locked) "
            "VALUES (?, ?, 0, 0)",
            (user_id, instrument_id),
        )
        return (0.0, 0.0)

    # ------------------------------------------------------------------
    # Writes — each is a single parameterised SQL statement
    # ------------------------------------------------------------------

    def credit(self, user_id: int, instrument_id: int, qty: float) -> None:
        """Add *qty* to amount. Raises on qty < 0."""
        if qty < 0:
            raise ValueError(f"credit qty must be non-negative, got {qty}")
        self._ensure_row(user_id, instrument_id)
        self.conn.execute(
            "UPDATE balance SET amount = amount + ? "
            "WHERE user_id = ? AND instrument_id = ?",
            (qty, user_id, instrument_id),
        )

    def debit(self, user_id: int, instrument_id: int, qty: float) -> None:
        """Subtract *qty* from amount. Raises if insufficient."""
        if qty < 0:
            raise ValueError(f"debit qty must be non-negative, got {qty}")
        self._ensure_row(user_id, instrument_id)
        amount, _ = self.get(user_id, instrument_id)
        if amount < qty:
            raise ValueError(
                f"Insufficient balance: user {user_id} instrument {instrument_id} "
                f"has {amount}, need {qty}"
            )
        self.conn.execute(
            "UPDATE balance SET amount = amount - ? "
            "WHERE user_id = ? AND instrument_id = ?",
            (qty, user_id, instrument_id),
        )

    def lock(self, user_id: int, instrument_id: int, qty: float) -> None:
        """Move *qty* from amount to locked (escrow). Raises if insufficient."""
        if qty < 0:
            raise ValueError(f"lock qty must be non-negative, got {qty}")
        self._ensure_row(user_id, instrument_id)
        amount, _ = self.get(user_id, instrument_id)
        if amount < qty:
            raise ValueError(
                f"Insufficient balance for lock: user {user_id} instrument "
                f"{instrument_id} has {amount}, need {qty}"
            )
        self.conn.execute(
            "UPDATE balance SET amount = amount - ?, locked = locked + ? "
            "WHERE user_id = ? AND instrument_id = ?",
            (qty, qty, user_id, instrument_id),
        )

    def unlock(self, user_id: int, instrument_id: int, qty: float) -> None:
        """Move *qty* from locked back to amount (cancel refund)."""
        if qty < 0:
            raise ValueError(f"unlock qty must be non-negative, got {qty}")
        self._ensure_row(user_id, instrument_id)
        _, locked = self.get(user_id, instrument_id)
        if locked < qty:
            raise ValueError(
                f"Insufficient locked balance for unlock: user {user_id} "
                f"instrument {instrument_id} has {locked} locked, need {qty}"
            )
        self.conn.execute(
            "UPDATE balance SET locked = locked - ?, amount = amount + ? "
            "WHERE user_id = ? AND instrument_id = ?",
            (qty, qty, user_id, instrument_id),
        )

    def consume_locked(
        self, user_id: int, instrument_id: int, qty: float
    ) -> None:
        """Remove *qty* from locked without returning to amount (fill)."""
        if qty < 0:
            raise ValueError(
                f"consume_locked qty must be non-negative, got {qty}"
            )
        self._ensure_row(user_id, instrument_id)
        _, locked = self.get(user_id, instrument_id)
        if locked < qty:
            raise ValueError(
                f"Insufficient locked balance for consume: user {user_id} "
                f"instrument {instrument_id} has {locked} locked, need {qty}"
            )
        self.conn.execute(
            "UPDATE balance SET locked = locked - ? "
            "WHERE user_id = ? AND instrument_id = ?",
            (qty, user_id, instrument_id),
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ensure_row(self, user_id: int, instrument_id: int) -> None:
        """Insert a zero-balance row if it doesn't exist yet."""
        self.conn.execute(
            "INSERT OR IGNORE INTO balance (user_id, instrument_id, amount, locked) "
            "VALUES (?, ?, 0, 0)",
            (user_id, instrument_id),
        )
