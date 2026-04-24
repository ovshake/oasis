"""Top-level crypto exchange orchestrator.

Phase 2 deliverable. Owns the Wallet and MatchingEngine, exposes the
public API that the harness (Phase 8) and action gate (Phase 5) use.
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from typing import Iterator

from oasis.crypto.matching import MatchingEngine
from oasis.crypto.wallet import Wallet


@contextmanager
def _transaction(conn: sqlite3.Connection) -> Iterator[None]:
    """BEGIN IMMEDIATE ... COMMIT with rollback on exception."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield
    except Exception:
        conn.rollback()
        raise
    else:
        conn.commit()


class Exchange:
    """Top-level crypto exchange orchestrator."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        # Switch to manual transaction control so our explicit BEGIN/COMMIT
        # never collides with Python's implicit transaction management.
        conn.isolation_level = None
        self.conn = conn
        self.wallet = Wallet(conn)
        self.matcher = MatchingEngine(conn, self.wallet)

    # ----------------------------------------------------------------
    # Account setup
    # ----------------------------------------------------------------

    def ensure_balances(
        self, user_id: int, holdings: dict[str, float]
    ) -> None:
        """Initialise a user's balances from ``{symbol: amount}``.

        Looks up ``instrument_id`` for each symbol via the ``instrument``
        table. Raises if a balance row already has a non-zero amount
        (caller should use fresh users).
        """
        with _transaction(self.conn):
            for symbol, amount in holdings.items():
                row = self.conn.execute(
                    "SELECT instrument_id FROM instrument WHERE symbol = ?",
                    (symbol,),
                ).fetchone()
                if row is None:
                    raise ValueError(f"Unknown instrument symbol: {symbol!r}")
                instrument_id = row[0]

                existing = self.conn.execute(
                    "SELECT amount, locked FROM balance "
                    "WHERE user_id = ? AND instrument_id = ?",
                    (user_id, instrument_id),
                ).fetchone()
                if existing is not None and (existing[0] != 0 or existing[1] != 0):
                    raise ValueError(
                        f"Balance already exists for user {user_id}, "
                        f"instrument {symbol} ({instrument_id}): "
                        f"amount={existing[0]}, locked={existing[1]}"
                    )
                self.conn.execute(
                    "INSERT OR REPLACE INTO balance "
                    "(user_id, instrument_id, amount, locked) "
                    "VALUES (?, ?, ?, 0)",
                    (user_id, instrument_id, amount),
                )

    # ----------------------------------------------------------------
    # Order placement + cancellation
    # ----------------------------------------------------------------

    def place_order(
        self,
        user_id: int,
        pair_id: int,
        side: str,
        price: float,
        quantity: float,
        step: int,
    ) -> int:
        """Place a limit order. Returns order_id.

        Escrow is locked atomically with the order insert. Does NOT
        trigger matching — matching is driven by
        ``match_all_pairs(step)`` at end of tick.
        """
        if side not in ("buy", "sell"):
            raise ValueError(f"Invalid side: {side!r}")
        if price <= 0:
            raise ValueError(f"Price must be positive, got {price}")
        if quantity <= 0:
            raise ValueError(f"Quantity must be positive, got {quantity}")

        # Resolve pair instruments for escrow.
        pair_row = self.conn.execute(
            "SELECT base_instrument_id, quote_instrument_id FROM pair "
            "WHERE pair_id = ?",
            (pair_id,),
        ).fetchone()
        if pair_row is None:
            raise ValueError(f"Unknown pair_id {pair_id}")
        base_id, quote_id = pair_row

        with _transaction(self.conn):
            if side == "buy":
                self.wallet.lock(user_id, quote_id, price * quantity)
            else:
                self.wallet.lock(user_id, base_id, quantity)

            cursor = self.conn.execute(
                "INSERT INTO crypto_order "
                "(user_id, pair_id, side, order_type, price, quantity, "
                " filled_quantity, status, step) "
                "VALUES (?, ?, ?, 'limit', ?, ?, 0, 'open', ?)",
                (user_id, pair_id, side, price, quantity, step),
            )
            order_id: int = cursor.lastrowid  # type: ignore[assignment]

        return order_id

    def cancel_order(self, order_id: int) -> None:
        """Cancel an open order. Refund remaining escrow.

        No-op if order is already ``'filled'`` or ``'cancelled'``.
        """
        row = self.conn.execute(
            "SELECT user_id, pair_id, side, price, quantity, "
            "       filled_quantity, status "
            "FROM crypto_order WHERE order_id = ?",
            (order_id,),
        ).fetchone()
        if row is None:
            raise ValueError(f"Unknown order_id {order_id}")

        user_id, pair_id, side, price, quantity, filled_qty, status = row

        if status in ("filled", "cancelled"):
            return  # no-op

        pair_row = self.conn.execute(
            "SELECT base_instrument_id, quote_instrument_id FROM pair "
            "WHERE pair_id = ?",
            (pair_id,),
        ).fetchone()
        base_id, quote_id = pair_row  # type: ignore[misc]

        remaining = quantity - filled_qty

        with _transaction(self.conn):
            if side == "buy":
                self.wallet.unlock(user_id, quote_id, price * remaining)
            else:
                self.wallet.unlock(user_id, base_id, remaining)

            self.conn.execute(
                "UPDATE crypto_order SET status = 'cancelled' "
                "WHERE order_id = ?",
                (order_id,),
            )

    # ----------------------------------------------------------------
    # Tick driver
    # ----------------------------------------------------------------

    def match_all_pairs(self, step: int) -> dict[int, list[int]]:
        """Called once per tick from the harness.

        Returns ``{pair_id: [trade_ids]}``. Each pair's match cycle
        runs in its own ``BEGIN IMMEDIATE`` transaction.
        """
        # Find all pairs that have at least one open order.
        rows = self.conn.execute(
            "SELECT DISTINCT pair_id FROM crypto_order WHERE status = 'open'"
        ).fetchall()

        results: dict[int, list[int]] = {}
        for (pair_id,) in rows:
            with _transaction(self.conn):
                trade_ids = self.matcher.match_pair(pair_id, step)
            if trade_ids:
                results[pair_id] = trade_ids
        return results

    # ----------------------------------------------------------------
    # Introspection
    # ----------------------------------------------------------------

    def order_book_snapshot(
        self, pair_id: int, depth: int = 5
    ) -> dict[str, list[tuple[float, float]]]:
        """Return aggregated order book levels.

        ``{bids: [(price, total_qty), ...], asks: [(price, total_qty), ...]}``
        sorted best-to-worst (bids descending, asks ascending).
        """
        bids = self.conn.execute(
            "SELECT price, SUM(quantity - filled_quantity) AS qty "
            "FROM crypto_order "
            "WHERE pair_id = ? AND side = 'buy' AND status = 'open' "
            "GROUP BY price ORDER BY price DESC LIMIT ?",
            (pair_id, depth),
        ).fetchall()

        asks = self.conn.execute(
            "SELECT price, SUM(quantity - filled_quantity) AS qty "
            "FROM crypto_order "
            "WHERE pair_id = ? AND side = 'sell' AND status = 'open' "
            "GROUP BY price ORDER BY price ASC LIMIT ?",
            (pair_id, depth),
        ).fetchall()

        return {
            "bids": [(row[0], row[1]) for row in bids],
            "asks": [(row[0], row[1]) for row in asks],
        }

    def pair_state(self, pair_id: int) -> dict:
        """Return ``{last_price, prev_close_price, base_symbol, quote_symbol}``."""
        row = self.conn.execute(
            "SELECT p.last_price, p.prev_close_price, "
            "       b.symbol AS base_symbol, q.symbol AS quote_symbol "
            "FROM pair p "
            "JOIN instrument b ON p.base_instrument_id = b.instrument_id "
            "JOIN instrument q ON p.quote_instrument_id = q.instrument_id "
            "WHERE p.pair_id = ?",
            (pair_id,),
        ).fetchone()
        if row is None:
            raise ValueError(f"Unknown pair_id {pair_id}")
        return {
            "last_price": row[0],
            "prev_close_price": row[1],
            "base_symbol": row[2],
            "quote_symbol": row[3],
        }
