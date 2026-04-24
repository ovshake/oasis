"""Order-book matching engine for the crypto exchange simulation.

Phase 2 deliverable. Price-time priority, maker-price execution, partial
fills, self-trade guard. The caller (Exchange) manages transactions.
"""

from __future__ import annotations

import sqlite3

from oasis.crypto.wallet import Wallet


class MatchingEngine:
    """Price-time priority matching with escrow settlement."""

    def __init__(self, conn: sqlite3.Connection, wallet: Wallet) -> None:
        self.conn = conn
        self.wallet = wallet

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def match_pair(self, pair_id: int, step: int) -> list[int]:
        """Run a complete match cycle for one pair. Returns new trade_ids.

        The full loop must be called inside a transaction managed by
        :class:`Exchange`.
        """
        # Look up base/quote instrument IDs for settlement.
        pair_row = self.conn.execute(
            "SELECT base_instrument_id, quote_instrument_id FROM pair "
            "WHERE pair_id = ?",
            (pair_id,),
        ).fetchone()
        if pair_row is None:
            raise ValueError(f"Unknown pair_id {pair_id}")
        base_id, quote_id = pair_row

        trade_ids: list[int] = []
        last_trade_price: float | None = None

        while True:
            bid = self._best_bid(pair_id)
            ask = self._best_ask(pair_id)

            if bid is None or ask is None:
                break

            bid_oid, bid_uid, bid_price, bid_qty, bid_filled = bid
            ask_oid, ask_uid, ask_price, ask_qty, ask_filled = ask

            # Book not crossed — stop.
            if bid_price < ask_price:
                break

            # ---- Self-trade guard ----
            if bid_uid == ask_uid:
                # Cancel the NEWER order (higher order_id) and refund.
                if bid_oid > ask_oid:
                    self._cancel_order_internal(
                        bid_oid, bid_uid, bid_price,
                        bid_qty, bid_filled, "buy",
                        base_id, quote_id,
                    )
                else:
                    self._cancel_order_internal(
                        ask_oid, ask_uid, ask_price,
                        ask_qty, ask_filled, "sell",
                        base_id, quote_id,
                    )
                continue

            # ---- Determine maker price ----
            # Maker is the resting order (earlier created_at / lower order_id).
            # The resting order's price is the trade price.
            if ask_oid < bid_oid:
                trade_price = ask_price
            else:
                trade_price = bid_price

            bid_remaining = bid_qty - bid_filled
            ask_remaining = ask_qty - ask_filled
            trade_qty = min(bid_remaining, ask_remaining)

            # ---- Settlement ----
            # Buyer pays quote: consume locked quote.
            self.wallet.consume_locked(bid_uid, quote_id, trade_price * trade_qty)
            # Buyer receives base.
            self.wallet.credit(bid_uid, base_id, trade_qty)
            # Seller pays base: consume locked base.
            self.wallet.consume_locked(ask_uid, base_id, trade_qty)
            # Seller receives quote.
            self.wallet.credit(ask_uid, quote_id, trade_price * trade_qty)

            # Refund if buyer got a better price than their limit.
            if trade_price < bid_price:
                diff = (bid_price - trade_price) * trade_qty
                self.wallet.unlock(bid_uid, quote_id, diff)

            # ---- Insert trade ----
            cursor = self.conn.execute(
                "INSERT INTO trade "
                "(pair_id, buy_order_id, sell_order_id, price, quantity, "
                " buyer_id, seller_id, step) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    pair_id, bid_oid, ask_oid,
                    trade_price, trade_qty,
                    bid_uid, ask_uid, step,
                ),
            )
            trade_ids.append(cursor.lastrowid)  # type: ignore[arg-type]

            # ---- Update order states ----
            new_bid_filled = bid_filled + trade_qty
            new_ask_filled = ask_filled + trade_qty

            bid_status = "filled" if new_bid_filled >= bid_qty else "open"
            ask_status = "filled" if new_ask_filled >= ask_qty else "open"

            self.conn.execute(
                "UPDATE crypto_order SET filled_quantity = ?, status = ? "
                "WHERE order_id = ?",
                (new_bid_filled, bid_status, bid_oid),
            )
            self.conn.execute(
                "UPDATE crypto_order SET filled_quantity = ?, status = ? "
                "WHERE order_id = ?",
                (new_ask_filled, ask_status, ask_oid),
            )

            last_trade_price = trade_price

        # Update pair.last_price if any trade happened.
        if last_trade_price is not None:
            self.conn.execute(
                "UPDATE pair SET last_price = ? WHERE pair_id = ?",
                (last_trade_price, pair_id),
            )

        return trade_ids

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _best_bid(self, pair_id: int) -> tuple | None:
        """Highest-price open buy, ties broken by order_id ASC."""
        return self.conn.execute(
            "SELECT order_id, user_id, price, quantity, filled_quantity "
            "FROM crypto_order "
            "WHERE pair_id = ? AND side = 'buy' AND status = 'open' "
            "ORDER BY price DESC, order_id ASC "
            "LIMIT 1",
            (pair_id,),
        ).fetchone()

    def _best_ask(self, pair_id: int) -> tuple | None:
        """Lowest-price open sell, ties broken by order_id ASC."""
        return self.conn.execute(
            "SELECT order_id, user_id, price, quantity, filled_quantity "
            "FROM crypto_order "
            "WHERE pair_id = ? AND side = 'sell' AND status = 'open' "
            "ORDER BY price ASC, order_id ASC "
            "LIMIT 1",
            (pair_id,),
        ).fetchone()

    def _cancel_order_internal(
        self,
        order_id: int,
        user_id: int,
        price: float,
        quantity: float,
        filled_quantity: float,
        side: str,
        base_id: int,
        quote_id: int,
    ) -> None:
        """Cancel an order and refund its remaining escrow."""
        remaining = quantity - filled_quantity
        if side == "buy":
            self.wallet.unlock(user_id, quote_id, price * remaining)
        else:
            self.wallet.unlock(user_id, base_id, remaining)
        self.conn.execute(
            "UPDATE crypto_order SET status = 'cancelled' WHERE order_id = ?",
            (order_id,),
        )
