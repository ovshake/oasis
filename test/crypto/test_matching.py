"""Tests for oasis.crypto matching engine + exchange integration.

Comprehensive: basic match, crossed book, partial fill, multiple matches,
self-trade guard, cancel/refund, insufficient funds, conservation,
atomicity, and determinism.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest

from oasis.crypto.exchange import Exchange
from oasis.crypto.instrument import CryptoSchema
from oasis.crypto.wallet import Wallet

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent

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

# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------

def _make_db() -> sqlite3.Connection:
    """Return a fresh in-memory DB with schema, users, and BTC/USD pair."""
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(_USER_TABLE_DDL)
    conn.execute("PRAGMA foreign_keys = ON")

    schema = CryptoSchema(":memory:")
    schema.init_schema(conn)

    # Users: alice=1, bob=2, charlie=3, dave=4, eve=5
    for uid, name in [(1, "alice"), (2, "bob"), (3, "charlie"), (4, "dave"), (5, "eve")]:
        conn.execute(
            "INSERT INTO user (user_id, user_name, name) VALUES (?, ?, ?)",
            (uid, name, name.title()),
        )

    # Instruments: USD=1, BTC=2
    conn.execute(
        "INSERT INTO instrument (instrument_id, symbol, name, asset_class, "
        "is_quote_asset, default_price) VALUES (1, 'USD', 'US Dollar', 'fiat', 1, 1.0)"
    )
    conn.execute(
        "INSERT INTO instrument (instrument_id, symbol, name, asset_class, "
        "default_price) VALUES (2, 'BTC', 'Bitcoin', 'crypto', 80000.0)"
    )

    # Pair: BTC/USD = pair_id 1
    conn.execute(
        "INSERT INTO pair (pair_id, base_instrument_id, quote_instrument_id, "
        "last_price) VALUES (1, 2, 1, 80000.0)"
    )
    conn.commit()
    return conn


@pytest.fixture()
def conn() -> sqlite3.Connection:
    return _make_db()


@pytest.fixture()
def exchange(conn: sqlite3.Connection) -> Exchange:
    return Exchange(conn)


def _fund(exchange: Exchange, user_id: int, usd: float = 0, btc: float = 0) -> None:
    """Credit a user with USD and/or BTC. Helper for test setup."""
    if usd > 0:
        exchange.wallet.credit(user_id, 1, usd)  # instrument 1 = USD
    if btc > 0:
        exchange.wallet.credit(user_id, 2, btc)  # instrument 2 = BTC


def _balance(exchange: Exchange, user_id: int, instrument_id: int) -> tuple[float, float]:
    """Return (amount, locked) for a user/instrument."""
    return exchange.wallet.get(user_id, instrument_id)


def _total_supply(conn: sqlite3.Connection, instrument_id: int) -> float:
    """Sum of amount + locked across all users for an instrument."""
    row = conn.execute(
        "SELECT COALESCE(SUM(amount + locked), 0) FROM balance "
        "WHERE instrument_id = ?",
        (instrument_id,),
    ).fetchone()
    return row[0]


# ------------------------------------------------------------------
# 1. Basic match
# ------------------------------------------------------------------


class TestBasicMatch:
    def test_exact_match(self, exchange: Exchange, conn: sqlite3.Connection) -> None:
        """Alice sells 1 BTC @ $80k; Bob buys 1 BTC @ $80k. One trade."""
        _fund(exchange, 1, btc=1.0)        # alice has 1 BTC
        _fund(exchange, 2, usd=80_000.0)   # bob has $80k

        exchange.place_order(1, 1, "sell", 80_000.0, 1.0, step=1)
        exchange.place_order(2, 1, "buy", 80_000.0, 1.0, step=2)

        results = exchange.match_all_pairs(step=2)
        assert 1 in results
        assert len(results[1]) == 1

        # Verify trade row.
        trade = conn.execute(
            "SELECT price, quantity, buyer_id, seller_id FROM trade WHERE trade_id = ?",
            (results[1][0],),
        ).fetchone()
        assert trade == (80_000.0, 1.0, 2, 1)

        # Balances: alice has $80k, bob has 1 BTC.
        assert _balance(exchange, 1, 1) == (80_000.0, 0.0)  # alice USD
        assert _balance(exchange, 1, 2) == (0.0, 0.0)       # alice BTC
        assert _balance(exchange, 2, 1) == (0.0, 0.0)       # bob USD
        assert _balance(exchange, 2, 2) == (1.0, 0.0)       # bob BTC


# ------------------------------------------------------------------
# 2. Crossed book — maker price wins
# ------------------------------------------------------------------


class TestCrossedBook:
    def test_maker_price_wins(self, exchange: Exchange, conn: sqlite3.Connection) -> None:
        """Alice sells @ $80k (maker), Bob buys @ $81k (taker). Trade @ $80k.
        Bob gets $1k refund."""
        _fund(exchange, 1, btc=1.0)
        _fund(exchange, 2, usd=81_000.0)

        exchange.place_order(1, 1, "sell", 80_000.0, 1.0, step=1)
        exchange.place_order(2, 1, "buy", 81_000.0, 1.0, step=2)

        results = exchange.match_all_pairs(step=2)
        trade = conn.execute(
            "SELECT price FROM trade WHERE trade_id = ?",
            (results[1][0],),
        ).fetchone()
        assert trade[0] == 80_000.0

        # Bob's USD: started $81k, locked $81k, trade consumed $80k,
        # refund $1k → amount = $1k.
        bob_usd_amount, bob_usd_locked = _balance(exchange, 2, 1)
        assert bob_usd_amount == 1_000.0
        assert bob_usd_locked == 0.0


# ------------------------------------------------------------------
# 3. Partial fill
# ------------------------------------------------------------------


class TestPartialFill:
    def test_partial_fill(self, exchange: Exchange, conn: sqlite3.Connection) -> None:
        """Alice sells 5 BTC @ $80k; Bob buys 2 BTC @ $80k.
        Bob fully fills; Alice partially fills (filled_qty=2, status=open)."""
        _fund(exchange, 1, btc=5.0)
        _fund(exchange, 2, usd=160_000.0)

        sell_id = exchange.place_order(1, 1, "sell", 80_000.0, 5.0, step=1)
        exchange.place_order(2, 1, "buy", 80_000.0, 2.0, step=2)

        exchange.match_all_pairs(step=2)

        row = conn.execute(
            "SELECT filled_quantity, status FROM crypto_order WHERE order_id = ?",
            (sell_id,),
        ).fetchone()
        assert row[0] == 2.0
        assert row[1] == "open"


# ------------------------------------------------------------------
# 4. Multiple matches in one cycle
# ------------------------------------------------------------------


class TestMultipleMatches:
    def test_three_sellers_one_buyer(
        self, exchange: Exchange, conn: sqlite3.Connection
    ) -> None:
        """3 sellers (1 BTC each) + 1 buyer (3 BTC). All matched."""
        _fund(exchange, 1, btc=1.0)
        _fund(exchange, 2, btc=1.0)
        _fund(exchange, 3, btc=1.0)
        _fund(exchange, 4, usd=240_000.0)  # $80k x 3

        exchange.place_order(1, 1, "sell", 80_000.0, 1.0, step=1)
        exchange.place_order(2, 1, "sell", 80_000.0, 1.0, step=1)
        exchange.place_order(3, 1, "sell", 80_000.0, 1.0, step=1)
        exchange.place_order(4, 1, "buy", 80_000.0, 3.0, step=2)

        results = exchange.match_all_pairs(step=2)
        assert len(results[1]) == 3

        # Buyer should have 3 BTC, no USD.
        assert _balance(exchange, 4, 2)[0] == 3.0
        assert _balance(exchange, 4, 1) == (0.0, 0.0)


# ------------------------------------------------------------------
# 5. Self-trade guard
# ------------------------------------------------------------------


class TestSelfTradeGuard:
    def test_self_trade_cancelled(
        self, exchange: Exchange, conn: sqlite3.Connection
    ) -> None:
        """Alice sells then buys at same price. No trade; newer order cancelled."""
        _fund(exchange, 1, btc=1.0)
        _fund(exchange, 1, usd=80_000.0)

        sell_id = exchange.place_order(1, 1, "sell", 80_000.0, 1.0, step=1)
        buy_id = exchange.place_order(1, 1, "buy", 80_000.0, 1.0, step=2)

        results = exchange.match_all_pairs(step=2)
        # No trades.
        assert results.get(1, []) == []

        # Newer order (buy, higher order_id) is cancelled.
        buy_row = conn.execute(
            "SELECT status FROM crypto_order WHERE order_id = ?", (buy_id,)
        ).fetchone()
        assert buy_row[0] == "cancelled"

        # Older order (sell) remains open.
        sell_row = conn.execute(
            "SELECT status FROM crypto_order WHERE order_id = ?", (sell_id,)
        ).fetchone()
        assert sell_row[0] == "open"

        # Escrow refunded: buy order locked $80k quote → should be back.
        alice_usd_amount, alice_usd_locked = _balance(exchange, 1, 1)
        assert alice_usd_amount == 80_000.0
        assert alice_usd_locked == 0.0


# ------------------------------------------------------------------
# 6. Cancel refunds escrow
# ------------------------------------------------------------------


class TestCancelOrder:
    def test_cancel_refunds_full_escrow(self, exchange: Exchange) -> None:
        """Place buy 1 BTC @ $80k → $80k locked. Cancel. Locked returns."""
        _fund(exchange, 2, usd=80_000.0)
        order_id = exchange.place_order(2, 1, "buy", 80_000.0, 1.0, step=1)

        # Check locked.
        _, locked = _balance(exchange, 2, 1)
        assert locked == 80_000.0

        exchange.cancel_order(order_id)
        amount, locked = _balance(exchange, 2, 1)
        assert amount == 80_000.0
        assert locked == 0.0


# ------------------------------------------------------------------
# 7. Partial cancel refunds remaining
# ------------------------------------------------------------------


class TestPartialCancel:
    def test_partial_cancel_refunds_remaining(
        self, exchange: Exchange, conn: sqlite3.Connection
    ) -> None:
        """Sell 5 BTC @ $80k; fill 2 BTC; cancel. Only 3 BTC returned."""
        _fund(exchange, 1, btc=5.0)
        _fund(exchange, 2, usd=160_000.0)

        sell_id = exchange.place_order(1, 1, "sell", 80_000.0, 5.0, step=1)
        exchange.place_order(2, 1, "buy", 80_000.0, 2.0, step=2)
        exchange.match_all_pairs(step=2)

        # Alice: 3 BTC still locked (5 - 2 filled).
        alice_btc_amount, alice_btc_locked = _balance(exchange, 1, 2)
        assert alice_btc_locked == 3.0

        exchange.cancel_order(sell_id)
        alice_btc_amount, alice_btc_locked = _balance(exchange, 1, 2)
        assert alice_btc_amount == 3.0  # refunded
        assert alice_btc_locked == 0.0


# ------------------------------------------------------------------
# 8. Insufficient funds rejected
# ------------------------------------------------------------------


class TestInsufficientFunds:
    def test_buy_insufficient_funds(
        self, exchange: Exchange, conn: sqlite3.Connection
    ) -> None:
        """User with $10k tries buy 1 BTC @ $80k → raises, no order row."""
        _fund(exchange, 2, usd=10_000.0)

        with pytest.raises(ValueError, match="Insufficient"):
            exchange.place_order(2, 1, "buy", 80_000.0, 1.0, step=1)

        # No order row exists.
        row = conn.execute(
            "SELECT COUNT(*) FROM crypto_order WHERE user_id = 2"
        ).fetchone()
        assert row[0] == 0

        # Balance unchanged.
        amount, locked = _balance(exchange, 2, 1)
        assert amount == 10_000.0
        assert locked == 0.0


# ------------------------------------------------------------------
# 9. Conservation invariant
# ------------------------------------------------------------------


class TestConservation:
    def test_conservation_over_random_operations(
        self, exchange: Exchange, conn: sqlite3.Connection
    ) -> None:
        """50 random place/cancel/match operations; supply invariant holds."""
        import random
        rng = random.Random(42)

        # Seed generous balances.
        for uid in range(1, 6):
            _fund(exchange, uid, usd=1_000_000.0, btc=100.0)

        initial_usd = _total_supply(conn, 1)
        initial_btc = _total_supply(conn, 2)

        order_ids: list[int] = []
        step = 0

        for _ in range(50):
            action = rng.choice(["place_buy", "place_sell", "cancel", "match"])
            uid = rng.randint(1, 5)

            if action == "place_buy":
                price = rng.uniform(70_000, 90_000)
                qty = rng.uniform(0.01, 2.0)
                try:
                    oid = exchange.place_order(uid, 1, "buy", price, qty, step)
                    order_ids.append(oid)
                except ValueError:
                    pass  # insufficient funds
            elif action == "place_sell":
                price = rng.uniform(70_000, 90_000)
                qty = rng.uniform(0.01, 2.0)
                try:
                    oid = exchange.place_order(uid, 1, "sell", price, qty, step)
                    order_ids.append(oid)
                except ValueError:
                    pass
            elif action == "cancel" and order_ids:
                oid = rng.choice(order_ids)
                exchange.cancel_order(oid)
            elif action == "match":
                step += 1
                exchange.match_all_pairs(step)

            # Invariant check after every operation.
            current_usd = _total_supply(conn, 1)
            current_btc = _total_supply(conn, 2)
            assert abs(current_usd - initial_usd) < 1e-6, (
                f"USD conservation violated: {initial_usd} -> {current_usd}"
            )
            assert abs(current_btc - initial_btc) < 1e-6, (
                f"BTC conservation violated: {initial_btc} -> {current_btc}"
            )


# ------------------------------------------------------------------
# 10. Atomicity (simulated crash mid-match)
# ------------------------------------------------------------------


class TestAtomicity:
    def test_rollback_on_credit_failure(
        self, conn: sqlite3.Connection
    ) -> None:
        """Monkey-patch Wallet.credit to raise after the first credit;
        verify rollback — no trade row, balances unchanged."""
        exchange = Exchange(conn)
        _fund(exchange, 1, btc=1.0)
        _fund(exchange, 2, usd=80_000.0)

        exchange.place_order(1, 1, "sell", 80_000.0, 1.0, step=1)
        exchange.place_order(2, 1, "buy", 80_000.0, 1.0, step=2)

        # Snapshot state before match.
        pre_usd_alice = _balance(exchange, 1, 1)
        pre_btc_alice = _balance(exchange, 1, 2)
        pre_usd_bob = _balance(exchange, 2, 1)
        pre_btc_bob = _balance(exchange, 2, 2)

        call_count = [0]
        original_credit = Wallet.credit

        def failing_credit(self_w, user_id, instrument_id, qty):
            call_count[0] += 1
            if call_count[0] >= 2:
                raise RuntimeError("Simulated crash in credit")
            return original_credit(self_w, user_id, instrument_id, qty)

        with patch.object(Wallet, "credit", failing_credit):
            with pytest.raises(RuntimeError, match="Simulated crash"):
                exchange.match_all_pairs(step=2)

        # No trade rows.
        trade_count = conn.execute("SELECT COUNT(*) FROM trade").fetchone()[0]
        assert trade_count == 0

        # Balances unchanged (the transaction was rolled back).
        assert _balance(exchange, 1, 1) == pre_usd_alice
        assert _balance(exchange, 1, 2) == pre_btc_alice
        assert _balance(exchange, 2, 1) == pre_usd_bob
        assert _balance(exchange, 2, 2) == pre_btc_bob

        # Order statuses unchanged.
        orders = conn.execute(
            "SELECT status FROM crypto_order ORDER BY order_id"
        ).fetchall()
        assert all(row[0] == "open" for row in orders)


# ------------------------------------------------------------------
# 11. Deterministic match order
# ------------------------------------------------------------------


class TestDeterminism:
    def test_same_inputs_same_outputs(self) -> None:
        """Two sessions with identical inputs produce identical trade tables."""
        trade_tables: list[list[tuple]] = []

        for _ in range(2):
            c = _make_db()
            ex = Exchange(c)

            # Same funding.
            ex.wallet.credit(1, 2, 10.0)   # alice: 10 BTC
            ex.wallet.credit(2, 1, 500_000.0)  # bob: $500k
            ex.wallet.credit(3, 2, 5.0)    # charlie: 5 BTC
            ex.wallet.credit(4, 1, 300_000.0)  # dave: $300k

            # Same orders in same steps.
            ex.place_order(1, 1, "sell", 80_000.0, 3.0, step=1)
            ex.place_order(3, 1, "sell", 79_000.0, 2.0, step=1)
            ex.place_order(2, 1, "buy", 80_000.0, 4.0, step=2)
            ex.place_order(4, 1, "buy", 79_500.0, 1.0, step=2)

            ex.match_all_pairs(step=2)

            rows = c.execute(
                "SELECT pair_id, buy_order_id, sell_order_id, price, quantity, "
                "buyer_id, seller_id, step FROM trade ORDER BY trade_id"
            ).fetchall()
            trade_tables.append(rows)

        assert trade_tables[0] == trade_tables[1]
        assert len(trade_tables[0]) > 0  # sanity: trades actually happened

    def test_same_inputs_same_insertion_order_deterministic(self) -> None:
        """Same orders inserted in the SAME order produce identical trade
        sequences — order_id assignment is deterministic when insertion
        order is fixed."""
        trade_tables: list[list[tuple]] = []

        for _ in range(2):
            c = _make_db()
            ex = Exchange(c)
            ex.wallet.credit(1, 2, 5.0)
            ex.wallet.credit(2, 2, 5.0)
            ex.wallet.credit(3, 1, 200_000.0)

            ex.place_order(1, 1, "sell", 80_000.0, 1.0, step=1)
            ex.place_order(2, 1, "sell", 79_000.0, 1.0, step=1)
            ex.place_order(3, 1, "buy", 80_000.0, 2.0, step=1)
            ex.match_all_pairs(step=1)

            rows = c.execute(
                "SELECT pair_id, buy_order_id, sell_order_id, price, quantity, "
                "buyer_id, seller_id, step FROM trade ORDER BY trade_id"
            ).fetchall()
            trade_tables.append(rows)

        assert trade_tables[0] == trade_tables[1]
        assert len(trade_tables[0]) > 0

    def test_price_priority_dominates_over_insertion_order(self) -> None:
        """Regardless of insertion order, the best-priced ask ($79k) is
        matched first when crossing a bid at $80k."""
        for sells_first in [True, False]:
            c = _make_db()
            ex = Exchange(c)
            ex.wallet.credit(1, 2, 5.0)
            ex.wallet.credit(2, 2, 5.0)
            ex.wallet.credit(3, 1, 200_000.0)

            if sells_first:
                ex.place_order(1, 1, "sell", 80_000.0, 1.0, step=1)
                ex.place_order(2, 1, "sell", 79_000.0, 1.0, step=1)
                ex.place_order(3, 1, "buy", 80_000.0, 2.0, step=1)
            else:
                ex.place_order(3, 1, "buy", 80_000.0, 2.0, step=1)
                ex.place_order(1, 1, "sell", 80_000.0, 1.0, step=1)
                ex.place_order(2, 1, "sell", 79_000.0, 1.0, step=1)

            ex.match_all_pairs(step=1)

            rows = c.execute(
                "SELECT seller_id, price FROM trade ORDER BY trade_id"
            ).fetchall()
            # In both cases, user 2 (cheapest seller @ $79k) is matched first.
            assert len(rows) == 2
            assert rows[0][0] == 2  # seller_id = user 2 first


# ------------------------------------------------------------------
# Additional edge cases
# ------------------------------------------------------------------


class TestOrderBookSnapshot:
    def test_aggregated_levels(self, exchange: Exchange) -> None:
        _fund(exchange, 1, usd=300_000.0)
        _fund(exchange, 2, usd=300_000.0)
        _fund(exchange, 3, btc=5.0)

        exchange.place_order(1, 1, "buy", 79_000.0, 1.0, step=1)
        exchange.place_order(2, 1, "buy", 79_000.0, 2.0, step=1)
        exchange.place_order(3, 1, "sell", 81_000.0, 3.0, step=1)

        book = exchange.order_book_snapshot(1)
        assert len(book["bids"]) == 1
        assert book["bids"][0] == (79_000.0, 3.0)  # aggregated
        assert len(book["asks"]) == 1
        assert book["asks"][0] == (81_000.0, 3.0)


class TestPairState:
    def test_pair_state_basic(self, exchange: Exchange) -> None:
        state = exchange.pair_state(1)
        assert state["base_symbol"] == "BTC"
        assert state["quote_symbol"] == "USD"
        assert state["last_price"] == 80_000.0

    def test_pair_state_updates_after_trade(self, exchange: Exchange) -> None:
        _fund(exchange, 1, btc=1.0)
        _fund(exchange, 2, usd=75_000.0)
        exchange.place_order(1, 1, "sell", 75_000.0, 1.0, step=1)
        exchange.place_order(2, 1, "buy", 75_000.0, 1.0, step=2)
        exchange.match_all_pairs(step=2)
        state = exchange.pair_state(1)
        assert state["last_price"] == 75_000.0


class TestEnsureBalances:
    def test_ensure_balances_basic(self, exchange: Exchange) -> None:
        exchange.ensure_balances(1, {"USD": 100_000.0, "BTC": 2.0})
        assert _balance(exchange, 1, 1) == (100_000.0, 0.0)
        assert _balance(exchange, 1, 2) == (2.0, 0.0)

    def test_ensure_balances_rejects_existing(self, exchange: Exchange) -> None:
        exchange.ensure_balances(1, {"USD": 100.0})
        with pytest.raises(ValueError, match="already exists"):
            exchange.ensure_balances(1, {"USD": 200.0})

    def test_ensure_balances_unknown_symbol(self, exchange: Exchange) -> None:
        with pytest.raises(ValueError, match="Unknown instrument"):
            exchange.ensure_balances(1, {"DOGE": 1000.0})


class TestEdgeCases:
    def test_cancel_already_filled_is_noop(self, exchange: Exchange) -> None:
        _fund(exchange, 1, btc=1.0)
        _fund(exchange, 2, usd=80_000.0)
        sell_id = exchange.place_order(1, 1, "sell", 80_000.0, 1.0, step=1)
        exchange.place_order(2, 1, "buy", 80_000.0, 1.0, step=2)
        exchange.match_all_pairs(step=2)
        # Cancel a filled order — should be a no-op.
        exchange.cancel_order(sell_id)

    def test_cancel_already_cancelled_is_noop(self, exchange: Exchange) -> None:
        _fund(exchange, 2, usd=80_000.0)
        order_id = exchange.place_order(2, 1, "buy", 80_000.0, 1.0, step=1)
        exchange.cancel_order(order_id)
        exchange.cancel_order(order_id)  # no-op, no error

    def test_invalid_side_raises(self, exchange: Exchange) -> None:
        with pytest.raises(ValueError, match="Invalid side"):
            exchange.place_order(1, 1, "hold", 80_000.0, 1.0, step=1)

    def test_invalid_price_raises(self, exchange: Exchange) -> None:
        with pytest.raises(ValueError):
            exchange.place_order(1, 1, "buy", 0, 1.0, step=1)

    def test_invalid_quantity_raises(self, exchange: Exchange) -> None:
        with pytest.raises(ValueError):
            exchange.place_order(1, 1, "buy", 80_000.0, 0, step=1)

    def test_match_empty_book(self, exchange: Exchange) -> None:
        """match_all_pairs on an empty book returns empty dict."""
        results = exchange.match_all_pairs(step=1)
        assert results == {}
