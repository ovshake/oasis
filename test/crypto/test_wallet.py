"""Tests for oasis.crypto.wallet — multi-asset balance operations."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

# Reuse the conftest light-import shim — it runs before any test import.
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


@pytest.fixture()
def conn() -> sqlite3.Connection:
    """In-memory DB with schema + a test user + a test instrument."""
    db = sqlite3.connect(":memory:")
    db.execute("PRAGMA foreign_keys = ON")
    db.executescript(_USER_TABLE_DDL)
    db.execute("PRAGMA foreign_keys = ON")

    schema = CryptoSchema(":memory:")
    schema.init_schema(db)

    # Insert a test user and a test instrument.
    db.execute(
        "INSERT INTO user (user_id, user_name, name) VALUES (1, 'alice', 'Alice')"
    )
    db.execute(
        "INSERT INTO instrument (instrument_id, symbol, name, asset_class) "
        "VALUES (1, 'USD', 'US Dollar', 'fiat')"
    )
    db.execute(
        "INSERT INTO instrument (instrument_id, symbol, name, asset_class) "
        "VALUES (2, 'BTC', 'Bitcoin', 'crypto')"
    )
    db.commit()
    return db


@pytest.fixture()
def wallet(conn: sqlite3.Connection) -> Wallet:
    return Wallet(conn)


# ------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------


class TestWalletGet:
    def test_fresh_wallet_returns_zeros(self, wallet: Wallet) -> None:
        amount, locked = wallet.get(1, 1)
        assert amount == 0.0
        assert locked == 0.0

    def test_get_creates_row(self, wallet: Wallet, conn: sqlite3.Connection) -> None:
        wallet.get(1, 1)
        row = conn.execute(
            "SELECT amount, locked FROM balance WHERE user_id=1 AND instrument_id=1"
        ).fetchone()
        assert row == (0, 0)


class TestWalletCredit:
    def test_credit_increases_amount(self, wallet: Wallet) -> None:
        wallet.credit(1, 1, 100.0)
        amount, locked = wallet.get(1, 1)
        assert amount == 100.0
        assert locked == 0.0

    def test_credit_debit_round_trip(self, wallet: Wallet) -> None:
        wallet.credit(1, 1, 500.0)
        wallet.debit(1, 1, 200.0)
        amount, locked = wallet.get(1, 1)
        assert amount == 300.0
        assert locked == 0.0


class TestWalletDebit:
    def test_debit_insufficient_raises(self, wallet: Wallet) -> None:
        wallet.credit(1, 1, 50.0)
        with pytest.raises(ValueError, match="Insufficient"):
            wallet.debit(1, 1, 100.0)

    def test_debit_exact_balance(self, wallet: Wallet) -> None:
        wallet.credit(1, 1, 100.0)
        wallet.debit(1, 1, 100.0)
        amount, _ = wallet.get(1, 1)
        assert amount == 0.0


class TestWalletLockUnlock:
    def test_lock_unlock_round_trip(self, wallet: Wallet) -> None:
        wallet.credit(1, 1, 1000.0)
        wallet.lock(1, 1, 400.0)
        amount, locked = wallet.get(1, 1)
        assert amount == 600.0
        assert locked == 400.0

        wallet.unlock(1, 1, 400.0)
        amount, locked = wallet.get(1, 1)
        assert amount == 1000.0
        assert locked == 0.0

    def test_lock_insufficient_raises(self, wallet: Wallet) -> None:
        wallet.credit(1, 1, 50.0)
        with pytest.raises(ValueError, match="Insufficient"):
            wallet.lock(1, 1, 100.0)

    def test_unlock_insufficient_raises(self, wallet: Wallet) -> None:
        wallet.credit(1, 1, 100.0)
        wallet.lock(1, 1, 50.0)
        with pytest.raises(ValueError, match="Insufficient"):
            wallet.unlock(1, 1, 100.0)


class TestWalletConsumeLocked:
    def test_consume_locked_reduces_locked(self, wallet: Wallet) -> None:
        wallet.credit(1, 1, 1000.0)
        wallet.lock(1, 1, 500.0)
        wallet.consume_locked(1, 1, 200.0)
        amount, locked = wallet.get(1, 1)
        assert amount == 500.0  # unchanged
        assert locked == 300.0  # reduced

    def test_consume_locked_insufficient_raises(self, wallet: Wallet) -> None:
        wallet.credit(1, 1, 100.0)
        wallet.lock(1, 1, 50.0)
        with pytest.raises(ValueError, match="Insufficient"):
            wallet.consume_locked(1, 1, 100.0)


class TestWalletNegativeQty:
    @pytest.mark.parametrize("method", ["credit", "debit", "lock", "unlock", "consume_locked"])
    def test_negative_qty_raises(self, wallet: Wallet, method: str) -> None:
        with pytest.raises(ValueError, match="non-negative"):
            getattr(wallet, method)(1, 1, -1.0)


class TestWalletMultiAsset:
    def test_independent_instruments(self, wallet: Wallet) -> None:
        wallet.credit(1, 1, 1000.0)  # USD
        wallet.credit(1, 2, 5.0)     # BTC
        usd_amount, _ = wallet.get(1, 1)
        btc_amount, _ = wallet.get(1, 2)
        assert usd_amount == 1000.0
        assert btc_amount == 5.0
