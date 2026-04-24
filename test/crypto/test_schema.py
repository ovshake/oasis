"""Tests for oasis.crypto schema initialisation and seeding."""

from __future__ import annotations

import importlib
import sqlite3
import sys
from pathlib import Path

import pytest

# Resolve paths relative to the repo root (two levels up from this file).
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_ASSETS_YAML = _REPO_ROOT / "data" / "market" / "assets.yaml"

# ---------------------------------------------------------------------------
# Import CryptoSchema without triggering the heavy oasis/__init__.py chain.
# We register the intermediate packages as empty namespace stubs so that
# ``oasis.crypto.instrument`` resolves without loading sentence_transformers
# and the rest of the OASIS platform stack.
# ---------------------------------------------------------------------------
for _pkg in ("oasis", "oasis.crypto"):
    if _pkg not in sys.modules:
        _spec = importlib.util.spec_from_file_location(
            _pkg,
            _REPO_ROOT / _pkg.replace(".", "/") / "__init__.py",
            submodule_search_locations=[
                str(_REPO_ROOT / _pkg.replace(".", "/"))
            ],
        )
        _mod = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
        sys.modules[_pkg] = _mod
        # Only execute oasis.crypto's __init__ (it is lightweight).
        # Skip oasis/__init__.py (it pulls in heavy deps).
        if _pkg == "oasis.crypto":
            _spec.loader.exec_module(_mod)  # type: ignore[union-attr]

from oasis.crypto.instrument import CryptoSchema  # noqa: E402

# Minimal DDL for the OASIS ``user`` table so that FKs in crypto tables
# (balance, crypto_order, trade, agent_persona, agent_memory) can resolve.
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
    """Return an in-memory SQLite connection with the crypto schema loaded."""
    conn = sqlite3.connect(":memory:")
    # Create the base OASIS user table first (crypto tables reference it).
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(_USER_TABLE_DDL)
    conn.execute("PRAGMA foreign_keys = ON")

    schema = CryptoSchema(":memory:")
    schema.init_schema(conn)
    return conn


@pytest.fixture()
def seeded_db(db: sqlite3.Connection) -> sqlite3.Connection:
    """Return a connection with schema + seeded assets/pairs."""
    schema = CryptoSchema(":memory:")
    schema.seed_assets(db, str(_ASSETS_YAML))
    return db


# ------------------------------------------------------------------
# Schema + seed verification
# ------------------------------------------------------------------


class TestSchemaInit:
    def test_foreign_keys_enabled(self, db: sqlite3.Connection) -> None:
        (fk_on,) = db.execute("PRAGMA foreign_keys").fetchone()
        assert fk_on == 1

    def test_all_tables_exist(self, db: sqlite3.Connection) -> None:
        rows = db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        table_names = {r[0] for r in rows}
        expected = {
            "user",
            "instrument",
            "pair",
            "balance",
            "crypto_order",
            "trade",
            "news_event",
            "persona",
            "agent_persona",
            "agent_memory",
        }
        assert expected.issubset(table_names), (
            f"Missing tables: {expected - table_names}"
        )

    def test_indices_exist(self, db: sqlite3.Connection) -> None:
        rows = db.execute(
            "SELECT name FROM sqlite_master WHERE type='index' ORDER BY name"
        ).fetchall()
        index_names = {r[0] for r in rows}
        expected = {
            "idx_crypto_order_pair_side_status_price",
            "idx_trade_pair_step",
            "idx_news_step",
            "idx_memory_user_step",
        }
        assert expected.issubset(index_names), (
            f"Missing indices: {expected - index_names}"
        )


class TestSeedAssets:
    def test_instrument_count(self, seeded_db: sqlite3.Connection) -> None:
        (count,) = seeded_db.execute("SELECT COUNT(*) FROM instrument").fetchone()
        assert count == 6

    def test_pair_count(self, seeded_db: sqlite3.Connection) -> None:
        """5 non-quote assets each paired with 1 quote asset (USD) = 5 pairs."""
        (count,) = seeded_db.execute("SELECT COUNT(*) FROM pair").fetchone()
        assert count == 5

    def test_pair_last_price_seeded(self, seeded_db: sqlite3.Connection) -> None:
        """Pairs should have last_price set from the base instrument default."""
        row = seeded_db.execute(
            """
            SELECT p.last_price
            FROM pair p
            JOIN instrument i ON p.base_instrument_id = i.instrument_id
            WHERE i.symbol = 'BTC'
            """
        ).fetchone()
        assert row is not None
        assert row[0] == 80000.0

    def test_quote_asset_is_usd(self, seeded_db: sqlite3.Connection) -> None:
        rows = seeded_db.execute(
            """
            SELECT DISTINCT i.symbol
            FROM pair p
            JOIN instrument i ON p.quote_instrument_id = i.instrument_id
            """
        ).fetchall()
        symbols = {r[0] for r in rows}
        assert symbols == {"USD"}


# ------------------------------------------------------------------
# Constraint enforcement
# ------------------------------------------------------------------


class TestConstraints:
    def test_bad_asset_class_rejected(self, db: sqlite3.Connection) -> None:
        with pytest.raises(sqlite3.IntegrityError):
            db.execute(
                """
                INSERT INTO instrument (symbol, name, asset_class)
                VALUES ('BAD', 'Bad Asset', 'nonsense')
                """
            )

    def test_duplicate_symbol_rejected(self, seeded_db: sqlite3.Connection) -> None:
        with pytest.raises(sqlite3.IntegrityError):
            seeded_db.execute(
                """
                INSERT INTO instrument (symbol, name, asset_class)
                VALUES ('BTC', 'Duplicate Bitcoin', 'crypto')
                """
            )

    def test_fk_violation_pair(self, db: sqlite3.Connection) -> None:
        """Inserting a pair with a nonexistent base_instrument_id must fail."""
        with pytest.raises(sqlite3.IntegrityError):
            db.execute(
                """
                INSERT INTO pair (base_instrument_id, quote_instrument_id)
                VALUES (9999, 9998)
                """
            )

    def test_bad_order_side_rejected(self, db: sqlite3.Connection) -> None:
        with pytest.raises(sqlite3.IntegrityError):
            db.execute(
                """
                INSERT INTO crypto_order
                    (user_id, pair_id, side, quantity, step)
                VALUES (1, 1, 'invalid_side', 1.0, 0)
                """
            )

    def test_bad_order_type_rejected(self, db: sqlite3.Connection) -> None:
        with pytest.raises(sqlite3.IntegrityError):
            db.execute(
                """
                INSERT INTO crypto_order
                    (user_id, pair_id, side, order_type, quantity, step)
                VALUES (1, 1, 'buy', 'stop_loss', 1.0, 0)
                """
            )

    def test_bad_order_status_rejected(self, db: sqlite3.Connection) -> None:
        with pytest.raises(sqlite3.IntegrityError):
            db.execute(
                """
                INSERT INTO crypto_order
                    (user_id, pair_id, side, status, quantity, step)
                VALUES (1, 1, 'buy', 'expired', 1.0, 0)
                """
            )
