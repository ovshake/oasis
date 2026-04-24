"""Asset/pair definitions and schema loader for the crypto exchange simulation."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import yaml

# Ordered list of SQL files — respects FK dependencies.
_SCHEMA_ORDER: list[str] = [
    "instrument.sql",
    "pair.sql",
    "balance.sql",
    "crypto_order.sql",
    "trade.sql",
    "news_event.sql",
    "persona.sql",
    "agent_persona.sql",
    "agent_memory.sql",
]

_SCHEMA_DIR = Path(__file__).resolve().parent / "schema"


class CryptoSchema:
    """Manages the crypto exchange schema lifecycle.

    Usage::

        schema = CryptoSchema("exchange.db")
        conn = sqlite3.connect(schema.db_path)
        schema.init_schema(conn)
        schema.seed_assets(conn, "data/market/assets.yaml")
    """

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path

    # ------------------------------------------------------------------
    # Schema initialisation
    # ------------------------------------------------------------------

    def init_schema(self, conn: sqlite3.Connection) -> None:
        """Enable foreign keys and execute all DDL files in dependency order."""
        conn.execute("PRAGMA foreign_keys = ON")
        for filename in _SCHEMA_ORDER:
            sql_path = _SCHEMA_DIR / filename
            sql = sql_path.read_text(encoding="utf-8")
            conn.executescript(sql)
        # Re-enable FK enforcement after executescript (executescript may
        # implicitly commit and reset pragma state in some SQLite builds).
        conn.execute("PRAGMA foreign_keys = ON")

    # ------------------------------------------------------------------
    # Seed helpers
    # ------------------------------------------------------------------

    def seed_assets(
        self,
        conn: sqlite3.Connection,
        assets_yaml_path: str,
    ) -> None:
        """Read *assets_yaml_path*, insert instruments, and auto-create pairs.

        Every non-quote-asset instrument is paired with every quote asset
        (typically USD).  Pair defaults (tick_size, lot_size) come from the
        ``pair_defaults`` section of the YAML file.
        """
        data: dict[str, Any] = yaml.safe_load(
            Path(assets_yaml_path).read_text(encoding="utf-8")
        )
        assets: list[dict[str, Any]] = data["assets"]
        pair_defaults: dict[str, Any] = data.get("pair_defaults", {})
        tick_size: float = pair_defaults.get("tick_size", 0.01)
        lot_size: float = pair_defaults.get("lot_size", 0.0001)

        # Insert instruments
        for asset in assets:
            conn.execute(
                """
                INSERT INTO instrument
                    (symbol, name, asset_class, decimals, total_supply,
                     peg_target, is_quote_asset, yfinance_ticker,
                     binance_symbol, default_price, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    asset["symbol"],
                    asset["name"],
                    asset["asset_class"],
                    asset.get("decimals", 8),
                    asset.get("total_supply"),
                    asset.get("peg_target"),
                    asset.get("is_quote_asset", 0),
                    asset.get("yfinance_ticker"),
                    asset.get("binance_symbol"),
                    asset.get("default_price"),
                    asset.get("metadata_json"),
                ),
            )

        # Identify quote asset(s) and non-quote assets
        quote_rows = conn.execute(
            "SELECT instrument_id, symbol FROM instrument WHERE is_quote_asset = 1"
        ).fetchall()
        non_quote_rows = conn.execute(
            "SELECT instrument_id, symbol FROM instrument WHERE is_quote_asset = 0"
        ).fetchall()

        # Create pairs: every non-quote asset paired with every quote asset
        for base_id, _base_sym in non_quote_rows:
            for quote_id, _quote_sym in quote_rows:
                # Look up the default_price for the base to seed last_price
                row = conn.execute(
                    "SELECT default_price FROM instrument WHERE instrument_id = ?",
                    (base_id,),
                ).fetchone()
                default_price = row[0] if row else None

                conn.execute(
                    """
                    INSERT INTO pair
                        (base_instrument_id, quote_instrument_id,
                         tick_size, lot_size, last_price)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (base_id, quote_id, tick_size, lot_size, default_price),
                )

        conn.commit()
