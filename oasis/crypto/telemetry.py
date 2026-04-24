"""Per-step snapshot collector -> parquet.

Phase 8 deliverable. Buffers rows in memory; flush writes to parquet at end.
Uses pyarrow directly (not pandas) for efficient columnar writes.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Arrow schemas for each parquet file
# ---------------------------------------------------------------------------

_SCHEMAS: dict[str, pa.Schema] = {
    "prices": pa.schema([
        ("step", pa.int32()),
        ("pair_id", pa.int32()),
        ("base_symbol", pa.string()),
        ("quote_symbol", pa.string()),
        ("last_price", pa.float64()),
        ("prev_close_price", pa.float64()),
        ("volume_step", pa.float64()),
    ]),
    "trades": pa.schema([
        ("trade_id", pa.int32()),
        ("step", pa.int32()),
        ("pair_id", pa.int32()),
        ("price", pa.float64()),
        ("qty", pa.float64()),
        ("buyer_id", pa.int32()),
        ("seller_id", pa.int32()),
        ("side", pa.string()),  # aggressive side: "buy" or "sell"
    ]),
    "orders": pa.schema([
        ("order_id", pa.int32()),
        ("step", pa.int32()),
        ("user_id", pa.int32()),
        ("pair_id", pa.int32()),
        ("side", pa.string()),
        ("price", pa.float64()),
        ("qty", pa.float64()),
        ("filled_qty", pa.float64()),
        ("status", pa.string()),
    ]),
    "posts": pa.schema([
        ("post_id", pa.int32()),
        ("step", pa.int32()),
        ("author_user_id", pa.int32()),
        ("content", pa.string()),
        ("sentiment", pa.float64()),
    ]),
    "actions": pa.schema([
        ("step", pa.int32()),
        ("user_id", pa.int32()),
        ("archetype", pa.string()),
        ("tier", pa.string()),
        ("action_type", pa.string()),
    ]),
    "stimuli": pa.schema([
        ("step", pa.int32()),
        ("user_id", pa.int32()),
        ("price_stimulus", pa.float64()),
        ("news_stimulus", pa.float64()),
        ("total_stimulus", pa.float64()),
    ]),
    "tiers": pa.schema([
        ("step", pa.int32()),
        ("tier", pa.string()),
        ("count", pa.int32()),
    ]),
    "news": pa.schema([
        ("step", pa.int32()),
        ("source", pa.string()),
        ("title", pa.string()),
        ("sentiment_valence", pa.float64()),
        ("audience", pa.string()),
        ("affected_assets", pa.string()),
    ]),
    "conservation": pa.schema([
        ("step", pa.int32()),
        ("instrument", pa.string()),
        ("total_amount", pa.float64()),
        ("total_locked", pa.float64()),
        ("total_supply", pa.float64()),
    ]),
}


class Telemetry:
    """Per-step snapshot collector. Buffers rows in memory; flush to parquet."""

    def __init__(self, output_dir: Path, buffer_size: int = 5000) -> None:
        self.output_dir = Path(output_dir)
        self.buffer_size = buffer_size
        self._buffers: dict[str, list[dict[str, Any]]] = {
            name: [] for name in _SCHEMAS
        }
        self._flushed: dict[str, bool] = {name: False for name in _SCHEMAS}

    # ---- Generic record ----

    def record_step(self, step: int, **sections: list[dict]) -> None:
        """Record multiple sections at once. Key names must match buffer names."""
        for name, rows in sections.items():
            if name not in self._buffers:
                logger.warning("Unknown telemetry section: %s", name)
                continue
            for row in rows:
                row.setdefault("step", step)
            self._buffers[name].extend(rows)
        self._maybe_auto_flush()

    # ---- Per-section convenience ----

    def record_prices(self, step: int, rows: list[dict]) -> None:
        for r in rows:
            r.setdefault("step", step)
        self._buffers["prices"].extend(rows)
        self._maybe_auto_flush()

    def record_trades(self, step: int, rows: list[dict]) -> None:
        for r in rows:
            r.setdefault("step", step)
        self._buffers["trades"].extend(rows)
        self._maybe_auto_flush()

    def record_orders(self, step: int, rows: list[dict]) -> None:
        for r in rows:
            r.setdefault("step", step)
        self._buffers["orders"].extend(rows)
        self._maybe_auto_flush()

    def record_posts(self, step: int, rows: list[dict]) -> None:
        for r in rows:
            r.setdefault("step", step)
        self._buffers["posts"].extend(rows)
        self._maybe_auto_flush()

    def record_actions(self, step: int, rows: list[dict]) -> None:
        for r in rows:
            r.setdefault("step", step)
        self._buffers["actions"].extend(rows)
        self._maybe_auto_flush()

    def record_stimuli(self, step: int, rows: list[dict]) -> None:
        for r in rows:
            r.setdefault("step", step)
        self._buffers["stimuli"].extend(rows)
        self._maybe_auto_flush()

    def record_tiers(self, step: int, rows: list[dict]) -> None:
        for r in rows:
            r.setdefault("step", step)
        self._buffers["tiers"].extend(rows)
        self._maybe_auto_flush()

    def record_news(self, step: int, rows: list[dict]) -> None:
        for r in rows:
            r.setdefault("step", step)
        self._buffers["news"].extend(rows)
        self._maybe_auto_flush()

    def record_conservation(self, step: int, rows: list[dict]) -> None:
        for r in rows:
            r.setdefault("step", step)
        self._buffers["conservation"].extend(rows)
        self._maybe_auto_flush()

    # ---- Flush ----

    def flush(self) -> None:
        """Write all buffered rows to parquet. Safe to call multiple times."""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        for name, schema in _SCHEMAS.items():
            rows = self._buffers[name]
            if not rows:
                continue
            table = _rows_to_table(rows, schema)
            out_path = self.output_dir / f"{name}.parquet"
            if self._flushed[name] and out_path.exists():
                # Append by reading existing and concatenating
                existing = pq.read_table(out_path, schema=schema)
                table = pa.concat_tables([existing, table])
            pq.write_table(table, out_path)
            self._flushed[name] = True
            self._buffers[name] = []

    # ---- Internals ----

    def _maybe_auto_flush(self) -> None:
        """Auto-flush when any buffer exceeds buffer_size."""
        for name, rows in self._buffers.items():
            if len(rows) >= self.buffer_size:
                self.flush()
                return


def _rows_to_table(rows: list[dict], schema: pa.Schema) -> pa.Table:
    """Convert list-of-dicts to a pyarrow Table with the given schema.

    Missing fields get None; extra fields are dropped.
    """
    columns: dict[str, list] = {field.name: [] for field in schema}
    for row in rows:
        for field in schema:
            columns[field.name].append(row.get(field.name))
    arrays = []
    for field in schema:
        arrays.append(pa.array(columns[field.name], type=field.type))
    return pa.table(arrays, schema=schema)
