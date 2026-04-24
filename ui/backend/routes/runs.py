"""Run lifecycle routes.

POST   /api/runs                         -- start a run
GET    /api/runs                         -- list all runs
GET    /api/runs/{run_id}                -- run details
DELETE /api/runs/{run_id}                -- stop a run
GET    /api/runs/{run_id}/parquet/{sec}  -- telemetry section as JSON
GET    /api/runs/{run_id}/config         -- config.yaml
GET    /api/runs/{run_id}/initial_prices -- initial_prices.json
GET    /api/runs/{run_id}/graph          -- social graph {nodes, edges}
POST   /api/runs/{run_id}/inject-news    -- god-mode news injection
GET    /api/runs/{run_id}/god-mode-events -- list injected god-mode events
"""

from __future__ import annotations

import json
import logging
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

import yaml
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from ui.backend.services.run_manager import RunManager

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/runs", tags=["runs"])

VALID_SECTIONS = {
    "prices", "trades", "posts", "actions",
    "stimuli", "tiers", "news", "conservation", "orders",
}


class StartRunRequest(BaseModel):
    scenario_name: str
    seeds: list[int] = Field(default_factory=lambda: [42])
    no_llm: bool = False


@router.post("")
async def start_run(req: StartRunRequest):
    """Start a simulation run."""
    mgr = RunManager.get()

    # Locate scenario
    from ui.backend.routes.scenarios import _find_scenario_path
    path = _find_scenario_path(req.scenario_name)
    if path is None:
        raise HTTPException(404, detail=f"Scenario '{req.scenario_name}' not found")

    seed = req.seeds[0] if req.seeds else 42
    run_id, pid, output_dir = mgr.start_run(
        scenario_path=str(path),
        seed=seed,
        no_llm=req.no_llm,
    )
    return {"run_id": run_id, "pid": pid, "output_dir": output_dir}


@router.get("")
async def list_runs():
    """List all runs with status."""
    mgr = RunManager.get()
    return [r.to_dict() for r in mgr.list_runs()]


@router.get("/{run_id}")
async def get_run(run_id: str):
    """Get detailed run info."""
    mgr = RunManager.get()
    info = mgr.get_run(run_id)
    if info is None:
        raise HTTPException(404, detail=f"Run '{run_id}' not found")
    return info.to_dict()


@router.delete("/{run_id}")
async def stop_run(run_id: str):
    """Stop a running simulation."""
    mgr = RunManager.get()
    stopped = mgr.stop_run(run_id)
    if not stopped:
        raise HTTPException(404, detail=f"Run '{run_id}' not found or not running")
    return {"stopped": run_id}


@router.get("/{run_id}/parquet/{section}")
async def read_parquet(run_id: str, section: str):
    """Read a parquet telemetry section as JSON."""
    if section not in VALID_SECTIONS:
        raise HTTPException(400, detail=f"Invalid section '{section}'. Valid: {sorted(VALID_SECTIONS)}")

    mgr = RunManager.get()
    info = mgr.get_run(run_id)
    if info is None:
        raise HTTPException(404, detail=f"Run '{run_id}' not found")

    rows = mgr.read_parquet_section(run_id, section)
    return {"run_id": run_id, "section": section, "count": len(rows), "rows": rows}


@router.get("/{run_id}/config")
async def get_run_config(run_id: str):
    """Return the config.yaml for a run."""
    mgr = RunManager.get()
    info = mgr.get_run(run_id)
    if info is None:
        raise HTTPException(404, detail=f"Run '{run_id}' not found")

    config_path = Path(info.output_dir) / "config.yaml"
    if not config_path.exists():
        raise HTTPException(404, detail="config.yaml not found for this run")

    try:
        return yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise HTTPException(500, detail=str(exc))


@router.get("/{run_id}/initial_prices")
async def get_initial_prices(run_id: str):
    """Return initial_prices.json for a run."""
    mgr = RunManager.get()
    info = mgr.get_run(run_id)
    if info is None:
        raise HTTPException(404, detail=f"Run '{run_id}' not found")

    prices_path = Path(info.output_dir) / "initial_prices.json"
    if not prices_path.exists():
        raise HTTPException(404, detail="initial_prices.json not found for this run")

    try:
        return json.loads(prices_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise HTTPException(500, detail=str(exc))


MAX_GRAPH_NODES = 2000


@router.get("/{run_id}/graph")
async def get_run_graph(run_id: str):
    """Return {nodes, edges} for the run's persisted social graph.

    nodes: [{user_id, persona_id, archetype, name, follower_count}]
    edges: [{source: user_id, target: user_id}]
    """
    mgr = RunManager.get()
    info = mgr.get_run(run_id)
    if info is None:
        raise HTTPException(404, detail=f"Run '{run_id}' not found")

    db_path = Path(info.output_dir) / "simulation.db"
    if not db_path.exists():
        raise HTTPException(404, detail="run not initialized")

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row

        # Fetch edges
        edges_raw = conn.execute(
            "SELECT follower_id, followee_id FROM follow"
        ).fetchall()
        edges = [
            {"source": row["follower_id"], "target": row["followee_id"]}
            for row in edges_raw
        ]

        # Compute follower counts
        follower_counts: Counter[int] = Counter()
        user_ids_in_edges: set[int] = set()
        for e in edges:
            follower_counts[e["target"]] += 1
            user_ids_in_edges.add(e["source"])
            user_ids_in_edges.add(e["target"])

        # Fetch user -> persona -> archetype mapping
        node_rows = conn.execute(
            """
            SELECT u.user_id, ap.persona_id, p.archetype, p.name
            FROM user u
            JOIN agent_persona ap ON ap.user_id = u.user_id
            JOIN persona p ON p.persona_id = ap.persona_id
            """
        ).fetchall()
        conn.close()

        nodes = []
        for row in node_rows:
            uid = row["user_id"]
            nodes.append({
                "user_id": uid,
                "persona_id": row["persona_id"],
                "archetype": row["archetype"] or "lurker",
                "name": row["name"] or f"agent_{uid}",
                "follower_count": follower_counts.get(uid, 0),
            })

        # Cap at MAX_GRAPH_NODES, keeping highest-follower nodes
        if len(nodes) > MAX_GRAPH_NODES:
            nodes.sort(key=lambda n: n["follower_count"], reverse=True)
            nodes = nodes[:MAX_GRAPH_NODES]
            kept_ids = {n["user_id"] for n in nodes}
            edges = [
                e for e in edges
                if e["source"] in kept_ids and e["target"] in kept_ids
            ]

        return {"nodes": nodes, "edges": edges}

    except sqlite3.OperationalError as exc:
        raise HTTPException(500, detail=f"Database error: {exc}")


# ---------------------------------------------------------------------------
# God-mode news injection
# ---------------------------------------------------------------------------


class NewsInjection(BaseModel):
    """Request body for injecting a news event into a running simulation."""

    title: str
    content: str
    sentiment_valence: float = Field(ge=-1.0, le=1.0)
    affected_assets: list[str] = Field(default_factory=list)
    audience: Literal[
        "all", "news_traders", "kols", "crypto_natives", "whales"
    ] = "all"
    magnitude: Literal["minor", "moderate", "major", "critical"] = "moderate"
    credibility: Literal["rumor", "reported", "confirmed"] = "reported"


def _get_simulation_db(run_id: str) -> tuple[sqlite3.Connection, Path]:
    """Look up a run's simulation.db, returning (conn, db_path).

    Raises HTTPException(404) if the run or DB file doesn't exist.
    """
    mgr = RunManager.get()
    info = mgr.get_run(run_id)
    if info is None:
        raise HTTPException(404, detail=f"Run '{run_id}' not found")

    db_path = Path(info.output_dir) / "simulation.db"
    if not db_path.exists():
        raise HTTPException(
            404,
            detail=f"simulation.db not found for run '{run_id}' "
            "(simulation may not have started yet)",
        )

    try:
        conn = sqlite3.connect(str(db_path))
        return conn, db_path
    except sqlite3.Error as exc:
        raise HTTPException(500, detail=f"Cannot open simulation.db: {exc}")


def _compute_next_step(conn: sqlite3.Connection) -> int:
    """Estimate the next upcoming simulation step.

    Strategy: MAX(step) + 1 from the actions table (written each tick).
    Falls back to MAX(step) + 1 from news_event, then 1.
    """
    row = conn.execute(
        "SELECT MAX(step) FROM action"
    ).fetchone()
    if row and row[0] is not None:
        return int(row[0]) + 1

    # Fallback: check news_event table
    row = conn.execute(
        "SELECT MAX(step) FROM news_event"
    ).fetchone()
    if row and row[0] is not None:
        return int(row[0]) + 1

    return 1


@router.get("/{run_id}/orderbook")
def get_orderbook(run_id: str, pair: str = "BTC/USD", depth: int = 10) -> dict:
    """Return top-N bids and asks for a pair from the run's simulation.db.

    For completed runs this reflects the final resting book. For running
    runs it's a live snapshot. Used by the OrderBook panel in both
    replay and live views.

    Response: {
      pair: "BTC/USD",
      last_price: float,
      bids: [{price, size, count}],   # descending price
      asks: [{price, size, count}],   # ascending price
      spread: float,
    }
    Aggregates same-price orders into levels.
    """
    mgr = RunManager.get()
    info = mgr.get_run(run_id)
    if info is None:
        raise HTTPException(404, detail=f"Run '{run_id}' not found")

    db_path = Path(info.output_dir) / "simulation.db"
    if not db_path.exists():
        raise HTTPException(404, detail="run not initialized")

    base_sym, _, quote_sym = pair.partition("/")
    if not base_sym or not quote_sym:
        raise HTTPException(400, detail=f"bad pair format: {pair!r}")

    try:
        conn = sqlite3.connect(str(db_path))
        pair_row = conn.execute(
            """
            SELECT p.pair_id, p.last_price
            FROM pair p
            JOIN instrument bi ON bi.instrument_id = p.base_instrument_id
            JOIN instrument qi ON qi.instrument_id = p.quote_instrument_id
            WHERE bi.symbol = ? AND qi.symbol = ?
            """,
            (base_sym, quote_sym),
        ).fetchone()
        if pair_row is None:
            raise HTTPException(404, detail=f"pair {pair!r} not found")
        pair_id, last_price = pair_row

        # Aggregate remaining (unfilled) volume per price level for open orders.
        bid_rows = conn.execute(
            """
            SELECT price,
                   ROUND(SUM(quantity - filled_quantity), 8) AS size,
                   COUNT(*) AS count
            FROM crypto_order
            WHERE pair_id = ? AND side = 'buy' AND status = 'open'
            GROUP BY price
            ORDER BY price DESC
            LIMIT ?
            """,
            (pair_id, depth),
        ).fetchall()
        ask_rows = conn.execute(
            """
            SELECT price,
                   ROUND(SUM(quantity - filled_quantity), 8) AS size,
                   COUNT(*) AS count
            FROM crypto_order
            WHERE pair_id = ? AND side = 'sell' AND status = 'open'
            GROUP BY price
            ORDER BY price ASC
            LIMIT ?
            """,
            (pair_id, depth),
        ).fetchall()

        bids = [{"price": r[0], "size": r[1], "count": r[2]} for r in bid_rows]
        asks = [{"price": r[0], "size": r[1], "count": r[2]} for r in ask_rows]
        spread = (asks[0]["price"] - bids[0]["price"]) if (bids and asks) else None

        return {
            "pair": pair,
            "last_price": last_price,
            "bids": bids,
            "asks": asks,
            "spread": spread,
        }
    finally:
        conn.close()


@router.post("/{run_id}/inject-news")
def inject_news(run_id: str, body: NewsInjection) -> dict:
    """Insert a news_event row into the running simulation's DB.

    The event fires at the next upcoming step. Returns the inserted row
    metadata including the target step.
    """
    conn, db_path = _get_simulation_db(run_id)
    try:
        next_step = _compute_next_step(conn)
        affected_str = ",".join(body.affected_assets)
        now = datetime.now(timezone.utc).isoformat()

        cursor = conn.execute(
            "INSERT INTO news_event "
            "(step, source, audience, content, title, sentiment_valence, "
            " magnitude, credibility, affected_instruments, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                next_step,
                "god_mode",
                body.audience,
                body.content,
                body.title,
                body.sentiment_valence,
                body.magnitude,
                body.credibility,
                affected_str,
                now,
            ),
        )
        conn.commit()
        event_id = cursor.lastrowid

        return {
            "step": next_step,
            "event_id": event_id,
            "source": "god_mode",
            "title": body.title,
            "audience": body.audience,
            "magnitude": body.magnitude,
            "sentiment_valence": body.sentiment_valence,
        }
    except sqlite3.Error as exc:
        raise HTTPException(500, detail=f"Database error: {exc}")
    finally:
        conn.close()


@router.get("/{run_id}/god-mode-events")
def list_god_mode_events(run_id: str) -> list[dict]:
    """Return news_event rows where source='god_mode', newest first, limit 20."""
    conn, db_path = _get_simulation_db(run_id)
    try:
        rows = conn.execute(
            "SELECT event_id, step, source, audience, content, title, "
            "sentiment_valence, magnitude, credibility, "
            "affected_instruments, created_at "
            "FROM news_event WHERE source = 'god_mode' "
            "ORDER BY event_id DESC LIMIT 20"
        ).fetchall()

        return [
            {
                "event_id": r[0],
                "step": r[1],
                "source": r[2],
                "audience": r[3],
                "content": r[4],
                "title": r[5],
                "sentiment_valence": r[6],
                "magnitude": r[7],
                "credibility": r[8],
                "affected_instruments": r[9],
                "created_at": r[10],
            }
            for r in rows
        ]
    except sqlite3.Error as exc:
        raise HTTPException(500, detail=f"Database error: {exc}")
    finally:
        conn.close()
