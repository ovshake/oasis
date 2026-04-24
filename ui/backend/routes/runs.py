"""Run lifecycle routes.

POST   /api/runs                         -- start a run
GET    /api/runs                         -- list all runs
GET    /api/runs/{run_id}                -- run details
DELETE /api/runs/{run_id}                -- stop a run
GET    /api/runs/{run_id}/parquet/{sec}  -- telemetry section as JSON
GET    /api/runs/{run_id}/config         -- config.yaml
GET    /api/runs/{run_id}/initial_prices -- initial_prices.json
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

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
