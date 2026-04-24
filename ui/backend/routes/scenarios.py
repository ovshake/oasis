"""Scenario CRUD routes.

GET  /api/scenarios              -- list from scenarios/ + calibration/
GET  /api/scenarios/{name}       -- parsed Scenario JSON
POST /api/scenarios              -- create/update (write YAML)
DELETE /api/scenarios/{name}     -- delete YAML file
"""

from __future__ import annotations

import logging
from pathlib import Path

import yaml
from fastapi import APIRouter, HTTPException

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/scenarios", tags=["scenarios"])

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SCENARIO_DIRS = [
    PROJECT_ROOT / "scenarios",
    PROJECT_ROOT / "calibration",
]


def _find_all_scenarios() -> list[dict]:
    """Scan scenario directories and return metadata dicts."""
    results = []
    for d in SCENARIO_DIRS:
        if not d.exists():
            continue
        for p in sorted(d.glob("*.yaml")):
            try:
                raw = yaml.safe_load(p.read_text(encoding="utf-8"))
                results.append({
                    "name": raw.get("name", p.stem),
                    "path": str(p),
                    "duration_steps": raw.get("duration_steps"),
                    "agents_count": raw.get("agents_count"),
                    "llm_enabled": raw.get("llm_enabled", True),
                    "source_dir": d.name,
                })
            except Exception as exc:
                logger.warning("Failed to parse %s: %s", p, exc)
    return results


def _find_scenario_path(name: str) -> Path | None:
    """Locate a scenario YAML by name (stem match)."""
    for d in SCENARIO_DIRS:
        candidate = d / f"{name}.yaml"
        if candidate.exists():
            return candidate
    return None


@router.get("")
async def list_scenarios():
    """List all available scenarios."""
    return _find_all_scenarios()


@router.get("/{name}")
async def get_scenario(name: str):
    """Return a parsed scenario as JSON."""
    path = _find_scenario_path(name)
    if path is None:
        raise HTTPException(404, detail=f"Scenario '{name}' not found")
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        return raw
    except Exception as exc:
        raise HTTPException(500, detail=str(exc))


@router.post("")
async def create_or_update_scenario(body: dict):
    """Create or update a scenario YAML file."""
    name = body.get("name")
    if not name:
        raise HTTPException(400, detail="Missing 'name' in body")

    # Sanitize name
    safe_name = "".join(c for c in name if c.isalnum() or c in "_-").lower()
    if not safe_name:
        raise HTTPException(400, detail="Invalid scenario name")

    target_dir = SCENARIO_DIRS[0]  # scenarios/
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / f"{safe_name}.yaml"

    try:
        content = yaml.dump(body, default_flow_style=False, sort_keys=False)
        target.write_text(content, encoding="utf-8")
    except Exception as exc:
        raise HTTPException(500, detail=str(exc))

    return {"name": safe_name, "path": str(target), "created": not target.exists()}


@router.delete("/{name}")
async def delete_scenario(name: str):
    """Delete a scenario YAML file."""
    path = _find_scenario_path(name)
    if path is None:
        raise HTTPException(404, detail=f"Scenario '{name}' not found")
    try:
        path.unlink()
    except Exception as exc:
        raise HTTPException(500, detail=str(exc))
    return {"deleted": name}
