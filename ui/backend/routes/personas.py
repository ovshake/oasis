"""Persona library browse routes.

GET /api/personas                  -- paginated list (filter by archetype)
GET /api/personas/distribution     -- count per archetype
GET /api/personas/{persona_id}     -- single persona by ID
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/personas", tags=["personas"])

PROJECT_ROOT = Path(__file__).resolve().parents[3]
LIBRARY_PATH = PROJECT_ROOT / "data" / "personas" / "library.jsonl"
SMOKE_LIBRARY_PATH = PROJECT_ROOT / "data" / "personas" / "library_smoke_100.jsonl"

# Cached persona data (loaded once)
_personas: list[dict] | None = None


def _load_personas() -> list[dict]:
    """Load the persona library from JSONL. Uses smoke library as fallback."""
    global _personas
    if _personas is not None:
        return _personas

    path = LIBRARY_PATH if LIBRARY_PATH.exists() else SMOKE_LIBRARY_PATH
    if not path.exists():
        logger.warning("No persona library found at %s or %s", LIBRARY_PATH, SMOKE_LIBRARY_PATH)
        _personas = []
        return _personas

    personas = []
    for line in path.read_text(encoding="utf-8").strip().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            personas.append(json.loads(line))
        except json.JSONDecodeError as exc:
            logger.warning("Bad JSONL line: %s", exc)
    _personas = personas
    return _personas


def _invalidate_cache() -> None:
    """Clear cached data (for testing)."""
    global _personas
    _personas = None


@router.get("/distribution")
async def persona_distribution():
    """Return count per archetype."""
    personas = _load_personas()
    counts: dict[str, int] = {}
    for p in personas:
        arch = p.get("archetype", "unknown")
        counts[arch] = counts.get(arch, 0) + 1
    return {"total": len(personas), "distribution": counts}


@router.get("/{persona_id}")
async def get_persona(persona_id: str):
    """Return a single persona by ID."""
    personas = _load_personas()
    for p in personas:
        if p.get("persona_id") == persona_id:
            return p
    raise HTTPException(404, detail=f"Persona '{persona_id}' not found")


@router.get("")
async def list_personas(
    archetype: str | None = Query(None, description="Filter by archetype"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """Paginated list of personas, optionally filtered by archetype."""
    personas = _load_personas()

    if archetype:
        personas = [p for p in personas if p.get("archetype") == archetype]

    total = len(personas)
    page = personas[offset : offset + limit]
    return {"total": total, "offset": offset, "limit": limit, "personas": page}
