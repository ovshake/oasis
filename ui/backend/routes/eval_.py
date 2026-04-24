"""Eval report routes.

POST /api/runs/{run_id}/eval  -- trigger eval generation (background)
GET  /api/runs/{run_id}/eval  -- return eval_report.json if ready
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field

from ui.backend.services.run_manager import RunManager, PYTHON, PROJECT_ROOT

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/runs", tags=["eval"])

EVALUATE_SCRIPT = str(PROJECT_ROOT / "scripts" / "evaluate.py")

# Track in-progress eval jobs (run_id -> True while running)
_eval_in_progress: dict[str, bool] = {}


class EvalRequest(BaseModel):
    mode: str = "sanity"
    gt_start: str | None = None
    gt_end: str | None = None
    assets: list[str] = Field(default_factory=lambda: ["BTC", "ETH"])


def _run_eval(run_id: str, output_dir: str, req: EvalRequest) -> None:
    """Execute evaluate.py in a subprocess (runs in background task)."""
    cmd = [
        PYTHON,
        EVALUATE_SCRIPT,
        output_dir,
        "--mode", req.mode,
        "--assets", *req.assets,
    ]
    if req.gt_start:
        cmd.extend(["--gt-start", req.gt_start])
    if req.gt_end:
        cmd.extend(["--gt-end", req.gt_end])

    try:
        subprocess.run(
            cmd,
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            timeout=300,
        )
    except Exception as exc:
        logger.error("Eval failed for run %s: %s", run_id, exc)
    finally:
        _eval_in_progress.pop(run_id, None)


@router.post("/{run_id}/eval")
async def trigger_eval(run_id: str, req: EvalRequest, bg: BackgroundTasks):
    """Trigger eval report generation in the background."""
    mgr = RunManager.get()
    info = mgr.get_run(run_id)
    if info is None:
        raise HTTPException(404, detail=f"Run '{run_id}' not found")

    if run_id in _eval_in_progress:
        return {"run_id": run_id, "status": "already_running"}

    _eval_in_progress[run_id] = True
    bg.add_task(_run_eval, run_id, info.output_dir, req)
    return {"run_id": run_id, "status": "started"}


@router.get("/{run_id}/eval")
async def get_eval(run_id: str):
    """Return eval_report.json if it has been generated."""
    mgr = RunManager.get()
    info = mgr.get_run(run_id)
    if info is None:
        raise HTTPException(404, detail=f"Run '{run_id}' not found")

    report_path = Path(info.output_dir) / "eval_report.json"
    if not report_path.exists():
        if run_id in _eval_in_progress:
            raise HTTPException(202, detail="Eval in progress")
        raise HTTPException(404, detail="eval_report.json not yet generated")

    try:
        return json.loads(report_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise HTTPException(500, detail=str(exc))
