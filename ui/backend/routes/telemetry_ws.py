"""WebSocket telemetry streaming.

WS /ws/runs/{run_id}  -- push step-by-step summaries as JSON

Protocol (server -> client):
    {"type": "step",     "step": N, "data": {...per-step summary...}}
    {"type": "complete", "total_steps": N, "elapsed_ms": M}
    {"type": "error",    "message": "..."}

Client -> server (optional):
    {"type": "ping"}
"""

from __future__ import annotations

import asyncio
import json
import logging
import time

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from ui.backend.services.run_manager import RunManager

logger = logging.getLogger(__name__)

router = APIRouter(tags=["websocket"])

POLL_INTERVAL_S = 1.0


@router.websocket("/ws/runs/{run_id}")
async def telemetry_ws(ws: WebSocket, run_id: str):
    """Stream telemetry for a run via WebSocket.

    Polls actions.parquet every ~1s and pushes new rows grouped by step.
    """
    await ws.accept()

    mgr = RunManager.get()
    info = mgr.get_run(run_id)
    if info is None:
        await ws.send_json({"type": "error", "message": f"Run '{run_id}' not found"})
        await ws.close()
        return

    last_step = -1
    start_ms = time.monotonic() * 1000

    try:
        while True:
            # Check for incoming ping (non-blocking)
            try:
                data = await asyncio.wait_for(ws.receive_text(), timeout=0.05)
                msg = json.loads(data)
                if msg.get("type") == "ping":
                    await ws.send_json({"type": "pong"})
            except asyncio.TimeoutError:
                pass
            except (WebSocketDisconnect, RuntimeError):
                break

            # Poll for new telemetry rows
            rows = mgr.poll_telemetry(run_id, since_step=last_step)
            if rows:
                # Group by step and send one message per step
                steps: dict[int, list[dict]] = {}
                for row in rows:
                    s = row.get("step", 0)
                    steps.setdefault(s, []).append(row)

                for step_num in sorted(steps):
                    step_rows = steps[step_num]
                    summary = _summarize_step(step_num, step_rows)
                    await ws.send_json({
                        "type": "step",
                        "step": step_num,
                        "data": summary,
                    })
                    last_step = max(last_step, step_num)

            # Check if run is done
            info = mgr.get_run(run_id)
            if info is None or info.status in ("completed", "failed"):
                elapsed = time.monotonic() * 1000 - start_ms
                if info and info.status == "completed":
                    await ws.send_json({
                        "type": "complete",
                        "total_steps": last_step + 1,
                        "elapsed_ms": int(elapsed),
                    })
                elif info and info.status == "failed":
                    await ws.send_json({
                        "type": "error",
                        "message": info.error or "run failed",
                    })
                break

            await asyncio.sleep(POLL_INTERVAL_S)

    except WebSocketDisconnect:
        logger.debug("WebSocket disconnected for run %s", run_id)
    except Exception as exc:
        logger.exception("WebSocket error for run %s: %s", run_id, exc)
        try:
            await ws.send_json({"type": "error", "message": str(exc)})
        except Exception:
            pass
    finally:
        try:
            await ws.close()
        except Exception:
            pass


def _summarize_step(step: int, rows: list[dict]) -> dict:
    """Summarize action rows for a single step.

    Returns a dict with counts by tier and archetype breakdown.
    """
    tier_counts: dict[str, int] = {}
    archetype_counts: dict[str, int] = {}
    action_types: dict[str, int] = {}

    for row in rows:
        tier = row.get("tier", "unknown")
        arch = row.get("archetype", "unknown")
        act = row.get("action_type", "unknown")
        tier_counts[tier] = tier_counts.get(tier, 0) + 1
        archetype_counts[arch] = archetype_counts.get(arch, 0) + 1
        action_types[act] = action_types.get(act, 0) + 1

    return {
        "total_actions": len(rows),
        "tier_counts": tier_counts,
        "archetype_counts": archetype_counts,
        "action_types": action_types,
    }
