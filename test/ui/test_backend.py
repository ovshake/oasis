"""Tests for the OASIS Crypto Sim FastAPI backend.

Run with::

    ~/venvs/aragen/bin/python -m pytest test/ui/test_backend.py -v
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

# Ensure project root is on sys.path
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from ui.backend.main import app
from ui.backend.services.run_manager import RunManager

client = TestClient(app)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_run_manager():
    """Reset RunManager singleton between tests."""
    RunManager._instance = None
    yield
    RunManager._instance = None


@pytest.fixture
def tmp_output_dir():
    """Create a temporary output directory with fixture parquet files."""
    with tempfile.TemporaryDirectory() as d:
        _write_fixture_parquets(Path(d))
        yield d


def _write_fixture_parquets(out: Path):
    """Write minimal parquet files for testing."""
    # actions.parquet
    actions_schema = pa.schema([
        ("step", pa.int32()),
        ("user_id", pa.int32()),
        ("archetype", pa.string()),
        ("tier", pa.string()),
        ("action_type", pa.string()),
    ])
    actions_table = pa.table({
        "step": pa.array([0, 0, 1, 1, 2], type=pa.int32()),
        "user_id": pa.array([1, 2, 1, 3, 2], type=pa.int32()),
        "archetype": pa.array(["hodler", "fomo_degen", "hodler", "lurker", "fomo_degen"], type=pa.string()),
        "tier": pa.array(["trade", "post", "silent", "react", "trade"], type=pa.string()),
        "action_type": pa.array(["buy", "post", "silent", "like", "sell"], type=pa.string()),
    }, schema=actions_schema)
    pq.write_table(actions_table, str(out / "actions.parquet"))

    # prices.parquet
    prices_schema = pa.schema([
        ("step", pa.int32()),
        ("pair_id", pa.int32()),
        ("base_symbol", pa.string()),
        ("quote_symbol", pa.string()),
        ("last_price", pa.float64()),
        ("prev_close_price", pa.float64()),
        ("volume_step", pa.float64()),
    ])
    prices_table = pa.table({
        "step": pa.array([0, 1, 2], type=pa.int32()),
        "pair_id": pa.array([1, 1, 1], type=pa.int32()),
        "base_symbol": pa.array(["BTC", "BTC", "BTC"], type=pa.string()),
        "quote_symbol": pa.array(["USD", "USD", "USD"], type=pa.string()),
        "last_price": pa.array([60000.0, 60100.0, 59900.0], type=pa.float64()),
        "prev_close_price": pa.array([59800.0, 60000.0, 60100.0], type=pa.float64()),
        "volume_step": pa.array([10.5, 8.2, 15.1], type=pa.float64()),
    }, schema=prices_schema)
    pq.write_table(prices_table, str(out / "prices.parquet"))

    # trades.parquet
    trades_schema = pa.schema([
        ("trade_id", pa.int32()),
        ("step", pa.int32()),
        ("pair_id", pa.int32()),
        ("price", pa.float64()),
        ("qty", pa.float64()),
        ("buyer_id", pa.int32()),
        ("seller_id", pa.int32()),
    ])
    trades_table = pa.table({
        "trade_id": pa.array([1, 2], type=pa.int32()),
        "step": pa.array([0, 2], type=pa.int32()),
        "pair_id": pa.array([1, 1], type=pa.int32()),
        "price": pa.array([60000.0, 59900.0], type=pa.float64()),
        "qty": pa.array([0.5, 1.2], type=pa.float64()),
        "buyer_id": pa.array([1, 2], type=pa.int32()),
        "seller_id": pa.array([2, 1], type=pa.int32()),
    }, schema=trades_schema)
    pq.write_table(trades_table, str(out / "trades.parquet"))


@pytest.fixture
def mock_run(tmp_output_dir):
    """Set up a mock run in the RunManager."""
    from ui.backend.services.run_manager import RunInfo

    mgr = RunManager.get()
    info = RunInfo(
        run_id="test123",
        scenario_name="quiet_market",
        scenario_path="scenarios/quiet_market.yaml",
        seed=42,
        no_llm=True,
        pid=99999,
        output_dir=tmp_output_dir,
        status="running",
        start_time="2026-04-23T00:00:00+00:00",
    )
    mgr._runs["test123"] = info
    return info


# ---------------------------------------------------------------------------
# 1. Health check
# ---------------------------------------------------------------------------


def test_health():
    """GET /health returns {status: ok}."""
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


# ---------------------------------------------------------------------------
# 2. Scenarios: list
# ---------------------------------------------------------------------------


def test_list_scenarios():
    """GET /api/scenarios lists the committed scenario fixtures."""
    resp = client.get("/api/scenarios")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    names = {s["name"] for s in data}
    # We have at least quiet_market, fed_hawkish, kol_pump, live_today
    assert "quiet_market" in names
    assert "fed_hawkish" in names
    assert len(data) >= 4


# ---------------------------------------------------------------------------
# 3. Scenarios: get by name
# ---------------------------------------------------------------------------


def test_get_scenario_quiet_market():
    """GET /api/scenarios/quiet_market returns valid Scenario JSON."""
    resp = client.get("/api/scenarios/quiet_market")
    assert resp.status_code == 200
    data = resp.json()
    assert data["name"] == "quiet_market"
    assert "duration_steps" in data
    assert "population_mix" in data
    assert data["duration_steps"] == 240


def test_get_scenario_not_found():
    """GET /api/scenarios/nonexistent returns 404."""
    resp = client.get("/api/scenarios/nonexistent")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# 4. Runs: start with monkey-patched subprocess
# ---------------------------------------------------------------------------


def test_start_run():
    """POST /api/runs with patched subprocess returns a run_id."""
    mock_proc = MagicMock()
    mock_proc.pid = 12345
    mock_proc.poll.return_value = None

    with patch("ui.backend.services.run_manager.subprocess.Popen", return_value=mock_proc) as mock_popen:
        resp = client.post("/api/runs", json={
            "scenario_name": "quiet_market",
            "seeds": [42],
            "no_llm": True,
        })

    assert resp.status_code == 200
    data = resp.json()
    assert "run_id" in data
    assert data["pid"] == 12345
    assert "output_dir" in data
    mock_popen.assert_called_once()


# ---------------------------------------------------------------------------
# 5. Runs: list shows running
# ---------------------------------------------------------------------------


def test_list_runs_shows_running():
    """GET /api/runs shows the started run as running."""
    mock_proc = MagicMock()
    mock_proc.pid = 12345
    mock_proc.poll.return_value = None

    with patch("ui.backend.services.run_manager.subprocess.Popen", return_value=mock_proc):
        start_resp = client.post("/api/runs", json={
            "scenario_name": "quiet_market",
            "seeds": [42],
            "no_llm": True,
        })

    run_id = start_resp.json()["run_id"]

    resp = client.get("/api/runs")
    assert resp.status_code == 200
    runs = resp.json()
    assert len(runs) >= 1

    found = [r for r in runs if r["run_id"] == run_id]
    assert len(found) == 1
    assert found[0]["status"] == "running"


# ---------------------------------------------------------------------------
# 6. Parquet section: actions
# ---------------------------------------------------------------------------


def test_read_parquet_actions(mock_run):
    """GET /api/runs/{id}/parquet/actions returns rows from fixture parquet."""
    resp = client.get("/api/runs/test123/parquet/actions")
    assert resp.status_code == 200
    data = resp.json()
    assert data["section"] == "actions"
    assert data["count"] == 5
    rows = data["rows"]
    assert len(rows) == 5
    # Verify structure
    assert rows[0]["step"] == 0
    assert rows[0]["archetype"] == "hodler"
    assert rows[0]["tier"] == "trade"


def test_read_parquet_prices(mock_run):
    """GET /api/runs/{id}/parquet/prices returns price rows."""
    resp = client.get("/api/runs/test123/parquet/prices")
    assert resp.status_code == 200
    data = resp.json()
    assert data["section"] == "prices"
    assert data["count"] == 3
    assert data["rows"][0]["base_symbol"] == "BTC"


def test_read_parquet_invalid_section(mock_run):
    """Invalid parquet section returns 400."""
    resp = client.get("/api/runs/test123/parquet/bogus")
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# 7. WebSocket: telemetry stream
# ---------------------------------------------------------------------------


def test_websocket_telemetry(mock_run, tmp_output_dir):
    """WS /ws/runs/{id} receives step messages from fixture telemetry."""
    # Mark the run as completed so the WS loop terminates
    mock_run.status = "completed"

    with client.websocket_connect("/ws/runs/test123") as ws:
        # Should receive step messages from the fixture parquet data,
        # then a complete message
        messages = []
        for _ in range(10):
            try:
                msg = ws.receive_json(mode="text")
                messages.append(msg)
                if msg.get("type") in ("complete", "error"):
                    break
            except Exception:
                break

    # We should have at least one step message and a complete message
    step_msgs = [m for m in messages if m["type"] == "step"]
    assert len(step_msgs) >= 1, f"Expected step messages, got: {messages}"
    # The step message should have the expected shape
    assert "data" in step_msgs[0]
    assert "total_actions" in step_msgs[0]["data"]

    complete_msgs = [m for m in messages if m["type"] == "complete"]
    assert len(complete_msgs) == 1


def test_websocket_not_found():
    """WS /ws/runs/{id} for nonexistent run sends error and closes."""
    with client.websocket_connect("/ws/runs/nonexistent") as ws:
        msg = ws.receive_json(mode="text")
        assert msg["type"] == "error"


# ---------------------------------------------------------------------------
# 8. Personas: filtered list
# ---------------------------------------------------------------------------


def test_list_personas_filtered():
    """GET /api/personas?archetype=lurker&limit=5 returns lurkers."""
    from ui.backend.routes.personas import _invalidate_cache
    _invalidate_cache()

    resp = client.get("/api/personas", params={
        "archetype": "lurker",
        "limit": 5,
    })
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["personas"]) == 5
    for p in data["personas"]:
        assert p["archetype"] == "lurker"


def test_list_personas_all():
    """GET /api/personas returns paginated results."""
    from ui.backend.routes.personas import _invalidate_cache
    _invalidate_cache()

    resp = client.get("/api/personas", params={"limit": 10, "offset": 0})
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 100
    assert len(data["personas"]) == 10


# ---------------------------------------------------------------------------
# 9. Personas: distribution
# ---------------------------------------------------------------------------


def test_persona_distribution():
    """GET /api/personas/distribution returns archetype counts."""
    from ui.backend.routes.personas import _invalidate_cache
    _invalidate_cache()

    resp = client.get("/api/personas/distribution")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 100
    dist = data["distribution"]
    assert "lurker" in dist
    assert "hodler" in dist
    assert dist["lurker"] == 10
    assert sum(dist.values()) == 100
